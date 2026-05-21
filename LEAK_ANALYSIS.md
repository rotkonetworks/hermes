# A1: hermes memory-leak investigation

Branch: `harden/memory-leak-fix`
Base: `c0e16b07` (`p1-canonical-anchor-gate`)

## Context

p02 hermes RSS grows from ~30 MB at startup to 4–8 GB over minutes-to-hours,
OOMing 8+ times today (2026-05-19). One OOM took down `pd` (the validator)
as collateral damage at 12:09 UTC.

Sustained retry pressure is the driver:
- Cosmos client `07-tendermint-1317` is **expired**.
- Recovery requires `MsgRecoverClient` (governance), tracked in task #20.
- Until then, hermes retries indefinitely on every refresh / clear-interval
  pass.

## What we know (proven)

1. **The fallback header-construction path does NOT leak.**

   The A3 agent's regression test
   (`crates/relayer/tests/regression/memory_non_growth.rs`)
   drives `expired_fallback_with_fetcher` for N=1000 iterations and measures
   RSS. We re-ran it on this branch:

   ```
   rss_does_not_grow_under_sustained_expired_fallback_retry:
     baseline=14772 kB, after=14772 kB, grew=0 kB (threshold=51200 kB, N=1000)
   ```

   0 kB growth across 1000 cycles. This path is clean. `ValidatorSet`,
   `LightBlock`, `TmHeader`, and `Error` allocate-and-drop symmetrically.

2. **The refresh worker drives ~24 full retry stacks per expired client
   per spawn.**

   Found by code review in `crates/relayer/src/worker/client.rs`
   `spawn_refresh_client`:

   ```rust
   // BEFORE this branch:
   let res = retry_with_index(refresh_strategy(), |_| client.refresh());
   ```

   - `refresh_strategy()` = Fibonacci(5s, …) clamped to 1 h max-delay,
     1 day total.
   - `client.refresh()` → `try_refresh` → `validated_client_state` which
     queries the dst chain 3× (client state, consensus state at latest,
     application status) on EVERY attempt.
   - If the client is expired, `validated_client_state` errors with
     `ExpiredOrFrozen::Expired`. The retry loop catches the error and
     re-enters → another 3 queries + error formatting. ~24 times.
   - The spawn-time check `client.is_expired_or_frozen()` only catches
     clients that were ALREADY expired at supervisor startup. Clients
     that expire DURING the hermes run (the cosmoshub 07-tendermint-1317
     case) skip the spawn guard and reach the retry loop.

   Per-attempt allocations:
   - 3× fresh `tonic::transport::Channel` (each opens a new TCP/TLS
     connection — see `crates/relayer/src/util.rs::create_grpc_client`,
     which does **not** pool channels; every gRPC query connects anew).
   - 3× `Span::current()` + tracing instrument records.
   - 1× `AnyClientState`, 1× `AnyConsensusState`, 1× `ChainStatus`.
   - 1× chain-handle round trip (crossbeam `unbounded()` channel; the
     `ReplyTo` oneshot + the `Span` captured per-request).
   - The `Error::expired_or_frozen` value with the formatted description
     `"time elapsed since last client update: {elapsed:?}"`.

   Per-cycle scale: 5 s spawn interval × 24 retries × multiple workers.
   This is a slow leak by itself — but it provides a constant background
   allocation churn on top of which the fast leak (still under
   investigation) sits.

## What's fixed in this branch

### Fix 1: refresh worker short-circuits on expired/frozen clients

`crates/relayer/src/worker/client.rs`:

```rust
let res = retry_with_index(refresh_strategy(), |_| match client.refresh() {
    Ok(events) => OperationResult::Ok(events),
    Err(e) if e.is_expired_or_frozen_error() => OperationResult::Err(e),
    Err(e) => OperationResult::Retry(e),
});
```

On the first `ExpiredOrFrozen` error, the retry loop exits immediately,
the worker returns `TaskError::Fatal`, and `spawn_background_task` breaks
the outer loop. The worker terminates. The supervisor will re-spawn it
on the next scan IF the client has been recovered; until then, the
worker stays terminated and stops contributing to allocation churn.

**Behavior change:** an expired client now consumes exactly 1 refresh
attempt per supervisor scan instead of 24 per spawn-plus-1-day.

### Regression tests

`crates/relayer/tests/regression/refresh_worker_short_circuit.rs`:

- `refresh_retry_short_circuits_on_expired_client` — asserts
  `attempts == 1` when the closure returns `ExpiredOrFrozen::Expired`.
- `refresh_retry_short_circuits_on_frozen_client` — same for `Frozen`.
- `refresh_retry_still_retries_on_transient_errors` — sanity check that
  ordinary errors still retry; guards against an over-eager short-circuit
  that would break recovery from transient RPC failures.

All three pass on this branch.

## What's not fixed yet (open suspects)

The refresh-worker churn alone does NOT explain 4–8 GB growth in minutes.
Other suspect surfaces, ranked by my confidence:

### High-confidence: tonic Channel / HTTP/2 connection retention

`crates/relayer/src/util.rs::create_grpc_client` opens a **new**
`tonic::transport::Channel` on every gRPC query — every call to
`query_client_state`, `query_consensus_state`, `query_application_status`,
`query_packet_commitments`, etc. There is no pool. With ~30 different
call-sites in `crates/relayer/src/chain/cosmos*.rs`, even an
unloaded-but-running hermes opens hundreds of TCP connections per second.

If hyper's connection pool (held by the dropped Channel) doesn't free
the underlying `Connection` object promptly — e.g. it waits for the
HTTP/2 GOAWAY round-trip but the server never responds because we
dropped the request inflight — these accumulate. **Hyper's
`PoolClient` is dropped when the Channel is dropped, but the
`ConnectionPool` runs on the tokio runtime in the background and
needs to be polled to actually release sockets.** Under retry pressure
the runtime may not poll the pool's cleanup tasks fast enough.

Couldn't reproduce locally because the leak needs:
- a real cosmos RPC + gRPC server on the other end
- the specific failure mode where the chain rejects with
  `client status Expired` deep in `simulate` (not at gRPC transport)

**Recommended next experiment** (server-side, on a snapshot-protected
host): instrument `create_grpc_client` to count live channels using a
`Drop` newtype around the returned client; expose as a metric; observe
correlation between channel count and RSS over 1 hour.

### Medium-confidence: tracing-subscriber span retention

Every `#[instrument]` macro creates a span on entry; the `FmtSubscriber`
configured in `crates/relayer-cli/src/components.rs` should free spans
on exit, but:
- the `with_thread_ids(true)` option allocates a small per-span string
  per emit;
- the `error_span!` macros in workers capture `client = %client.id` and
  `src_chain = %client.src_chain.id()` as `Display` strings — formatted
  per-entry, dropped per-exit, but **per-attempt** during a hot retry.

This is per-attempt steady-state, not a true leak. Bounding factor.

### Medium-confidence: telemetry counter cardinality

`crates/telemetry/src/state.rs::broadcast_errors` and `simulate_errors`
add `error_description` as a metric label. OpenTelemetry counters
allocate a new time-series for each unique label tuple, and time-series
metadata is retained for the meter's lifetime. If chain log responses
contain transient information (hashes, sequences) embedded in the
`error_description`, this is unbounded growth.

Inspect: `crates/relayer/src/chain/cosmos/retry.rs` lines 114-117 —
`response.log` is passed as the description. Cosmos tx logs often
embed sequence numbers and hashes.

**Recommended next experiment:** scrape the prometheus endpoint on p02
and count the distinct label tuples for `broadcast_errors` over an
hour; if it grows linearly with retries, this is the leak.

### Low-confidence (ruled in only by code review)

- `PENDING_CACHE` (`crates/relayer/src/supervisor.rs:66`): bounded by
  `Vec<ChannelPending>` size, replaced wholesale every refresh.
  Not a leak.
- moka `Cache` in `crates/relayer/src/cache.rs`: bounded `max_capacity`.
  Not a leak.
- `excluded_sequences` / `quarantined_sequences`: `HashSet<Sequence>`,
  bounded by total packet count on the channel.

## dhat-rs heap profiling

I added `dhat = "0.3"` as a dev-dep and a `dhat-heap` feature flag on
the `ibc-relayer` crate (see `crates/relayer/Cargo.toml`). To collect
a profile of the regression test:

```bash
cargo test -p ibc-relayer --test regression_tests --features dhat-heap \
  rss_does_not_grow -- --nocapture
# Outputs dhat-heap.json in CWD; open with dh_view.html from dhat-rs.
```

I did NOT activate dhat as the global allocator in this commit because
the test only exercises the `expired_fallback_with_fetcher` shape,
which we already know is clean. Wiring dhat into a real hermes binary
running against a chain RPC is the next step — but **it requires running
hermes the binary, not the test**, because the suspected leak surfaces
(tonic Channel retention, telemetry cardinality) only manifest under
real RPC traffic.

To wire dhat into the hermes binary itself for an on-server
investigation:

```rust
// crates/relayer-cli/src/main.rs (top of file):
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

// in fn main():
#[cfg(feature = "dhat-heap")]
let _profiler = dhat::Profiler::builder().file_name("hermes-dhat.json").build();
```

Then build and run with `--features dhat-heap`. dhat writes the profile
on `Profiler` drop, so `SIGTERM` the binary (don't SIGKILL).

## How to verify the fix

```bash
cd /home/alice/rotko/hermes/.claude/worktrees/memory-leak-fix
cargo test -p ibc-relayer --test regression_tests -- --nocapture
```

Expected: 4 passed, 2 ignored (the view-canonicality tests, blocked on A2).

```
rss_does_not_grow_under_sustained_expired_fallback_retry:
  baseline=14772 kB, after=14772 kB, grew=0 kB (threshold=51200 kB, N=1000)
test result: ok. 4 passed; 0 failed; 2 ignored
```

To verify the fix on a server (the actual symptom):
1. Build this branch on p02.
2. Roll it in (one node at a time per project convention).
3. Confirm the cosmoshub-4 refresh worker terminates ONCE per scan
   instead of looping for ~1 day:
   ```
   journalctl -u hermes | grep "abandoning refresh worker"
   ```
4. Observe RSS slope vs. the previous build. Expect: still grows
   (suspected tonic-channel leak unchanged), but slower.

## Honest assessment

This branch is **necessary but not sufficient**. It:

1. Removes a known, code-review-confirmed allocation churn source on
   expired clients (the refresh-worker retry loop).
2. Lands a regression test that pins the short-circuit behavior.
3. Documents the remaining suspects and proposes specific on-server
   experiments to localize them.

It does NOT, by itself, explain 4–8 GB growth in minutes. That requires
the tonic-channel and/or telemetry-cardinality investigation, both of
which need server-side instrumentation against real RPC traffic.

Recommended deploy strategy: roll this branch out as a measured first
step (per project convention: one node first, watch for regressions),
THEN run the channel-count instrumentation experiment on the second node
to localize the remaining leak surface.
