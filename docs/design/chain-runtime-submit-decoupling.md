# Decoupling submission from the chain-runtime dispatch loop

**Status:** Proposed
**Author:** rotko networks (with notes from operational experience running Penumbra↔Noble relay)
**Intended audience:** penumbra-zone/hermes maintainers, ibc-relayer maintainers
**Filed against:** `crates/relayer/src/chain/runtime.rs`, `crates/relayer/src/chain/penumbra.rs`, `crates/relayer/src/chain/handle/base.rs`

## Summary

The chain runtime in `ibc-relayer` is a single-threaded dispatch loop that
processes `ChainRequest` messages serially. For Cosmos SDK chains this is
fine — `send_messages_and_wait_commit` returns within a block time
(~6 s) and the loop drains quickly. For Penumbra, the same call performs a
local Groth16 proof build inside `send_messages_in_penumbratx` →
`tx::build_and_submit_penumbra_tx`, which typically takes 30–90 s and can
exceed a minute under load. While that is in flight, the runtime cannot
dispatch **any** other request for the same chain — including reads from
the wallet worker, telemetry queries, and queued submissions from other
relay workers.

Under continuous traffic this manifests as a self-sustaining wedge:

1. Submit-1 occupies the dispatch thread.
2. Submit-2 (a follow-up packet, a misbehaviour check, anything) queues.
3. The caller of Submit-2 hits its `recv_timeout` (5 min default) and
   surfaces the symptom as a "wallet worker wedged" error.
4. The caller retries, pushing Submit-3, Submit-4, … onto the queue,
   each waiting behind a build that hasn't even started yet.
5. The only known mitigation is to kill the hermes process (the queue
   gets dropped) — the historical hermes-watchdog.timer was exploiting
   exactly this.

In production we have seen this taking down all `noble→penumbra`
delivery for hours at a time. The workaround in current rotko fork is
a 3-step manual ritual (`clear_on_start=true`, restart, flip back) that
gets the first packet through before the queue fills up. It works but
it isn't auto-relay.

This document proposes the architectural change that closes the gap.

## Non-goals

- **Not** an async/await rewrite of the entire `ibc-relayer` crate.
  That is a multi-week project and isn't required to fix the wedge.
- **Not** changing the public `ChainHandle` trait. Callers should be
  unaware.
- **Not** parallelising Penumbra submissions against each other. The
  Penumbra wallet has correctness constraints (note nullification, SCT
  anchor freshness) that require submissions to serialise per wallet.
  The goal is to keep that serial property while not blocking unrelated
  queries.

## What we ship in the rotko fork as a stopgap

Before this refactor lands upstream, two defensive changes have been
made:

- **A1e** (`43d70c31`): cache `ChainConfig` in `BaseChainHandle` so the
  wallet worker's per-5s `config()` poll bypasses the runtime channel
  entirely. The poll was the loudest symptom (it spammed "wedged"
  alerts every iteration) even though it was harmless. This is purely a
  hot-path read of an immutable value; safe to cache. Commit message
  has details.

- **A1d-2** (`8177fbdf`): split `RECV_TIMEOUT` into `RECV_TIMEOUT`
  (5 min, for queries) and `RECV_TIMEOUT_SUBMIT` (15 min, for the two
  submit variants). This stops a Submit-2 from declaring itself wedged
  while a legitimate Submit-1 build is still in flight. It does not
  fix throughput — second submission still waits — but it removes the
  spurious feedback loop where a false wedge signal causes retry-
  amplification.

Both are mitigations. Neither addresses the underlying constraint.

## Proposed architecture

We propose to keep the single-thread `ChainRuntime` for query dispatch
and to delegate `SendMessagesAndWaitCommit` / `SendMessagesAndWaitCheckTx`
to a per-chain **submission worker** thread.

### Sketch

```
                +-----------------------+      +-----------------------+
                |  ChainHandle (caller) |      |  ChainHandle (caller) |
                +-----------+-----------+      +----------+------------+
                            |  ChainRequest                |
                            +--+----------+---+------------+
                               |          |   |
                               v          v   v
                       +------------------------------+
                       |  ChainRuntime dispatch loop  |
                       |  (single thread, today)      |
                       +---+--------------------+-----+
                           |                    |
            non-submit     |                    |    submit
            (handled here  |                    |    (forwarded)
             as today)     |                    v
                           |        +-----------------------------+
                           v        |  SubmitWorker (new thread)  |
              +------------------+  |  - owns submit-side state   |
              |  chain endpoint  |  |  - serial inside this thread|
              | (shared via Arc) |  +-----------------------------+
              +------------------+
```

### Concrete changes

1. **`ChainRuntime::chain` becomes `Arc<dyn ChainEndpoint>` (or a wrapper)**
   so both the dispatch loop and the submit worker can hold references.
   Methods that today take `&mut self` on the endpoint trait become
   `&self` with interior mutability where needed. The mutating fields
   are already mostly behind `Arc<Mutex<_>>` for Penumbra (`view_client`,
   `tx_build_lock`, `query_service`). The remaining `&mut self` callers
   for Penumbra are `custody_client`, `tendermint_light_client`,
   `tx_monitor_cmd`. Wrapping these is straightforward.

2. **Add a `submit_tx` MPSC channel** alongside the existing request
   channel. The dispatch loop selects on both. The submit channel is
   drained by a sibling worker thread `SubmitWorker` which owns nothing
   except the receiver and an `Arc` to the endpoint.

3. **Dispatch loop's handling of `SendMessages*`** changes from
   ```rust
   ChainRequest::SendMessagesAndWaitCommit { tracked_msgs, reply_to } => {
       self.send_messages_and_wait_commit(tracked_msgs, reply_to)
   }
   ```
   to
   ```rust
   ChainRequest::SendMessagesAndWaitCommit { tracked_msgs, reply_to } => {
       self.submit_tx.send(SubmitJob::Commit { tracked_msgs, reply_to })?;
       Ok(())
   }
   ```
   — the dispatch loop returns immediately and resumes pulling work.

4. **`SubmitWorker::run`** is a simple loop that pulls `SubmitJob`s and
   calls `self.endpoint.send_messages_and_wait_commit(tracked_msgs)`,
   sending the result through `reply_to`. Because there's exactly one
   submit worker per chain runtime, per-wallet serialisation is preserved.

5. **No change to `ChainHandle` trait or to caller code.** All callers
   still see the same `send_messages_and_wait_commit(&self, ...)` API
   on the handle, with the same blocking semantics. They just no longer
   block other unrelated queries on the same chain.

### Why this is enough

The wedge is specifically: "long-running submit holds the dispatch
thread, queries behind it time out." Moving submit off the dispatch
thread removes that. Multiple submits queued behind each other still
serialise — that's correct per Penumbra wallet semantics. They just
serialise on the submit-worker queue instead of on the runtime-dispatch
queue, and they no longer starve queries.

### What stays the same

- Wallet correctness (nullifier ordering, SCT anchor freshness).
- The behaviour of `send_messages_and_wait_commit` from the caller's POV
  (it still blocks until commit).
- The `ChainEndpoint` trait shape — only the interior mutability moves.
- Tendermint / Cosmos SDK chains continue to work exactly as today;
  their submit happens to be fast so the submit worker drains quickly,
  but the architecture is the same.

## Migration plan

1. Land the trait-internal refactor (move `&mut self` → `&self` +
   interior mutability for the mutating callers on Penumbra). Pure
   refactor, no behaviour change, can be reviewed/merged independently.

2. Add the submit-worker plumbing. Default to "submit happens inline
   on the dispatch thread" so this is a no-op until step 3.

3. Flip Penumbra's submit to route through the submit worker. Test on
   a continuous traffic profile (a few `MsgTransfer` packets back to
   back across noble↔penumbra).

4. (Optional) Flip Cosmos chains too. Their submits are short so the
   benefit is small, but it makes the codepath uniform.

## Testing strategy

The wedge only reproduces under load — single-packet relays work fine
even with the bug because nothing else is competing for the runtime.
Reproducer:

1. Generate ≥ 2 pending packets noble→penumbra (e.g. send two small
   USDC transfers within a 60 s window).
2. Run hermes with `clear_on_start = true` and observe.

Pre-fix: second packet's `send_messages_and_wait_commit` times out at
`RECV_TIMEOUT` while the first packet's Groth16 build is still running.

Post-fix: second packet queues on the submit worker and runs
immediately after the first packet's commit lands.

## Open questions for review

- Is there appetite in penumbra-zone for taking this as a defensive PR,
  or would maintainers prefer to wait for a larger async rewrite?
- For the trait refactor in step 1, are there chain implementations
  outside of `cosmos.rs` and `penumbra.rs` that need consideration?
- Is the submit-worker per-chain (proposed) or should there be a
  shared submit pool? Per-chain is simpler and matches wallet semantics
  — a shared pool would just be wasted scheduling.

## Operational notes (from running this in prod)

For anyone who picks this up: the symptom user-facing is the watchdog
keeping hermes alive by restarting it before the queue gets too long.
The "watchdog kept everything moving" story we documented in
`project-hermes-watchdog-timer-was-flapping` was actually the watchdog
inadvertently masking exactly this bug for months. When the watchdog
was removed in 2026-05-29, the wedge surfaced openly as packet 7610
stuck for hours. The one-shot clear pattern (flip `clear_on_start`,
restart, flip back) is a manual version of what the watchdog used to
do automatically; it confirms the wedge is real and that the fix is
along these lines.
