# Preemptive Health Metrics

Added in branch `harden/health-metrics`. These metrics are emitted on
the existing Hermes Prometheus endpoint (default `:3001/metrics`,
namespaced `hermes_*`) alongside upstream metrics.

The goal is **preemptive** signals: every metric below was designed
because some operator-visible incident lacked an alert that would have
fired *before* user-facing degradation. The "would have caught"
column states what each metric retroactively detects.

---

## Summary

| Metric                                            | Type    | Labels                                | Unit    | Wired? |
|---------------------------------------------------|---------|---------------------------------------|---------|--------|
| `hermes_client_seconds_to_expiry`                 | gauge   | `src_chain`, `dst_chain`, `client_id` | seconds | yes    |
| `hermes_view_sync_lag_blocks`                     | gauge   | `chain`                               | blocks  | yes    |
| `hermes_view_canonical`                           | gauge   | `chain`                               | 0/1     | yes    |
| `hermes_gate_rejection_total`                     | counter | `chain`, `kind`                       | events  | yes    |
| `hermes_last_broadcast_success_timestamp_seconds` | gauge   | `src_chain`, `dst_chain`, `channel`   | seconds | yes    |
| `hermes_oom_score`                                | gauge   | (none)                                | unitless| yes (Linux) |

All counters carry the `_total` suffix per OpenMetrics convention.
The OTel→Prometheus exporter appends `_total` automatically to
counters (e.g. `gate_rejection` → `hermes_gate_rejection_total`).

---

## `hermes_client_seconds_to_expiry`

**Definition.** `trusting_period − (src_chain_now − latest_consensus_timestamp)`.

* Emitted on **every** client-refresh worker tick (5 s) inside
  `ForeignClient::try_refresh` once the existing
  `validated_client_state()` succeeds.
* Signed: goes **negative** if the client is past its trusting period.
  This is intentional — `-3600` is more informative than clamping to
  `0`.

**Source of measurement noise.** Bounded by:
* round-trip latency to source chain RPC (`query_application_status`),
  typically O(100 ms);
* the source chain's block-time granularity for
  `latest_block_time`.

Effective noise in steady state: < 1 source-chain block.

**Would have caught.** Today's noble-near-expiry incident: the
operator learned of a 3-hour outage via an external alert. With this
gauge, a 3-day lead time alert (`< 86 400 × 3`) would have fired.

**Alerts.**

```promql
# critical: client expires in less than 1 day
min by (client_id, dst_chain) (hermes_client_seconds_to_expiry) < 86400

# warning: < 3 days
min by (client_id, dst_chain) (hermes_client_seconds_to_expiry) < 86400 * 3

# already expired
min by (client_id, dst_chain) (hermes_client_seconds_to_expiry) < 0
```

---

## `hermes_view_sync_lag_blocks`

**Definition.** For each tracked Penumbra chain,
`chain.latest_block_height − view.full_sync_height`.

Emitted by a detached tokio task (`penumbra/health.rs`,
`HEALTH_PROBE_INTERVAL = 10 s`) spawned at `PenumbraChain::bootstrap`.

The gauge is signed (`i64`): a negative reading indicates RPC
load-balancer skew (one of the chain RPCs returned a height behind
what our view already ingested). In steady state it's `>= 0`.

**Source of measurement noise.**
* The chain status RPC and the view's gRPC are not read atomically.
* The chain RPC may be load-balanced across nodes at different
  heights (operator's setup uses such pools). Skew is typically 1-2
  blocks.

**Would have caught.** Any view-stall regression: a hermes process
whose view stops sealing blocks while the chain advances. (Stale
views are the precondition for the cold-view non-canonical-anchor
failure mode documented in `CLAUDE.md`.)

**Alerts.**

```promql
# warning: view is more than 50 blocks behind for >5m
avg_over_time(hermes_view_sync_lag_blocks[5m]) > 50

# critical: view is stalled (no progress)
deriv(hermes_view_sync_lag_blocks[2m]) > 0.4
# i.e. lag is growing at >24 blocks/min ⇒ view has stalled while chain advances
```

---

## `hermes_view_canonical`

**Definition.** `1` iff the chain (via
`sct.v1.QueryService/AnchorByHeight(view.full_sync_height)`) returns
an anchor for the view's current sync height. `0` otherwise. Emitted
on the same 10 s tick as `view_sync_lag_blocks`.

This shares logic with the existing **canonical-anchor gate** in
`chain/penumbra/tx.rs`: the gate also calls
`AnchorByHeight(sync_height − i)` for `i ∈ [0, ANCHOR_CANONICAL_WINDOW]`
and only broadcasts if some height in the window matches the built
anchor. The metric strips down to the necessary condition that the
chain *recorded* a root at our view's height; it does not check
equality of locally-reconstructed roots (the SDK does not expose the
view's current root without driving a transaction plan; see "Source
of measurement noise" below).

**Source of measurement noise.**
* This is a **necessary-but-not-sufficient** check. It returns `1` if
  the chain has *any* recorded anchor at `sync_height`. The
  *sufficient* check would compare that to the view's
  locally-reconstructed root at the same height, which the
  Penumbra view SDK does not expose without a `Planner` round-trip
  (see `crates/penumbra-sdk-view-patched/src/client.rs`).
* The actual failure mode we are guarding against — a cold view
  resting on an unrecorded intermediate frontier — would always
  return `0` here, because that frontier was never recorded by the
  chain. So the metric correctly flags the cold-view failure;
  it's only blind to the rarer scenario "view and chain both
  have anchors at this height but the values differ", which would be
  consensus divergence.

**Would have caught.** Cold-view non-canonical-anchor incidents — a
hermes restart that picks up a view sitting between two recorded
heights and cannot build a canonical tx.

**Alerts.**

```promql
# critical: view non-canonical for ≥2 consecutive scrapes
max_over_time(hermes_view_canonical[30s]) == 0
```

---

## `hermes_gate_rejection_total`

**Definition.** Monotonic counter incremented inside the
canonical-anchor gate when:

* `kind="non_canonical_anchor"` — the gate exhausted
  `MAX_WITNESS_RETRIES` (3) tries and the built anchor was never
  recorded by the chain; the tx is **not** broadcast.
* `kind="anchor_query_failed"` — the gate could not even connect to
  the SCT query service; the tx is broadcast **unverified** (fail-safe)
  but we count the bypass.

**Source of measurement noise.** None — these are exact, in-process
event counts.

**Alerts.**

```promql
# warning: any rejection in the last 5m
increase(hermes_gate_rejection_total[5m]) > 0

# critical: repeated rejections (view stuck)
increase(hermes_gate_rejection_total{kind="non_canonical_anchor"}[15m]) > 3
```

---

## `hermes_last_broadcast_success_timestamp_seconds`

**Definition.** Unix timestamp (seconds since epoch) of the most
recent successful `send_from_operational_data` reply per
`(src_chain, dst_chain, channel)`. Emitted in
`link/relay_path.rs` immediately after the existing `tx_submitted`
telemetry call.

Note: this fires on successful **broadcast** (RPC accepted), not
on tx **confirmation**. For confirmation latency use the existing
`tx_latency_confirmed` histogram.

**Source of measurement noise.** None.

**Alerts.**

```promql
# critical: no successful broadcast in 30 minutes
(time() - hermes_last_broadcast_success_timestamp_seconds) > 1800

# warning: 10 minutes
(time() - hermes_last_broadcast_success_timestamp_seconds) > 600
```

---

## `hermes_oom_score`

**Definition.** Current value of `/proc/self/oom_score`. Range
0..1000 (kernel-imposed). Refreshed on the same 10 s tick as the
Penumbra view probe.

**Source of measurement noise.** None — but the kernel updates this
value asynchronously, so the gauge can lag a kernel-internal
adjustment by < 1 s. Negligible for our alerting purposes.

**Platform.** Linux only. On other platforms `refresh_oom_score()`
is a no-op (the file is unreadable and the gauge is simply never
observed); Prometheus will not emit the series at all, which is the
desired behaviour.

**Alerts.**

```promql
# warning: oom_score above 200 for 5m
avg_over_time(hermes_oom_score[5m]) > 200

# critical: above 500 (kernel is actively considering us)
hermes_oom_score > 500
```

---

## Files modified / added

* `crates/telemetry/src/state.rs` — fields, constructors, setter
  methods.
* `crates/relayer/src/foreign_client.rs` — emit
  `client_seconds_to_expiry` in `try_refresh`.
* `crates/relayer/src/chain/penumbra.rs` — register and spawn the
  view health probe at bootstrap; pass `chain_id` into
  `build_and_submit_penumbra_tx`.
* `crates/relayer/src/chain/penumbra/tx.rs` — emit
  `gate_rejection` on both failure modes; new
  `chain_id: ChainId` parameter.
* `crates/relayer/src/chain/penumbra/health.rs` — new module;
  spawns the periodic view health probe.
* `crates/relayer/src/link/relay_path.rs` — emit
  `last_broadcast_success` next to existing `tx_submitted`.
