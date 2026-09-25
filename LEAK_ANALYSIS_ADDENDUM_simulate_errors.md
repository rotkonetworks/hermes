# A1 addendum — confirmed cardinality bug in `simulate_errors` telemetry

Investigation continuation of LEAK_ANALYSIS.md. Reading the suspect surfaces
identified there ("medium-confidence: telemetry counter cardinality") and
confirming one of them is a real, fixable bug.

## TL;DR

`broadcast_errors` is safe: it routes its description through
`BroadcastError::new()` which maps to a bounded enum of ~40 short strings.

`simulate_errors` is **not** safe. It uses `get_error_text(&e)` as a label
which is bounded for `GrpcStatus` errors only. For any other `ErrorDetail`
variant the function returns `detail.to_string()` — the formatted error
message — which embeds variable data and is effectively unbounded.

## Trace

### Call site
`crates/relayer/src/chain/cosmos/estimate.rs:170-175,186-191`

```rust
telemetry!(
    simulate_errors,
    &_account.address.to_string(),
    true,                  // or false in the second arm
    get_error_text(&e),
);
```

### Normalizer
`crates/relayer/src/chain/cosmos/estimate.rs:215-222`

```rust
fn get_error_text(e: &Error) -> String {
    use crate::error::ErrorDetail::*;
    match e.detail() {
        GrpcStatus(detail) => detail.status.code().to_string(),
        detail => detail.to_string(),
    }
}
```

`GrpcStatus.code().to_string()` returns one of `tonic::Code`'s ~16 named
variants (`"Ok"`, `"Cancelled"`, `"Unknown"`, ..., `"Unauthenticated"`).
Bounded. Safe.

`detail.to_string()` for any other variant uses the `Display` impl on
`ErrorDetail`. Many variants embed full chain responses, sequence numbers,
gas-used values, and packet bytes.

### Metric definition
`crates/telemetry/src/state.rs:1321-1329`

```rust
pub fn simulate_errors(&self, address: &String, recoverable: bool, error_description: String) {
    let labels = &[
        KeyValue::new("account", address.to_string()),
        KeyValue::new("recoverable", recoverable.to_string()),
        KeyValue::new("error_description", error_description.to_owned()),
    ];
    self.simulate_errors.add(1, labels);
}
```

The OpenTelemetry SDK retains one time-series per unique
(account, recoverable, error_description) tuple, for the meter's
lifetime. With unbounded `error_description`, this is unbounded growth.

## How this manifests in production

The cosmoshub-4 expired-client failure today causes a non-GrpcStatus
`Error` path through `send_tx_simulate` whose `detail()` matches a variant
whose `Display` impl includes the full Cosmos response message — which
itself includes the line `with gas used: 'NNNNNN'`. The gas-used value
varies per simulation attempt (depends on chain state at the time).

Each retry against the expired client therefore emits a fresh
`error_description` string, growing the metric's label set linearly with
retry count.

Refresh worker short-circuit fix (commit 330cd4c5) cuts retries per scan
from ~24 to 1 — but does not eliminate them. With one supervisor scan per
hour and the channel-940 / connection-1049 link still active, the
description set still grows ~24 strings per day per worker. Each string
~200 bytes after escaping → ~5 KB/day per worker for label storage alone,
plus the per-time-series metadata in the OTel SDK which is the larger
fraction.

This is consistent with the slow, sustained leak observed once the
refresh-worker churn is contained: 4-8 GB in minutes was likely the
combination of refresh-worker churn (fixed) and this cardinality leak
(unfixed), with the cardinality leak dominating the steady-state slope
once the burst churn is gone.

## Proposed fix

Apply the same normalization pattern used for `broadcast_errors`. The
minimum-viable change keeps the existing API but always maps
`error_description` through a bounded enum.

### Patch 1 — extend `BroadcastError` to cover simulate errors too

A new function `SimulateError::new` in
`crates/telemetry/src/broadcast_error.rs` or a new
`crates/telemetry/src/simulate_error.rs` that classifies the few
`ErrorDetail` variants that reach this code path:

```rust
pub fn classify_simulate_error(text: &str) -> &'static str {
    if text.contains("status Expired") { "client expired" }
    else if text.contains("status Frozen") { "client frozen" }
    else if text.contains("account sequence mismatch") { "account sequence mismatch" }
    else if text.contains("insufficient funds") { "insufficient funds" }
    else if text.contains("out of gas") { "out of gas" }
    else if text.contains("tx already in mempool") { "tx already in mempool" }
    else if text.contains("mempool is full") { "mempool full" }
    else if text.contains("invalid packet") { "invalid packet" }
    else if text.contains("packet timeout") { "packet timeout" }
    else if text.contains("packet already received") { "packet already received" }
    else if text.contains("acknowledgement for packet already exists") { "ack already exists" }
    else if text.contains("packet commitment not found") { "packet commitment not found" }
    else if text.contains("connection not found") { "connection not found" }
    else if text.contains("channel not found") { "channel not found" }
    else { "other" }
}
```

The returned `&'static str` guarantees zero allocation per emit and
bounded label cardinality (15 unique values).

### Patch 2 — apply at the call site

`crates/relayer/src/chain/cosmos/estimate.rs`:

```rust
use ibc_telemetry::classify_simulate_error;

// ...

telemetry!(
    simulate_errors,
    &_account.address.to_string(),
    true,
    classify_simulate_error(&get_error_text(&e)).to_owned(),
);
```

Or, less invasive, just truncate at the state.rs boundary:

```rust
pub fn simulate_errors(&self, address: &String, recoverable: bool, error_description: String) {
    // Defensive bound on label cardinality: classify into a small set of
    // known patterns. See LEAK_ANALYSIS_ADDENDUM_simulate_errors.md.
    let bounded = classify_simulate_error(&error_description);
    let labels = &[
        KeyValue::new("account", address.to_string()),
        KeyValue::new("recoverable", recoverable.to_string()),
        KeyValue::new("error_description", bounded.to_owned()),
    ];
    self.simulate_errors.add(1, labels);
}
```

The state.rs boundary is the safer place to apply the fix — even if
future call sites are added, they all funnel through here.

## Regression test sketch

`crates/telemetry/tests/simulate_errors_cardinality.rs`:

```rust
#[test]
fn simulate_errors_label_cardinality_is_bounded() {
    let state = TelemetryState::new(...);
    let account = "cosmos1...".to_string();
    // Emit N distinct descriptions (varying gas-used values).
    for gas in 0..10_000 {
        state.simulate_errors(
            &account,
            false,
            format!("client expired with gas used: '{}'", gas),
        );
    }
    // Scrape the prometheus exporter.
    let scrape = state.gather_prometheus();
    // Count distinct (account, recoverable, error_description) tuples
    // for the simulate_errors metric. Should be 1 (all classified as
    // "client expired"), not 10_000.
    assert!(scrape.distinct_label_tuples("simulate_errors") < 5);
}
```

Failing today; passing after either Patch 1 or 2.

## Dashboard impact

Existing Grafana panels that filter `simulate_errors{error_description=~"..."}`
need to change their regex to the new bounded set. The classifier above
intentionally uses the same wording as `BroadcastError::get_short_description`
so dashboards can use the same selectors across both metrics.

The dashboard at https://flarev.rotko.net/d/hermes-rotko has one such
panel (the "tx simulate failures by reason" pie chart per A5). Audit before
merging this fix.

## Estimated impact on the observed leak

With one supervisor scan per hour and the refresh-worker short-circuit in
effect, the cardinality leak adds ~5 KB/day of label string storage per
worker. The OTel SDK's per-time-series overhead is significantly larger
than the string itself (Aggregator + AttributeSet structs), likely on the
order of 1-2 KB per time-series. With workers per chain × chains in
config, the steady-state leak rate is in the range of a few MB/day per
worker.

This does not explain "4-8 GB in minutes" on its own. The remaining
discrepancy is explained by either:

1. The refresh-worker churn before commit 330cd4c5 (now fixed). With
   24 attempts per scan and many scans per hour during peak retry
   pressure, the burst rate was much higher.
2. The tonic Channel retention path still under investigation (no fix
   yet).

Suggested ordering: land Patch 2 (the cardinality fix), redeploy on one
node, observe the new steady-state slope. If the slope is still
inconsistent with retention being the dominant remaining factor, then
the tonic Channel pool experiment from LEAK_ANALYSIS.md "high-confidence"
section becomes the next step.

## Not in scope for this addendum

- The tonic Channel pool retention is not addressed here. The fix
  (introduce a per-chain channel cache keyed by host) is straightforward
  but invasive (~30 call sites need updating). Deferred.
- This addendum proposes the patches but does NOT land them. The fork
  hardening discipline (per CLAUDE.md and project memory) is "necessary
  but not sufficient" first, A2 view canonicality and #20 governance
  recovery before further code changes go live.

## Verification

The verification of this addendum's claim — that `simulate_errors` is
unbounded — does NOT require landing the patch. Just scrape the existing
prometheus endpoint on p02 for an hour while the cosmoshub client is
expired and the channel-940 worker is active, then count distinct
label tuples for `simulate_errors`. If they grow with the retry count,
the diagnosis is confirmed.

```bash
curl -s http://p02-internal-ip:3001/metrics | grep '^simulate_errors{' | wc -l
# repeat every 5 min for 1 h
```

Expect: grows by ~1 per cosmos retry per scan.
