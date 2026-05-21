//! Periodic Penumbra view health probe.
//!
//! Emits three preemptive metrics about the local Penumbra view's
//! relationship to the chain:
//!
//! * `view_sync_lag_blocks{chain}` —
//!   `chain.latest_block_height - view.full_sync_height`.
//!
//! * `view_canonical{chain}` — 1 iff the chain has a recorded SCT
//!   anchor at `view.full_sync_height`. 0 means the view is sitting
//!   on an intermediate frontier the chain never sealed
//!   (the cold-view failure mode that triggers
//!   "provided anchor is not a valid SCT root").
//!
//! * `oom_score` — `/proc/self/oom_score`, refreshed on the same tick
//!   for convenience.
//!
//! Measurement noise:
//! * `view_sync_lag_blocks` is observed at one wall-clock instant but
//!   the two reads (chain status RPC + view status gRPC) are not
//!   atomic; small skew (1-2 blocks) is normal.
//! * `view_canonical` queries `AnchorByHeight(view.full_sync_height)`.
//!   A view that hasn't ingested the next block yet but whose
//!   internal SCT root for the *current* height is canonical will
//!   read as 1, exactly as we want. The signal can flip 0 -> 1 once
//!   the view re-syncs without restart, which is the desired
//!   recovery indication.

use std::time::Duration;

use ibc_relayer_types::core::ics24_host::identifier::ChainId;
use penumbra_sdk_proto::box_grpc_svc::BoxGrpcService;
use penumbra_sdk_proto::core::component::sct::v1::query_service_client::QueryServiceClient as SctQueryServiceClient;
use penumbra_sdk_proto::core::component::sct::v1::AnchorByHeightRequest;
use penumbra_sdk_proto::view::v1::view_service_client::ViewServiceClient;
use penumbra_sdk_view::ViewClient;
use tendermint_rpc::{Client as _, HttpClient};

/// How often to refresh the view health metrics. Chosen short enough
/// to give an operator ~3 samples/min, long enough not to add measurable
/// load on the view sqlite + chain status RPC.
pub const HEALTH_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// Spawn the periodic Penumbra-view health probe.
///
/// The future is detached on the supplied tokio runtime and lives for
/// the lifetime of the process. Errors are logged at WARN; we
/// deliberately keep going on failure so a transient view or RPC
/// outage doesn't blind us to other chains' metrics.
pub fn spawn_view_health_probe(
    rt: &tokio::runtime::Runtime,
    chain_id: ChainId,
    mut view_client: ViewServiceClient<BoxGrpcService>,
    tendermint_rpc: HttpClient,
    grpc_url: String,
) {
    rt.spawn(async move {
        let mut tick = tokio::time::interval(HEALTH_PROBE_INTERVAL);
        // Avoid a burst of probes if the system clock jumps.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tick.tick().await;

            // 1. Read view sync height (gRPC). Our local clone of
            //    ViewServiceClient is dedicated to this task, so we
            //    don't contend with anyone for it.
            let sync_height = match ViewClient::status(&mut view_client).await {
                Ok(s) => s.full_sync_height,
                Err(e) => {
                    tracing::warn!(chain = %chain_id, error = %e, "view health probe: view status failed");
                    continue;
                }
            };

            // 2. Read chain latest height (Tendermint RPC).
            let chain_height = match tendermint_rpc.status().await {
                Ok(s) => s.sync_info.latest_block_height.value(),
                Err(e) => {
                    tracing::warn!(chain = %chain_id, error = %e, "view health probe: chain status failed");
                    continue;
                }
            };

            // 3. Emit the lag gauge. Signed so RPC skew (load-balanced
            //    endpoints, view-ahead-of-rpc raciness) reads cleanly
            //    instead of wrapping into a huge u64.
            let lag = chain_height as i64 - sync_height as i64;
            ibc_telemetry::global().view_sync_lag_blocks(&chain_id, lag);

            // 4. Query AnchorByHeight(sync_height). A successful
            //    response with a non-empty anchor means the chain
            //    has recorded a root for that height — necessary
            //    condition for the canonical-anchor gate to accept
            //    any tx built off this view.
            let canonical = match SctQueryServiceClient::connect(grpc_url.clone()).await {
                Ok(mut sct) => match sct
                    .anchor_by_height(AnchorByHeightRequest { height: sync_height })
                    .await
                {
                    Ok(resp) => resp.into_inner().anchor.is_some(),
                    Err(e) => {
                        // tonic Status — non-fatal; just emit 0 and move on.
                        tracing::debug!(
                            chain = %chain_id,
                            height = sync_height,
                            error = %e,
                            "view health probe: anchor_by_height returned error",
                        );
                        false
                    }
                },
                Err(e) => {
                    tracing::warn!(chain = %chain_id, error = %e, "view health probe: SCT gRPC connect failed");
                    // Connect failure: leave gauge at last observed value.
                    // (Don't lie that the anchor is non-canonical when we
                    // actually have no signal.) Continue so we still refresh
                    // OOM score below on the same tick.
                    ibc_telemetry::global().refresh_oom_score();
                    continue;
                }
            };
            ibc_telemetry::global().view_canonical(&chain_id, canonical);

            // 5. Cheap, always refreshable on the same cadence.
            ibc_telemetry::global().refresh_oom_score();
        }
    });
}
