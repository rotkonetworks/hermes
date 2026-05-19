//! Penumbra transaction building and submission.
//!
//! Standalone async functions that separate the transaction lifecycle
//! (plan -> build -> sign -> submit -> confirm) from the ChainEndpoint impl,
//! making each step independently testable and composable.

use std::sync::Arc;

use anyhow::Context;
use futures::{FutureExt, TryStreamExt};
use tokio::sync::RwLock;
use tracing::info;

use penumbra_sdk_fee::FeeTier;
use penumbra_sdk_ibc::IbcRelay;
use penumbra_sdk_keys::FullViewingKey;
use penumbra_sdk_keys::keys::AddressIndex;
use penumbra_sdk_proto::box_grpc_svc::BoxGrpcService;
use penumbra_sdk_proto::core::component::ibc::v1::IbcRelay as ProtoIbcRelay;
use penumbra_sdk_proto::custody::v1::custody_service_client::CustodyServiceClient;
use penumbra_sdk_proto::view::v1::{
    broadcast_transaction_response::Status as BroadcastStatus,
    view_service_client::ViewServiceClient, GasPricesRequest,
};
use penumbra_sdk_transaction::Transaction;
use penumbra_sdk_transaction::txhash::TransactionId;
use penumbra_sdk_view::ViewClient;
use penumbra_sdk_wallet::plan::Planner;
use signature::rand_core::OsRng;

use crate::chain::penumbra::error::PenumbraError;
use crate::chain::tracking::TrackedMsgs;

/// Plan and build a Penumbra transaction from IBC relay messages.
///
/// This is a pure async function: the caller (ChainEndpoint) is responsible
/// for calling `rt.block_on()`.
/// Maximum number of retries when the planner selects a note whose SCT
/// witness was already pruned (the narrow window between block commit and
/// the next plan attempt).
const MAX_WITNESS_RETRIES: usize = 3;

pub async fn build_penumbra_tx(
    view_client: &mut ViewServiceClient<BoxGrpcService>,
    custody_client: &mut CustodyServiceClient<BoxGrpcService>,
    fvk: &FullViewingKey,
    tracked_msgs: TrackedMsgs,
    tx_build_lock: &Arc<RwLock<()>>,
) -> Result<Transaction, PenumbraError> {
    let gas_prices: penumbra_sdk_fee::GasPrices = view_client
        .gas_prices(GasPricesRequest {})
        .await
        .map_err(|e| PenumbraError::ViewService {
            operation: "gas_prices",
            source: anyhow::anyhow!("{}", e),
        })?
        .into_inner()
        .gas_prices
        .ok_or(PenumbraError::GasPricesUnavailable)?
        .try_into()
        .map_err(|e: anyhow::Error| PenumbraError::TxBuild {
            reason: format!("failed to parse gas prices: {}", e),
        })?;

    let fee_tier = FeeTier::default();

    let ibc_actions: Vec<IbcRelay> = tracked_msgs
        .msgs
        .iter()
        .map(|msg| {
            let raw = ProtoIbcRelay {
                raw_action: Some(pbjson_types::Any {
                    type_url: msg.type_url.clone(),
                    value: msg.value.clone().into(),
                }),
            };
            IbcRelay::try_from(raw).map_err(|e| PenumbraError::IbcRelayConversion(e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Retry loop: if a witness fails because a note was forgotten between
    // the last block commit and our plan attempt (the narrow window that
    // the tx_build_lock cannot fully close), release the lock so the sync
    // worker can process more blocks, then re-plan with fresh notes.
    for attempt in 1..=MAX_WITNESS_RETRIES {
        // Pause the sync worker for the entire plan+witness+build cycle.
        // SAFETY: Do not wrap this section in tokio::select! or timeout —
        // dropping the lock mid-build leaves the planner's selected notes
        // stale, and the next attempt would pick the same (now invalid)
        // notes. The lock must be held until build_transaction completes
        // or we explicitly drop it before retrying.
        let _sync_pause = tx_build_lock.write().await;

        let mut planner = Planner::new(OsRng);
        planner.set_gas_prices(gas_prices.clone()).set_fee_tier(fee_tier);
        for action in &ibc_actions {
            planner.ibc_action(action.clone());
        }

        let plan = planner
            .plan(view_client, AddressIndex::new(0))
            .await
            .map_err(|e| PenumbraError::TxBuild {
                reason: format!("planner failed: {}", e),
            })?;

        match penumbra_sdk_wallet::build_transaction(fvk, view_client, custody_client, plan).await {
            Ok(tx) => {
                drop(_sync_pause);
                return Ok(tx);
            }
            Err(e) => {
                let err_msg = e.to_string();
                // Release lock so sync worker can process blocks and update
                // the SCT + SQL state before we retry.
                drop(_sync_pause);

                if err_msg.contains("Note commitment missing")
                    || err_msg.contains("commitment must be witnessed")
                {
                    if attempt < MAX_WITNESS_RETRIES {
                        tracing::warn!(
                            attempt,
                            "witness failed for selected note, releasing lock and retrying \
                             with fresh notes after sync catches up"
                        );
                        // Give the sync worker time to process pending blocks
                        // so the stale note gets marked spent in SQL.
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        continue;
                    }
                    tracing::error!(
                        "witness failed after {} attempts, note selection still stale",
                        MAX_WITNESS_RETRIES
                    );
                }
                return Err(PenumbraError::TxBuild {
                    reason: format!("build_transaction failed: {}", e),
                });
            }
        }
    }

    unreachable!("loop always returns")
}

/// Broadcast a built transaction via the ViewClient and wait for confirmation.
///
/// When `await_commit` is true, waits for the transaction to be included in a block.
/// When false, returns as soon as the broadcast succeeds (mempool acceptance).
pub async fn submit_transaction(
    view_client: &mut ViewServiceClient<BoxGrpcService>,
    transaction: Transaction,
    await_commit: bool,
) -> Result<TransactionId, PenumbraError> {
    info!("broadcasting penumbra transaction and awaiting confirmation...");
    let mut rsp = ViewClient::broadcast_transaction(view_client, transaction, await_commit)
        .await
        .map_err(|e| PenumbraError::TxBroadcast(e))?;

    let id = (async move {
        while let Some(rsp) = rsp.try_next().await? {
            match rsp.status {
                Some(status) => match status {
                    BroadcastStatus::BroadcastSuccess(bs) => {
                        if !await_commit {
                            return bs
                                .id
                                .ok_or_else(|| anyhow::anyhow!("BroadcastSuccess response missing transaction id"))?
                                .try_into();
                        }
                    }
                    BroadcastStatus::Confirmed(c) => {
                        let id = c.id
                            .ok_or_else(|| anyhow::anyhow!("Confirmed response missing transaction id"))?
                            .try_into()?;
                        info!(id = %id, "penumbra transaction confirmed");
                        return Ok(id);
                    }
                },
                None => {
                    return Err(anyhow::anyhow!(
                        "empty BroadcastTransactionResponse message"
                    ));
                }
            }
        }

        Err(anyhow::anyhow!(
            "should have received BroadcastTransaction status or error"
        ))
    }
    .boxed())
    .await
    .map_err(|e| {
        tracing::error!("error awaiting transaction broadcast: {}", e);
        e
    })
    .context("broadcast_transaction failed")
    .map_err(|e| PenumbraError::TxBroadcast(e))?;

    Ok(id)
}

/// P0 anchor-latency fix.
///
/// Plan -> build (witness+prove) -> submit, executed with `tx_build_lock`
/// held *continuously* across all three, and submit issued *immediately*
/// after build with nothing in between (no lock release, no re-plan, no
/// re-prove, no second view_client round-trip).
///
/// Rationale: a Penumbra spend proof's public inputs include the SCT
/// anchor, so a stale anchor CANNOT be repaired by re-witnessing without
/// re-proving. The only sound mitigation is to (a) minimize the wall-clock
/// between anchor capture (inside `build_transaction`'s witness step) and
/// the chain's `check_stateful`, and (b) stop the local sync worker from
/// forgetting the witnessed note mid-flight. Holding the lock across
/// build+submit does both. Deliberately re-staling via a backoff/rebuild
/// loop (the old MAX_SCT_RETRIES path) can never converge for a
/// high-latency tx and is therefore removed.
pub async fn build_and_submit_penumbra_tx(
    view_client: &mut ViewServiceClient<BoxGrpcService>,
    custody_client: &mut CustodyServiceClient<BoxGrpcService>,
    fvk: &FullViewingKey,
    tracked_msgs: TrackedMsgs,
    tx_build_lock: &Arc<RwLock<()>>,
    await_commit: bool,
) -> Result<TransactionId, PenumbraError> {
    let gas_prices: penumbra_sdk_fee::GasPrices = view_client
        .gas_prices(GasPricesRequest {})
        .await
        .map_err(|e| PenumbraError::ViewService {
            operation: "gas_prices",
            source: anyhow::anyhow!("{}", e),
        })?
        .into_inner()
        .gas_prices
        .ok_or(PenumbraError::GasPricesUnavailable)?
        .try_into()
        .map_err(|e: anyhow::Error| PenumbraError::TxBuild {
            reason: format!("failed to parse gas prices: {}", e),
        })?;

    let fee_tier = FeeTier::default();

    let ibc_actions: Vec<IbcRelay> = tracked_msgs
        .msgs
        .iter()
        .map(|msg| {
            let raw = ProtoIbcRelay {
                raw_action: Some(pbjson_types::Any {
                    type_url: msg.type_url.clone(),
                    value: msg.value.clone().into(),
                }),
            };
            IbcRelay::try_from(raw).map_err(|e| PenumbraError::IbcRelayConversion(e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    for attempt in 1..=MAX_WITNESS_RETRIES {
        // Hold the sync-pause across plan + witness + build + SUBMIT.
        // Do NOT drop before submit: the anchor is captured during
        // build_transaction's witness step; releasing here lets the local
        // view / chain advance and stales the anchor before check_stateful.
        let _sync_pause = tx_build_lock.write().await;

        let mut planner = Planner::new(OsRng);
        planner.set_gas_prices(gas_prices.clone()).set_fee_tier(fee_tier);
        for action in &ibc_actions {
            planner.ibc_action(action.clone());
        }

        let plan = planner
            .plan(view_client, AddressIndex::new(0))
            .await
            .map_err(|e| PenumbraError::TxBuild {
                reason: format!("planner failed: {}", e),
            })?;

        match penumbra_sdk_wallet::build_transaction(fvk, view_client, custody_client, plan).await {
            Ok(tx) => {
                // Submit immediately, still holding the lock. Smallest
                // possible anchor -> check_stateful window. No re-plan,
                // no re-prove, no backoff between build and submit.
                let res = submit_transaction(view_client, tx, await_commit).await;
                drop(_sync_pause);
                return res;
            }
            Err(e) => {
                let err_msg = e.to_string();
                drop(_sync_pause);
                if (err_msg.contains("Note commitment missing")
                    || err_msg.contains("commitment must be witnessed"))
                    && attempt < MAX_WITNESS_RETRIES
                {
                    tracing::warn!(
                        attempt,
                        "witness failed for selected note, retrying with fresh \
                         notes after sync catches up"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
                return Err(PenumbraError::TxBuild {
                    reason: format!("build_transaction failed: {}", e),
                });
            }
        }
    }

    unreachable!("loop always returns")
}
