//! Penumbra transaction building and submission.
//!
//! Standalone async functions that separate the transaction lifecycle
//! (plan -> build -> sign -> submit -> confirm) from the ChainEndpoint impl,
//! making each step independently testable and composable.

use anyhow::Context;
use futures::{FutureExt, TryStreamExt};
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
pub async fn build_penumbra_tx(
    view_client: &mut ViewServiceClient<BoxGrpcService>,
    custody_client: &mut CustodyServiceClient<BoxGrpcService>,
    fvk: &FullViewingKey,
    tracked_msgs: TrackedMsgs,
) -> Result<Transaction, PenumbraError> {
    let gas_prices = view_client
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

    let mut planner = Planner::new(OsRng);
    planner.set_gas_prices(gas_prices).set_fee_tier(fee_tier);

    for msg in tracked_msgs.msgs {
        let raw_ibcrelay_msg = ProtoIbcRelay {
            raw_action: Some(pbjson_types::Any {
                type_url: msg.type_url.clone(),
                value: msg.value.clone().into(),
            }),
        };
        let ibc_action =
            IbcRelay::try_from(raw_ibcrelay_msg).map_err(|e| PenumbraError::IbcRelayConversion(e))?;
        planner.ibc_action(ibc_action);
    }

    let plan = planner
        .plan(view_client, AddressIndex::new(0))
        .await
        .map_err(|e| PenumbraError::TxBuild {
            reason: format!("planner failed: {}", e),
        })?;

    penumbra_sdk_wallet::build_transaction(fvk, view_client, custody_client, plan)
        .await
        .map_err(|e| PenumbraError::TxBuild {
            reason: format!("build_transaction failed: {}", e),
        })
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
