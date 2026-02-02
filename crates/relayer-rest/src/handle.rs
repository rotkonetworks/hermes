use core::fmt::Debug;

use tracing::error;

use crossbeam_channel as channel;

use ibc_relayer::relay_events::{RelayEvent, RelayStats};
use ibc_relayer::supervisor::dump_state::SupervisorState;
use ibc_relayer::{
    config::ChainConfig,
    rest::{
        request::{reply_channel, ChainBalance, ChannelPending, ReplySender, Request, VersionInfo},
        RestApiError,
    },
};
use ibc_relayer_types::core::ics24_host::identifier::ChainId;

pub const NAME: &str = env!(
    "CARGO_PKG_NAME",
    "the env. variable CARGO_PKG_NAME of ibc-relayer-rest is not set!"
);
pub const VER: &str = env!(
    "CARGO_PKG_VERSION",
    "the env. variable CARGO_PKG_VERSION of ibc-relayer-rest is not set!"
);

fn submit_request<F, O>(request_sender: &channel::Sender<Request>, f: F) -> Result<O, RestApiError>
where
    F: FnOnce(ReplySender<O>) -> Request,
    O: Debug,
{
    let (reply_sender, reply_receiver) = reply_channel();

    // Construct the request, which is simply an enum variant
    let req = f(reply_sender);

    // Send the request
    request_sender
        .send(req)
        .map_err(|e| RestApiError::ChannelSend(e.to_string()))?;

    // Wait for the reply
    reply_receiver
        .recv()
        .map_err(|e| RestApiError::ChannelRecv(e.to_string()))?
}

pub fn all_chain_ids(sender: &channel::Sender<Request>) -> Result<Vec<ChainId>, RestApiError> {
    submit_request(sender, |reply_to| Request::GetChains { reply_to })
}

pub fn chain_config(
    sender: &channel::Sender<Request>,
    chain_id: &str,
) -> Result<ChainConfig, RestApiError> {
    submit_request(sender, |reply_to| Request::GetChain {
        chain_id: ChainId::from_string(chain_id),
        reply_to,
    })
}

pub fn supervisor_state(
    sender: &channel::Sender<Request>,
) -> Result<SupervisorState, RestApiError> {
    submit_request(sender, |reply_to| Request::State { reply_to })
}

/// Submit a request to clear all packets for the chain with the
/// specified `chain_id`.
pub fn trigger_clear_packets(
    sender: &channel::Sender<Request>,
    chain_id: Option<ChainId>,
) -> Result<(), RestApiError> {
    submit_request(sender, |reply_to| Request::ClearPackets {
        chain_id,
        reply_to,
    })
}

/// Get relay history with optional chain filter
pub fn get_history(
    sender: &channel::Sender<Request>,
    limit: usize,
    chain_filter: Option<String>,
) -> Result<Vec<RelayEvent>, RestApiError> {
    submit_request(sender, |reply_to| Request::GetHistory {
        limit,
        chain_filter,
        reply_to,
    })
}

/// Get relay statistics
pub fn get_stats(sender: &channel::Sender<Request>) -> Result<RelayStats, RestApiError> {
    submit_request(sender, |reply_to| Request::GetStats { reply_to })
}

/// Get pending packets for all channels
pub fn get_pending(
    sender: &channel::Sender<Request>,
    chain_id: Option<ChainId>,
) -> Result<Vec<ChannelPending>, RestApiError> {
    submit_request(sender, |reply_to| Request::GetPending {
        chain_id,
        reply_to,
    })
}

/// Get balances for all chains
pub fn get_balances(sender: &channel::Sender<Request>) -> Result<Vec<ChainBalance>, RestApiError> {
    submit_request(sender, |reply_to| Request::GetBalances { reply_to })
}

pub fn assemble_version_info(sender: &channel::Sender<Request>) -> Vec<VersionInfo> {
    // Fetch the relayer library version
    let lib_version = submit_request(sender, |reply_to| Request::Version { reply_to })
        .map_err(|e| {
            error!(
                "[rest-server] failed while fetching relayer lib version info: {}",
                e
            )
        })
        .unwrap_or(VersionInfo {
            name: "[ibc relayer library]".to_string(),
            version: "[failed to fetch the version]".to_string(),
        });
    // Append the REST API version info
    let rest_api_version = VersionInfo {
        name: NAME.to_string(),
        version: VER.to_string(),
    };

    vec![lib_version, rest_api_version]
}
