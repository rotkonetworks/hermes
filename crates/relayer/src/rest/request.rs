use serde::{Deserialize, Serialize};

use ibc_relayer_types::core::ics04_channel::packet::Sequence;
use ibc_relayer_types::core::ics24_host::identifier::ChainId;

use crate::{
    config::ChainConfig,
    relay_events::{RelayEvent, RelayStats},
    rest::RestApiError,
    supervisor::dump_state::SupervisorState,
};

pub type ReplySender<T> = crossbeam_channel::Sender<Result<T, RestApiError>>;
pub type ReplyReceiver<T> = crossbeam_channel::Receiver<Result<T, RestApiError>>;

pub fn reply_channel<T>() -> (ReplySender<T>, ReplyReceiver<T>) {
    crossbeam_channel::bounded(1)
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct VersionInfo {
    pub name: String,
    pub version: String,
}

/// REST API request variants
#[derive(Clone, Debug)]
pub enum Request {
    Version {
        reply_to: ReplySender<VersionInfo>,
    },

    State {
        reply_to: ReplySender<SupervisorState>,
    },

    GetChains {
        reply_to: ReplySender<Vec<ChainId>>,
    },

    GetChain {
        chain_id: ChainId,
        reply_to: ReplySender<ChainConfig>,
    },

    ClearPackets {
        chain_id: Option<ChainId>,
        reply_to: ReplySender<()>,
    },

    GetHistory {
        limit: usize,
        chain_filter: Option<String>,
        reply_to: ReplySender<Vec<RelayEvent>>,
    },

    GetStats {
        reply_to: ReplySender<RelayStats>,
    },

    GetPending {
        chain_id: Option<ChainId>,
        reply_to: ReplySender<Vec<ChannelPending>>,
    },

    GetBalances {
        reply_to: ReplySender<Vec<ChainBalance>>,
    },
}

/// Balance for a single chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChainBalance {
    pub chain_id: ChainId,
    pub address: String,
    pub balance: String,
    pub denom: String,
}

/// Pending packets for a single channel direction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelPending {
    pub src_chain: ChainId,
    pub dst_chain: ChainId,
    pub port: String,
    pub channel: String,
    pub unreceived: Vec<Sequence>,
    pub unreceived_acks: Vec<Sequence>,
}
