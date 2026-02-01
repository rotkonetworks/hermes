use crossbeam_channel::TryRecvError;
use ibc_relayer_types::core::ics24_host::identifier::ChainId;
use tracing::{error, trace};

use crate::{
    config::Config,
    relay_events::{get_relay_history, get_relay_stats},
    rest::request::{ChannelPending, ReplySender},
    rest::request::{Request, VersionInfo},
    supervisor::dump_state::SupervisorState,
};

pub mod request;

mod error;
pub use error::RestApiError;

pub const NAME: &str = env!(
    "CARGO_PKG_NAME",
    "the env. variable CARGO_PKG_NAME in ibc-relayer is not set!"
);
pub const VER: &str = env!(
    "CARGO_PKG_VERSION",
    "the env. variable CARGO_PKG_VERSION in ibc-relayer is not set!"
);

pub type Receiver = crossbeam_channel::Receiver<Request>;

// TODO: Unify this enum with `SupervisorCmd`
//  We won't unify yet as it is possible we will never implement
//  REST API `/chain` adding endpoint; instead of `/chain` we might
//  implement `/reload` for supporting a broader range of functionality
//  e.g., adjusting chain config, removing chains, etc.
pub enum Command {
    DumpState(ReplySender<SupervisorState>),
    ClearPackets(Option<ChainId>, ReplySender<()>),
    GetPending(Option<ChainId>, ReplySender<Vec<ChannelPending>>),
}

/// Process incoming REST requests.
///
/// Non-blocking receiving of requests from
/// the REST server, and tries to handle them locally.
///
/// Any request that cannot be handled locally here is propagated
/// as a [`Command`] to the caller, which the supervisor itself should handle.
pub fn process_incoming_requests(config: &Config, channel: &Receiver) -> Option<Command> {
    match channel.try_recv() {
        Ok(request) => match request {
            Request::Version { reply_to } => {
                trace!("Version");

                let v = VersionInfo {
                    name: NAME.to_string(),
                    version: VER.to_string(),
                };

                reply_to
                    .send(Ok(v))
                    .unwrap_or_else(|e| error!("error replying to a REST request {}", e));
            }

            Request::GetChains { reply_to } => {
                trace!("GetChains");

                reply_to
                    .send(Ok(config.chains.iter().map(|c| c.id().clone()).collect()))
                    .unwrap_or_else(|e| error!("error replying to a REST request {}", e));
            }

            Request::GetChain { chain_id, reply_to } => {
                trace!("GetChain {}", chain_id);

                let result = config
                    .find_chain(&chain_id)
                    .cloned()
                    .ok_or(RestApiError::ChainConfigNotFound(chain_id));

                reply_to
                    .send(result)
                    .unwrap_or_else(|e| error!("error replying to a REST request {}", e));
            }

            Request::State { reply_to } => {
                trace!("State");

                return Some(Command::DumpState(reply_to));
            }

            Request::ClearPackets { chain_id, reply_to } => {
                trace!("ClearPackets");

                return Some(Command::ClearPackets(chain_id, reply_to));
            }

            Request::GetHistory {
                limit,
                chain_filter,
                reply_to,
            } => {
                trace!("GetHistory limit={} chain={:?}", limit, chain_filter);

                let history = get_relay_history(limit, chain_filter.as_deref());
                reply_to
                    .send(Ok(history))
                    .unwrap_or_else(|e| error!("error replying to a REST request {}", e));
            }

            Request::GetStats { reply_to } => {
                trace!("GetStats");

                let stats = get_relay_stats();
                reply_to
                    .send(Ok(stats))
                    .unwrap_or_else(|e| error!("error replying to a REST request {}", e));
            }

            Request::GetPending { chain_id, reply_to } => {
                trace!("GetPending chain={:?}", chain_id);

                return Some(Command::GetPending(chain_id, reply_to));
            }
        },
        Err(e) => {
            if !matches!(e, TryRecvError::Empty) {
                error!("error while waiting for requests: {}", e);
            }
        }
    }

    None
}
