pub mod broadcast_error;
pub mod encoder;
mod path_identifier;
pub mod server;
pub mod simulate_error;
pub mod state;

use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::Range;
use std::sync::Arc;

use once_cell::sync::OnceCell;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

pub use crate::state::TelemetryState;

pub fn new_state(
    tx_latency_submitted_range: Range<u64>,
    tx_latency_submitted_buckets: u64,
    tx_latency_confirmed_range: Range<u64>,
    tx_latency_confirmed_buckets: u64,
    namespace: &str,
) -> Arc<TelemetryState> {
    Arc::new(TelemetryState::new(
        tx_latency_submitted_range,
        tx_latency_submitted_buckets,
        tx_latency_confirmed_range,
        tx_latency_confirmed_buckets,
        namespace,
    ))
}

static GLOBAL_STATE: OnceCell<Arc<TelemetryState>> = OnceCell::new();

pub fn init(
    tx_latency_submitted_range: Range<u64>,
    tx_latency_submitted_buckets: u64,
    tx_latency_confirmed_range: Range<u64>,
    tx_latency_confirmed_buckets: u64,
    namespace: &str,
) -> &'static Arc<TelemetryState> {
    let new_state = new_state(
        tx_latency_submitted_range,
        tx_latency_submitted_buckets,
        tx_latency_confirmed_range,
        tx_latency_confirmed_buckets,
        namespace,
    );
    match GLOBAL_STATE.set(new_state) {
        Ok(_) => debug!("initialised telemetry global state"),
        Err(_) => debug!("telemetry global state was already set"),
    }
    GLOBAL_STATE.get().unwrap()
}

pub fn global() -> &'static Arc<TelemetryState> {
    match GLOBAL_STATE.get() {
        Some(state) => state,
        None => {
            warn!(
                "global telemetry state not set, will initialize it using default histogram ranges"
            );
            init(
                Range {
                    start: 500,
                    end: 10000,
                },
                10,
                Range {
                    start: 1000,
                    end: 20000,
                },
                10,
                "",
            )
        }
    }
}

pub type BoxError = Box<dyn Error + Send + Sync>;

/// Dedicated tokio runtime that backs the telemetry HTTP server, so that
/// /metrics scrapes never share worker threads with the relayer's
/// CPU-heavy work (penumbra Groth16 proof building, tendermint
/// light-client header verification, chain-runtime dispatch).
///
/// Previously the server was spawned via `tokio::spawn` on the calling
/// runtime, which is the same runtime that services relay tasks. Under
/// load those tasks saturated worker threads, the HTTP server task
/// couldn't get scheduled, /metrics requests timed out at the watchdog's
/// 5–30s curl timeout, and the watchdog falsely declared "telemetry
/// unreachable" → restart hermes. Net effect: hermes was being restarted
/// not because it was broken, but because its monitoring channel was
/// blocked behind real work.
///
/// One worker thread on this dedicated runtime is enough: the HTTP
/// server handles a few scrapes per minute. Isolation is what matters.
static TELEMETRY_RUNTIME: once_cell::sync::OnceCell<tokio::runtime::Runtime> =
    once_cell::sync::OnceCell::new();

pub fn spawn<A>(
    addr: A,
    state: Arc<TelemetryState>,
) -> Result<(SocketAddr, JoinHandle<Result<(), BoxError>>), BoxError>
where
    A: ToSocketAddrs + Send + 'static,
{
    let addr = addr.to_socket_addrs()?.next().unwrap();

    let rt = TELEMETRY_RUNTIME.get_or_try_init(|| {
        // One worker thread is plenty for /metrics scrapes; the
        // important property is that it's a SEPARATE runtime from the
        // relayer's, so chain-runtime CPU work cannot starve the HTTP
        // server. Multi-thread (with worker_threads(1)) is used rather
        // than current_thread so the runtime auto-drives its own worker
        // — no need to manually block_on from an OS thread.
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("hermes-telemetry")
            .build()
    })?;

    let handle = rt.spawn(server::listen(addr, state));

    Ok((addr, handle))
}
