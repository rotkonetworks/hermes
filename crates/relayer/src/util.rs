mod block_on;
pub use block_on::{block_on, spawn_blocking};

pub mod collate;
pub mod compat_mode;
pub mod debug_section;
pub mod diff;
pub mod excluded_sequences;
pub mod iter;
pub mod lock;
pub mod pretty;
pub mod profiling;
pub mod queue;
pub mod retry;
pub mod seq_range;
pub mod stream;
pub mod task;

/// Process-global pool of `tonic::transport::Channel`s, keyed by gRPC URI.
///
/// **Why this exists:** the previous implementation of `create_grpc_client`
/// opened a brand-new `tonic::transport::Channel` (and underlying hyper
/// Client + TCP/TLS connection pool) on every gRPC call. Hermes performs
/// dozens of gRPC queries per second across ~30 distinct call sites
/// (`query_client_state`, `query_consensus_state`, `query_packet_commitments`,
/// etc.). Under load this generated thousands of dropped Channels per
/// minute, and hyper's background `ConnectionPool` cleanup task could not
/// keep up — sockets in TIME_WAIT/CLOSE_WAIT accumulated, hyper's internal
/// state retained per-connection metadata, and RSS climbed by gigabytes
/// every few minutes.
///
/// `tonic::transport::Channel` is internally `Arc`-shared; cloning is cheap
/// and the underlying HTTP/2 connection pool is shared across clones. So
/// caching the Channel per URI is safe and lets every call site reuse the
/// same upstream connection(s).
///
/// See `LEAK_ANALYSIS.md` "High-confidence: tonic Channel / HTTP/2
/// connection retention" — this fix is the targeted remediation for that
/// suspect surface.
static GRPC_CHANNEL_POOL: once_cell::sync::Lazy<
    std::sync::RwLock<std::collections::HashMap<String, tonic::transport::Channel>>,
> = once_cell::sync::Lazy::new(|| std::sync::RwLock::new(std::collections::HashMap::new()));

/// Helper function to create a gRPC client.
///
/// Reuses an existing `tonic::transport::Channel` for the given `grpc_addr`
/// from a process-global pool when available; otherwise opens a fresh
/// Channel and inserts it into the pool for subsequent callers.
pub async fn create_grpc_client<T>(
    grpc_addr: &tonic::transport::Uri,
    client_constructor: impl FnOnce(tonic::transport::Channel) -> T,
) -> Result<T, crate::error::Error> {
    let key = grpc_addr.to_string();

    // Fast path: pool hit. Hold the read lock only long enough to clone
    // the Channel (cheap, Arc-internal).
    if let Some(channel) = GRPC_CHANNEL_POOL
        .read()
        .ok()
        .and_then(|m| m.get(&key).cloned())
    {
        return Ok(client_constructor(channel));
    }

    // Slow path: open a new Channel, insert into the pool, return a clone.
    let builder = tonic::transport::Channel::builder(grpc_addr.clone());

    // Don't configures TLS for the endpoint if using IPv6
    let builder = if grpc_addr.scheme() == Some(&http::uri::Scheme::HTTPS) {
        let domain = grpc_addr
            .host()
            .map(|d| d.replace(['[', ']'], ""))
            .ok_or_else(|| crate::error::Error::invalid_http_host(grpc_addr.to_string()))?;
        let tls_config = tonic::transport::ClientTlsConfig::new()
            .with_native_roots()
            .domain_name(domain);
        builder
            .tls_config(tls_config)
            .map_err(crate::error::Error::grpc_transport)?
    } else {
        builder
    };

    let channel = builder
        .connect()
        .await
        .map_err(crate::error::Error::grpc_transport)?;

    // Insert into pool. A concurrent caller may have raced us — that's
    // fine, they inserted an equivalent Channel; HashMap::insert keeps
    // the newest one and the older Channel is dropped (cheap, no
    // connections close because nothing's holding it). On future calls
    // everyone converges on a single shared Channel per URI.
    if let Ok(mut pool) = GRPC_CHANNEL_POOL.write() {
        pool.insert(key, channel.clone());
    }

    Ok(client_constructor(channel))
}
