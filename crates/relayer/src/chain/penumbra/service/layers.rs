//! Tower layers for cross-cutting concerns in Penumbra IBC queries.
//!
//! Each layer addresses one concern that was previously scattered across every
//! query method:
//!
//! - **`HeightMetadataLayer`** — injects `QueryHeight` into gRPC metadata.
//! - **`ProofDecodingLayer`** — decodes raw proof bytes into `MerkleProof`.
//! - **`TracingLayer`** — emits structured tracing spans per query.
//! - **`RetryLayer`** — retries transient gRPC errors with exponential backoff.
//! - **`TimeoutLayer`** — wraps each call with a deadline.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use ibc_relayer_types::core::ics23_commitment::merkle::MerkleProof;
use tower::{Layer, Service};
use tracing::Instrument;

use crate::chain::requests::IncludeProof;
use crate::error::Error;

use super::{IbcQuery, IbcQueryResponse};
use crate::chain::penumbra::query;

// ---------------------------------------------------------------------------
// HeightMetadataLayer
// ---------------------------------------------------------------------------

/// A tower `Layer` that extracts the `QueryHeight` from `IbcQuery` variants
/// and ensures the inner service receives requests with height metadata set.
///
/// Because the gRPC metadata injection happens at the tonic `Request` level
/// (inside the leaf service), this layer currently acts as a **marker** that
/// validates the height is present. The actual `set_height_metadata` call
/// lives in `PenumbraGrpcQueryService::call` where the tonic `Request` is
/// constructed. This layer is a compositional hook point for future
/// middleware that needs pre-call height access (e.g. caching by height).
#[derive(Debug, Clone)]
pub struct HeightMetadataLayer;

impl<S> Layer<S> for HeightMetadataLayer {
    type Service = HeightMetadataService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HeightMetadataService { inner }
    }
}

/// Service produced by [`HeightMetadataLayer`].
///
/// Forwards the `IbcQuery` to the inner service. The height information
/// is accessible via `IbcQuery::query_height()` for any layer that needs it.
#[derive(Debug, Clone)]
pub struct HeightMetadataService<S> {
    inner: S,
}

impl<S> Service<IbcQuery> for HeightMetadataService<S>
where
    S: Service<IbcQuery, Response = IbcQueryResponse, Error = Error> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = IbcQueryResponse;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: IbcQuery) -> Self::Future {
        // Log the height being used for this query at trace level.
        if let Some(height) = req.query_height() {
            let height_str = query::format_height(height);
            tracing::trace!(
                query = req.name(),
                height = %height_str,
                "injecting height metadata"
            );
        }

        let fut = self.inner.call(req);
        Box::pin(fut)
    }
}

// ---------------------------------------------------------------------------
// ProofDecodingLayer
// ---------------------------------------------------------------------------

/// A tower `Layer` that decodes raw proof bytes in `IbcQueryResponse`
/// variants into `MerkleProof` when `IncludeProof::Yes` was requested.
///
/// The decoded proof is **not** stored back into the protobuf response
/// (which only carries `Vec<u8>`). Instead, it is made available through
/// `ProofDecodingResponse`, which pairs the raw response with an optional
/// decoded proof.
#[derive(Debug, Clone)]
pub struct ProofDecodingLayer;

impl<S> Layer<S> for ProofDecodingLayer {
    type Service = ProofDecodingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ProofDecodingService { inner }
    }
}

/// Response wrapper carrying the raw `IbcQueryResponse` alongside an
/// optionally-decoded `MerkleProof`.
#[derive(Debug)]
pub struct ProofDecodingResponse {
    /// The original raw protobuf response.
    pub response: IbcQueryResponse,
    /// The decoded Merkle proof, if one was requested and present.
    pub proof: Option<MerkleProof>,
}

/// Service produced by [`ProofDecodingLayer`].
#[derive(Debug, Clone)]
pub struct ProofDecodingService<S> {
    inner: S,
}

impl<S> Service<IbcQuery> for ProofDecodingService<S>
where
    S: Service<IbcQuery, Response = IbcQueryResponse, Error = Error> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = ProofDecodingResponse;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: IbcQuery) -> Self::Future {
        let include_proof = extract_include_proof(&req);
        let query_name = req.name();
        let fut = self.inner.call(req);

        Box::pin(async move {
            let response = fut.await?;

            let proof = match include_proof {
                Some(IncludeProof::Yes) => {
                    let raw_bytes = extract_proof_bytes(&response);
                    if raw_bytes.is_empty() {
                        None
                    } else {
                        Some(query::decode_proof(raw_bytes, query_name)?)
                    }
                }
                _ => None,
            };

            Ok(ProofDecodingResponse { response, proof })
        })
    }
}

/// Extract the `IncludeProof` flag from an `IbcQuery`, if present.
fn extract_include_proof(req: &IbcQuery) -> Option<IncludeProof> {
    match req {
        IbcQuery::ClientState(_, ip)
        | IbcQuery::ConsensusState(_, ip)
        | IbcQuery::Connection(_, ip)
        | IbcQuery::Channel(_, ip)
        | IbcQuery::PacketCommitment(_, ip)
        | IbcQuery::PacketReceipt(_, ip)
        | IbcQuery::PacketAcknowledgement(_, ip)
        | IbcQuery::NextSequenceReceive(_, ip) => Some(*ip),
        _ => None,
    }
}

/// Extract the raw proof bytes from an `IbcQueryResponse`.
///
/// Returns an empty vec for variants that never carry proofs.
fn extract_proof_bytes(resp: &IbcQueryResponse) -> Vec<u8> {
    match resp {
        IbcQueryResponse::ClientState(r) => r.proof.clone(),
        IbcQueryResponse::ConsensusState(r) => r.proof.clone(),
        IbcQueryResponse::Connection(r) => r.proof.clone(),
        IbcQueryResponse::Channel(r) => r.proof.clone(),
        IbcQueryResponse::PacketCommitment(r) => r.proof.clone(),
        IbcQueryResponse::PacketReceipt(r) => r.proof.clone(),
        IbcQueryResponse::PacketAcknowledgement(r) => r.proof.clone(),
        IbcQueryResponse::NextSequenceReceive(r) => r.proof.clone(),
        // These variants never carry proofs.
        IbcQueryResponse::ClientStates(_)
        | IbcQueryResponse::ConsensusStateHeights(_)
        | IbcQueryResponse::Connections(_)
        | IbcQueryResponse::ConnectionChannels(_)
        | IbcQueryResponse::Channels(_)
        | IbcQueryResponse::ChannelClientState(_)
        | IbcQueryResponse::PacketCommitments(_)
        | IbcQueryResponse::UnreceivedPackets(_)
        | IbcQueryResponse::PacketAcknowledgements(_)
        | IbcQueryResponse::UnreceivedAcks(_) => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// TracingLayer
// ---------------------------------------------------------------------------

/// A tower `Layer` that emits structured `tracing` spans for each IBC query.
///
/// Replaces the `crate::time!` and `crate::telemetry!` macros with a
/// composable, per-request span that includes the query name and chain id.
#[derive(Debug, Clone)]
pub struct TracingLayer {
    chain_id: String,
}

impl TracingLayer {
    /// Create a new tracing layer bound to the given chain identifier.
    pub fn new(chain_id: impl Into<String>) -> Self {
        Self {
            chain_id: chain_id.into(),
        }
    }
}

impl<S> Layer<S> for TracingLayer {
    type Service = TracingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TracingService {
            inner,
            chain_id: self.chain_id.clone(),
        }
    }
}

/// Service produced by [`TracingLayer`].
#[derive(Debug, Clone)]
pub struct TracingService<S> {
    inner: S,
    chain_id: String,
}

impl<S> Service<IbcQuery> for TracingService<S>
where
    S: Service<IbcQuery, Response = IbcQueryResponse, Error = Error> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = IbcQueryResponse;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: IbcQuery) -> Self::Future {
        let query_name = req.name();
        let span = tracing::info_span!(
            "ibc_query",
            query = query_name,
            chain = %self.chain_id,
        );

        let fut = self.inner.call(req);
        Box::pin(fut.instrument(span))
    }
}

// ---------------------------------------------------------------------------
// RetryLayer
// ---------------------------------------------------------------------------

/// Configuration for retry behaviour on transient gRPC errors.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Initial delay before the first retry.
    pub initial_backoff: Duration,
    /// Maximum delay between retries (caps the exponential growth).
    pub max_backoff: Duration,
    /// Maximum number of retry attempts (not counting the original call).
    pub max_retries: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            max_retries: 3,
        }
    }
}

/// Transient gRPC status codes that warrant a retry.
///
/// We match on substrings of the error's `Display` output because the
/// underlying `tonic::Status` is converted to a stringly-typed
/// `Error::TempPenumbraError` before it reaches this layer.
const TRANSIENT_PATTERNS: &[&str] = &[
    "Unavailable",
    "DeadlineExceeded",
    "Aborted",
    // Internal can be transient (server-side hiccup).
    "Internal",
];

/// Returns `true` when the error looks like a transient gRPC failure that
/// is worth retrying.
fn is_transient(err: &Error) -> bool {
    let msg = err.to_string();
    TRANSIENT_PATTERNS.iter().any(|p| msg.contains(p))
}

/// A tower `Layer` that retries transient gRPC errors with exponential
/// backoff.
///
/// Only errors whose `Display` representation contains a known transient
/// gRPC status code (Unavailable, DeadlineExceeded, Aborted, Internal) are
/// retried. Permanent errors (NotFound, InvalidArgument, PermissionDenied,
/// etc.) are returned immediately.
#[derive(Debug, Clone)]
pub struct RetryLayer {
    policy: RetryPolicy,
}

impl RetryLayer {
    /// Create a retry layer with the given policy.
    pub fn new(policy: RetryPolicy) -> Self {
        Self { policy }
    }
}

impl<S> Layer<S> for RetryLayer {
    type Service = RetryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RetryService {
            inner,
            policy: self.policy.clone(),
        }
    }
}

/// Service produced by [`RetryLayer`].
#[derive(Debug, Clone)]
pub struct RetryService<S> {
    inner: S,
    policy: RetryPolicy,
}

impl<S> Service<IbcQuery> for RetryService<S>
where
    S: Service<IbcQuery, Response = IbcQueryResponse, Error = Error> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = IbcQueryResponse;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: IbcQuery) -> Self::Future {
        let mut svc = self.inner.clone();
        let policy = self.policy.clone();

        Box::pin(async move {
            let mut backoff = policy.initial_backoff;
            let mut last_err: Option<Error> = None;

            for attempt in 0..=policy.max_retries {
                // On retries we need to wait before calling again.
                if attempt > 0 {
                    let err_ref = last_err.as_ref().expect("last_err set on retry");
                    tracing::warn!(
                        query = req.name(),
                        attempt = attempt,
                        max_retries = policy.max_retries,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %err_ref,
                        "retrying transient gRPC error"
                    );
                    tokio::time::sleep(backoff).await;
                    // Exponential backoff capped at max.
                    backoff = std::cmp::min(backoff * 2, policy.max_backoff);
                }

                match svc.call(req.clone()).await {
                    Ok(resp) => return Ok(resp),
                    Err(err) => {
                        if !is_transient(&err) || attempt == policy.max_retries {
                            return Err(err);
                        }
                        last_err = Some(err);
                    }
                }
            }

            // Unreachable: the loop always returns.
            Err(last_err.unwrap())
        })
    }
}

// ---------------------------------------------------------------------------
// TimeoutLayer
// ---------------------------------------------------------------------------

/// A tower `Layer` that wraps each service call with a deadline.
///
/// If the inner service does not complete within the configured duration,
/// the call returns an error instead of blocking indefinitely.
#[derive(Debug, Clone)]
pub struct TimeoutLayer {
    duration: Duration,
}

impl TimeoutLayer {
    /// Create a timeout layer with the given deadline duration.
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl<S> Layer<S> for TimeoutLayer {
    type Service = TimeoutService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TimeoutService {
            inner,
            duration: self.duration,
        }
    }
}

/// Service produced by [`TimeoutLayer`].
#[derive(Debug, Clone)]
pub struct TimeoutService<S> {
    inner: S,
    duration: Duration,
}

impl<S> Service<IbcQuery> for TimeoutService<S>
where
    S: Service<IbcQuery, Response = IbcQueryResponse, Error = Error> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = IbcQueryResponse;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: IbcQuery) -> Self::Future {
        let query_name = req.name();
        let duration = self.duration;
        let fut = self.inner.call(req);

        Box::pin(async move {
            match tokio::time::timeout(duration, fut).await {
                Ok(result) => result,
                Err(_elapsed) => {
                    tracing::warn!(
                        query = query_name,
                        timeout_ms = duration.as_millis() as u64,
                        "query timed out"
                    );
                    Err(Error::temp_penumbra_error(format!(
                        "gRPC query `{}` timed out after {}ms",
                        query_name,
                        duration.as_millis()
                    )))
                }
            }
        })
    }
}
