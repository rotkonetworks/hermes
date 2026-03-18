//! Penumbra-specific error types with full causal chain preserved.
//!
//! Replaces the catch-all `temp_penumbra_error` pattern with structured errors
//! that provide actionable diagnostics.

use crate::error::Error;

/// Penumbra-specific errors.
#[derive(Debug, thiserror::Error)]
pub enum PenumbraError {
    #[error("gRPC transport error connecting to {endpoint}")]
    GrpcTransport {
        endpoint: String,
        #[source]
        source: tonic::transport::Error,
    },

    #[error("gRPC query `{method}` failed: {source}")]
    GrpcStatus {
        method: &'static str,
        #[source]
        source: tonic::Status,
    },

    #[error("view service error during {operation}")]
    ViewService {
        operation: &'static str,
        #[source]
        source: anyhow::Error,
    },

    #[error("transaction build failed: {reason}")]
    TxBuild { reason: String },

    #[error("transaction broadcast failed")]
    TxBroadcast(#[source] anyhow::Error),

    #[error("proof decoding failed for query `{query}`")]
    ProofDecode {
        query: &'static str,
        #[source]
        source: prost::DecodeError,
    },

    #[error("empty proof in response for query `{query}`")]
    EmptyProof { query: &'static str },

    #[error("response missing expected field: {field}")]
    MissingField { field: &'static str },

    #[error("gas prices not available from view service")]
    GasPricesUnavailable,

    #[error("IBC relay message conversion failed")]
    IbcRelayConversion(#[source] anyhow::Error),
}

impl PenumbraError {
    /// Convert into the shared hermes Error type for the ChainEndpoint boundary.
    pub fn into_error(self) -> Error {
        Error::temp_penumbra_error(self.to_string())
    }
}

impl From<PenumbraError> for Error {
    fn from(e: PenumbraError) -> Self {
        e.into_error()
    }
}
