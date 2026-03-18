//! Shared query helpers for Penumbra gRPC queries.
//!
//! Eliminates the duplicated height-to-metadata and proof-decoding patterns
//! that appeared 8+ times across query methods.

use ibc_proto::ibc::core::commitment::v1::MerkleProof as RawMerkleProof;
use ibc_relayer_types::core::ics23_commitment::merkle::MerkleProof;
use prost::Message;

use crate::chain::requests::QueryHeight;
use crate::error::Error;

use super::error::PenumbraError;

/// Formats a `QueryHeight` into the string representation expected by Penumbra's
/// gRPC metadata header. Penumbra expects IBC format: `"<revision>-<height>"` for
/// specific heights, or `"0"` for latest.
///
/// This centralizes the height formatting that was previously duplicated in every
/// query method.
pub fn format_height(height: &QueryHeight) -> String {
    match height {
        QueryHeight::Latest => 0.to_string(),
        QueryHeight::Specific(h) => h.to_string(),
    }
}

/// Injects the height metadata into a tonic request.
pub fn set_height_metadata<T>(request: &mut tonic::Request<T>, height: &QueryHeight) {
    let height_str = format_height(height);
    request
        .metadata_mut()
        .insert("height", height_str.parse().expect("valid ASCII height string"));
}

/// Decodes raw proof bytes from a gRPC response into a `MerkleProof`.
///
/// Returns `None` if `include_proof` is `No`.
/// Returns `Err` if proof bytes are empty or fail to decode.
pub fn decode_proof(
    raw_proof_bytes: Vec<u8>,
    query_name: &'static str,
) -> Result<MerkleProof, Error> {
    if raw_proof_bytes.is_empty() {
        return Err(PenumbraError::EmptyProof { query: query_name }.into());
    }

    let raw_proof = RawMerkleProof::decode(raw_proof_bytes.as_ref())
        .map_err(|e| PenumbraError::ProofDecode {
            query: query_name,
            source: e,
        })?;

    Ok(raw_proof.into())
}

/// Decodes raw proof bytes, returning `Ok(None)` if the proof is empty
/// (for cases where proofs are optional).
pub fn maybe_decode_proof(
    raw_proof_bytes: Vec<u8>,
    query_name: &'static str,
) -> Result<Option<MerkleProof>, Error> {
    if raw_proof_bytes.is_empty() {
        return Ok(None);
    }

    let raw_proof = RawMerkleProof::decode(raw_proof_bytes.as_ref())
        .map_err(|e| PenumbraError::ProofDecode {
            query: query_name,
            source: e,
        })?;

    Ok(Some(raw_proof.into()))
}
