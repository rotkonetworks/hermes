//! This module defines the various errors that be raised in the relayer.

use core::time::Duration;

use flex_error::{define_error, DisplayOnly, TraceError};
use http::uri::InvalidUri;
use humantime::format_duration;
use ibc_relayer_types::{
    applications::{
        ics29_fee::error::Error as FeeError, ics31_icq::error::Error as CrossChainQueryError,
    },
    clients::ics07_tendermint::error as tendermint_error,
    core::{
        ics02_client::{client_type::ClientType, error as client_error},
        ics03_connection::error as connection_error,
        ics23_commitment::error as commitment_error,
        ics24_host::identifier::{ChainId, ChannelId, ConnectionId},
    },
    proofs::ProofError,
};
use prost::{DecodeError, EncodeError};
use regex::Regex;
use tendermint::{abci, Error as TendermintError};
use tendermint_light_client::{
    builder::error::Error as LightClientBuilderError,
    components::io::IoError as LightClientIoError,
    errors::{Error as LightClientError, ErrorDetail as LightClientErrorDetail},
};
use tendermint_proto::Error as TendermintProtoError;
use tendermint_rpc::{
    endpoint::{abci_query::AbciQuery, broadcast::tx_sync::Response as TxSyncResponse},
    Error as TendermintRpcError,
};
use tonic::{
    metadata::errors::InvalidMetadataValue, transport::Error as TransportError,
    Status as GrpcStatus,
};

use crate::{
    chain::cosmos::{version, BLOCK_MAX_BYTES_MAX_FRACTION},
    config::Error as ConfigError,
    event::source,
    keyring::{errors::Error as KeyringError, KeyType},
    sdk_error::SdkError,
};

define_error! {
    Error {
        OtherWithString
            { source: String }
            |_| { "other error" },

        Io
            [ TraceError<std::io::Error> ]
            |_| { "I/O error" },

        Rpc
            { url: tendermint_rpc::Url }
            [ TendermintRpcError ]
            |e| { format!("RPC error to endpoint {}", e.url) },

        AbciQuery
            { query: AbciQuery }
            |e| { format!("ABCI query returned an error: {:?}", e.query) },

        Config
            [ ConfigError ]
            |_| { "Configuration error" },

        CheckTx
            {
                response: TxSyncResponse,
            }
            | e | { format!("CheckTx returned an error: {:?}", e.response) },

        DeliverTx
            {
                detail: SdkError,
                tx: abci::response::DeliverTx,
            }
            |e| { format!("DeliverTx Commit returns error: {0}. RawResult: {1:?}", e.detail, e.tx) },

        SendTx
            {
                detail: String
            }
            |e| { format_args!("send_tx resulted in chain error event: {}", e.detail) },

        WebSocket
            { url: tendermint_rpc::Url }
            |e| { format!("Websocket error to endpoint {}", e.url) },

        EventSource
            [ source::Error ]
            |_| { "event source error" },

        Grpc
            |_| { "gRPC error" },

        GrpcStatus
            { status: GrpcStatus, query: String }
            |e| { format!("gRPC call `{}` failed with status: {1}", e.query, e.status) },

        GrpcTransport
            [ TraceError<TransportError> ]
            |_| { "error in underlying transport when making gRPC call" },

        GrpcResponseParam
            { param: String }
            |e| { format!("missing parameter in GRPC response: {}", e.param) },

        Decode
            [ TendermintProtoError ]
            |_| { "error decoding protobuf" },

        LightClientBuilder
            [ LightClientBuilderError ]
            |_| { "light client builder error" },

        LightClientVerification
            { chain_id: String }
            [ LightClientError ]
            |e| { format!("light client verification error for chain id {0}", e.chain_id) },

        LightClientState
            [ client_error::Error ]
            |_| { "light client encountered error due to client state".to_string() },

        LightClientIo
            { address: String }
            [ LightClientIoError ]
            |e| { format!("light client error for RPC address {0}", e.address) },

        ChainNotCaughtUp
            {
                address: String,
                chain_id: ChainId,
            }
            |e| { format!("node at {} running chain {} not caught up", e.address, e.chain_id) },

        PrivateStore
            |_| { "requested proof for a path in the private store" },

        Event
            |_| { "bad notification" },

        ConversionFromAny
            [ TendermintProtoError ]
            |_| { "conversion from a protobuf `Any` into a domain type failed" },

        EmptyUpgradedClientState
            |_| { "found no upgraded client state" },

        ConsensusStateTypeMismatch
            {
                expected: ClientType,
                got: ClientType,
            }
            |e| { format!("consensus state type mismatch; hint: expected client type '{0}', got '{1}'", e.expected, e.got) },

        EmptyResponseValue
            |_| { "empty response value" },

        EmptyResponseProof
            |_| { "empty response proof" },

        RpcResponse
            { detail: String }
            | e | { format!("RPC client returns error response: {}", e.detail) },

        MalformedProof
            [ ProofError ]
            |_| { "malformed proof" },

        InvalidHeight
            [ TendermintError ]
            |_| { "invalid height" },

        InvalidHeightNoSource
            |_| { "invalid height" },

        InvalidMetadata
            [ TraceError<InvalidMetadataValue> ]
            |_| { "invalid metadata" },

        BuildClientStateFailure
            |_| { "failed to create client state" },

        CreateClient
            { client_id: String }
            |e| { format!("failed to create client {0}", e.client_id) },

        ClientStateType
            { client_state_type: String }
            |e| { format!("unexpected client state type {0}", e.client_state_type) },

        ConnectionNotFound
            { connection_id: ConnectionId }
            |e| { format!("connection not found: {0}", e.connection_id) },

        BadConnectionState
            |_| { "bad connection state" },

        ConnOpen
            { connection_id: ConnectionId, reason: String }
            |e| {
                format!("failed to build conn open message {0}: {1}", e.connection_id, e.reason)
            },

        ConnOpenInit
            { reason: String }
            |e| { format!("failed to build conn open init: {0}", e.reason) },

        ConnOpenTry
            { reason: String }
            |e| { format!("failed to build conn open try: {0}", e.reason) },

        ChanOpenAck
            { channel_id: ChannelId, reason: String }
            |e| {
                format!("failed to build channel open ack {0}: {1}", e.channel_id, e.reason)
            },

        ChanOpenConfirm
            { channel_id: ChannelId, reason: String }
            |e| {
                format!("failed to build channel open confirm {0}: {1}", e.channel_id, e.reason)
            },

        ConsensusProof
            [ ProofError ]
            |_| { "failed to build consensus proof" },

        Packet
            { channel_id: ChannelId, reason: String }
            |e| {
                format!("failed to build packet {0}: {1}", e.channel_id, e.reason)
            },

        RecvPacket
            { channel_id: ChannelId, reason: String }
            |e| {
                format!("failed to build recv packet {0}: {1}", e.channel_id, e.reason)
            },

        AckPacket
            { channel_id: ChannelId, reason: String }
            |e| {
                format!("failed to build acknowledge packet {0}: {1}", e.channel_id, e.reason)
            },

        TimeoutPacket
            { channel_id: ChannelId, reason: String }
            |e| {
                format!("failed to build timeout packet {0}: {1}", e.channel_id, e.reason)
            },

        MessageTransaction
            { reason: String }
            |e| { format!("message transaction failure: {0}", e.reason) },

        Query
            { query: String }
            |e| { format!("query error occurred (failed to query for {0})", e.query) },

        KeyBase
            [ KeyringError ]
            |_| { "keyring error" },

        KeyNotFound
            { key_name: String }
            [ KeyringError ]
            |e| { format!("signature key not found: {}", e.key_name) },

        Ics02
            [ client_error::Error ]
            |e| { format!("ICS 02 error: {}", e.source) },

        Ics03
            [ connection_error::Error ]
            |_| { "ICS 03 error" },

        Ics07
            [ tendermint_error::Error ]
            |_| { "ICS 07 error" },

        Ics23
            [ commitment_error::Error ]
            |_| { "ICS 23 error" },

        Ics29
            [ FeeError ]
            | _ | { "ICS 29 error" },

        Ics31
            [ CrossChainQueryError ]
            | _ | {"ICS 31 error"},

        InvalidUri
            { uri: String }
            [ TraceError<InvalidUri> ]
            |e| { format!("error parsing URI {}", e.uri) },

        ChainIdentifier
            { chain_id: String }
            |e| { format!("invalid chain identifier format: {0}", e.chain_id) },

        NonProvableData
            |_| { "requested proof for data in the privateStore" },

        ChannelSend
            |_| { "internal message-passing failure while sending inter-thread request/response" },

        ChannelReceive
            [ TraceError<crossbeam_channel::RecvError> ]
            |_| { "internal message-passing failure while receiving inter-thread request/response" },

        ChannelReceiveTimeout
            [ TraceError<crossbeam_channel::RecvTimeoutError> ]
            |_| { "timeout when waiting for response over inter-thread channel" },

        InvalidInputHeader
            |_| { "the input header is not recognized as a header for this chain" },

        TxNoConfirmation
            |_| { "failed tx: no confirmation" },

        Misbehaviour
            { reason: String }
            |e| { format!("error raised while submitting the misbehaviour evidence: {0}", e.reason) },

        InvalidKeyAddress
            { address: String }
            [ TendermintError ]
            |e| { format!("invalid key address: {0}", e.address) },

        Bech32Encoding
            [ TraceError<bech32::Error> ]
            |_| { "bech32 encoding failed" },

        ClientTypeMismatch
            {
                expected: ClientType,
                got: ClientType,
            }
            |e| {
                format!("client type mismatch: expected '{}', got '{}'",
                e.expected, e.got)
            },

        ProtobufDecode
            { payload_type: String }
            [ TraceError<DecodeError> ]
            |e| { format!("error decoding protocol buffer for {}", e.payload_type) },

        ProtobufEncode
            { payload_type: String }
            [ TraceError<EncodeError> ]
            |e| { format!("error encoding protocol buffer for {}", e.payload_type) },

        TxSimulateGasEstimateExceeded
            {
                chain_id: ChainId,
                estimated_gas: u64,
                max_gas: u64,
            }
            |e| {
                format!("{} gas estimate {} from simulated Tx exceeds the maximum configured {}",
                    e.chain_id, e.estimated_gas, e.max_gas)
            },

        HealthCheckJsonRpc
            {
                chain_id: ChainId,
                address: String,
                endpoint: String,
            }
            [ DisplayOnly<tendermint_rpc::error::Error> ]
            |e| {
                format!("health check failed for endpoint {0} on the JSON-RPC interface of chain {1}:{2}",
                    e.endpoint, e.chain_id, e.address)
            },

        FetchVersionParsing
            {
                chain_id: ChainId,
                address: String,
            }
            [ version::Error ]
            |e| {
                format!("failed while parsing version info for chain {0}:{1}; caused by: {2}",
                    e.chain_id, e.address, e.source)
            },

        FetchVersionGrpcTransport
            {
                chain_id: ChainId,
                address: String,
                endpoint: String,
            }
            [ DisplayOnly<tonic::transport::Error> ]
            |e| {
                format!("failed while fetching version info from endpoint {0} on the gRPC interface of chain {1}:{2}",
                    e.endpoint, e.chain_id, e.address)
            },

        FetchVersionGrpcStatus
            {
                chain_id: ChainId,
                address: String,
                endpoint: String,
                status: tonic::Status
            }
            |e| {
                format!("failed while fetching version info from endpoint {0} on the gRPC interface of chain {1}:{2}; caused by: {3}",
                    e.endpoint, e.chain_id, e.address, e.status)
            },

        FetchVersionInvalidVersionResponse
            {
                chain_id: ChainId,
                address: String,
                endpoint: String,
            }
            |e| {
                format!("failed while fetching version info from endpoint {0} on the gRPC interface of chain {1}:{2}; the gRPC response contains no application version information",
                    e.endpoint, e.chain_id, e.address)
            },

        ConfigValidationJsonRpc
            {
                chain_id: ChainId,
                address: String,
                endpoint: String,
            }
            [ DisplayOnly<tendermint_rpc::error::Error> ]
            |e| {
                format!("semantic config validation: failed to reach endpoint {0} on the JSON-RPC interface of chain {1}:{2}",
                    e.endpoint, e.chain_id, e.address)
            },

        ConfigValidationTxSizeOutOfBounds
            {
                chain_id: ChainId,
                configured_bound: usize,
                genesis_bound: u64,
            }
            |e| {
                format!("semantic config validation failed for option `max_tx_size` for chain '{}', reason: `max_tx_size` = {} is greater than {}% of the consensus parameter `max_size` = {}",
                    e.chain_id, e.configured_bound, BLOCK_MAX_BYTES_MAX_FRACTION * 100.0, e.genesis_bound)
            },

        ConfigValidationMaxGasTooHigh
            {
                chain_id: ChainId,
                configured_max_gas: u64,
                consensus_max_gas: i64,
            }
            |e| {
                format!("semantic config validation failed for option `max_gas` for chain '{}', reason: `max_gas` = {} is greater than the consensus parameter `max_gas` = {}",
                    e.chain_id, e.configured_max_gas, e.consensus_max_gas)
            },

        ConfigValidationTrustingPeriodSmallerThanZero
            {
                chain_id: ChainId,
                trusting_period: Duration,
            }
            |e| {
                format!("semantic config validation failed for option `trusting_period` of chain '{}', reason: trusting period ({}) must be greater than zero",
                    e.chain_id, format_duration(e.trusting_period))
            },

        ConfigValidationTrustingPeriodGreaterThanUnbondingPeriod
            {
                chain_id: ChainId,
                trusting_period: Duration,
                unbonding_period: Duration,
            }
            |e| {
                format!("semantic config validation failed for option `trusting_period` of chain '{}', reason: trusting period ({}) must be smaller than the unbonding period ({})",
                    e.chain_id, format_duration(e.trusting_period), format_duration(e.unbonding_period))
            },

        ConfigValidationDefaultGasTooHigh
            {
                chain_id: ChainId,
                default_gas: u64,
                max_gas: u64,
            }
            |e| {
                format!("semantic config validation failed for option `default_gas` of chain '{}', reason: default gas ({}) must be smaller than the max gas ({})",
                    e.chain_id, e.default_gas, e.max_gas)
            },

        ConfigValidationGasMultiplierLow
            {
                chain_id: ChainId,
                gas_multiplier: f64,
            }
            |e| {
                format!("semantic config validation failed for option `gas_multiplier` of chain '{}', reason: gas multiplier ({}) is smaller than `1.1`, which could trigger gas fee errors in production", e.chain_id, e.gas_multiplier)
            },

        CompatCheckFailed
            {
                chain_id: ChainId,
                address: String,
                cause: String
            }
            |e| {
                format!("compatibility check failed for chain '{0}' at '{1}': {2}",
                    e.chain_id, e.address, e.cause)
            },

        UnknownAccountType
            {
                type_url: String
            }
            |e| {
                format!("failed to deserialize account of an unknown protobuf type: {0}", e.type_url)
            },

        EmptyBaseAccount
            |_| { "empty BaseAccount within EthAccount" },

        EmptyQueryAccount
            { address: String }
            |e| { format!("Query/Account RPC returned an empty account for address: {}", e.address) },

        NoHistoricalEntries
            { chain_id: ChainId }
            |e| {
                format_args!(
                    "chain '{}' does not maintain any historical entries \
                    (`historical_entries` params is set to 0)",
                    e.chain_id
                )
            },

        InvalidHistoricalEntries
            {
                chain_id: ChainId,
                entries: i64,
            }
            |e| {
                format_args!(
                    "chain '{}' reports invalid historical entries value \
                    (`historical_entries` params is set to '{}')",
                    e.chain_id,
                    e.entries,
                )
            },
        GasPriceTooLow
            { chain_id: ChainId }
            |e| { format!("Hermes gas price is lower than the minimum gas price set by node operator'{}'", e.chain_id) },

        TxIndexingDisabled
            { chain_id: ChainId }
            |e| {
                format_args!(
                    "transaction indexing for chain '{}' is disabled (`node_info.other.tx_index` is off)",
                    e.chain_id
                )
            },

        EmptyDenomTrace
            { hash: String }
            |e| {
                format_args!(
                    "Query/DenomTrace RPC returned an empty denom trace for trace hash: {}", e.hash)
            },

        MessageTooBigForTx
            { len: usize }
            |e| {
                format_args!("message with length {} is too large for a transaction", e.len)
            },

        InvalidKeyType
            { key_type: KeyType }
            |e| {
                format!("Invalid key type {} for the current chain", e.key_type)
            },

        QueriedProofNotFound
            |_| { "Requested proof with query but no proof was returned." },

        InvalidArchiveAddress
            { address: String }
            [ TendermintRpcError ]
            |e| { format!("invalid archive node address {}", e.address) },

        InvalidCompatMode
            [ TendermintRpcError ]
            |_| { "Invalid CompatMode queried from chain and no `compat_mode` configured in Hermes. This can be fixed by specifying a `compat_mode` in Hermes config.toml" },

        Other {
            source: Box<dyn std::error::Error + Send + Sync>,
        }
            |e| { format!("other error: {}", e.source) },
        HttpRequest
            [ TraceError<reqwest::Error> ]
            |_| { "HTTP request error" },

        HttpResponse
            { status: reqwest::StatusCode }
            |e| { format!("HTTP response error with status code {}", e.status) },

        HttpResponseBody
            [ TraceError<reqwest::Error> ]
            |_| { "HTTP response body error" },

        JsonDeserialize
            [ TraceError<serde_json::Error> ]
            |_| { "JSON deserialization error" },

        JsonField
            { field: String }
            |e| { format!("Missing or invalid JSON field: {}", e.field) },

        ParseFloat
            [ TraceError<std::num::ParseFloatError> ]
            |_| { "Error parsing float" },

        ParseInt
            [ TraceError<std::num::ParseIntError> ]
            |_| { "Error parsing integer" },

        Base64Decode
            [ TraceError<subtle_encoding::Error> ]
            |_| { "Error decoding base64-encoded data" },

        // TODO: replace with finer error variants later
        TempPenumbraError
            { detail: String }
            |e| { format!("penumbra error: {}", e.detail) },
    }
}

impl Error {
    pub fn send<T>(_: crossbeam_channel::SendError<T>) -> Error {
        Error::channel_send()
    }

    pub fn is_trusted_state_outside_trusting_period_error(&self) -> bool {
        match self.detail() {
            ErrorDetail::LightClientVerification(e) => matches!(
                e.source,
                LightClientErrorDetail::TrustedStateOutsideTrustingPeriod(_)
            ),
            _ => false,
        }
    }
}

impl GrpcStatusSubdetail {
    /// Check whether this gRPC error matches
    /// - message: verification failed: ... failed packet acknowledgement verification for client: client state height < proof height ...
    pub fn is_client_state_height_too_low(&self) -> bool {
        // Gaia v6.0.1 (SDK 0.44.5) returns code`InvalidArgument`, whereas gaia v6.0.4
        // (SDK 0.44.6, and potentially others) returns code `Unknown`.
        // Workaround by matching strictly on the status message.
        // if self.status.code() != tonic::Code::InvalidArgument
        //     return false;
        // }

        let msg = self.status.message();
        msg.contains("verification failed") && msg.contains("client state height < proof height")
    }

    /// Check whether this gRPC error message contains the string "account sequence mismatch".
    ///
    /// ## Note
    /// This predicate is tested and validated against errors
    /// that appear at the `estimate_gas` step. The error
    /// predicate to be used at the `broadcast_tx_sync` step
    /// is different & relies on parsing the Response error code.
    ///
    /// It is currently expected that, in the case of a match, the error message is of form:
    /// "account sequence mismatch, expected E, got G: incorrect account sequence",
    /// where E > G.
    /// The case where E < G is considered recoverable and should have been previously handled
    /// (see `is_account_sequence_mismatch_that_can_be_ignored` for which the error is ignored and
    /// simulation uses default gas).
    /// However, if in future cosmos-sdk releases the gRPC error message changes such that
    /// it still starts with "account sequence mismatch" but the rest doesn't match the remainder of
    /// the pattern (", expected E, got G: incorrect account sequence"), or
    /// there are hermes code changes such that the E < G case is not previously caught anymore,
    /// then this predicate will catch all "account sequence mismatch" errors
    pub fn is_account_sequence_mismatch_that_requires_refresh(&self) -> bool {
        self.status.message().contains("account sequence mismatch")
    }

    /// Check whether this gRPC error message contains the string "packet sequence out of order".
    ///
    /// ## Note
    /// This error may happen even when packets are submitted in order when the `simulate_tx`
    /// gRPC endpoint is allowed to be called after a block is created and before
    /// Tendermint/mempool finishes `recheck_tx`, similarly to the issue described in
    /// <https://github.com/informalsystems/hermes/issues/2249>.
    ///
    /// See <https://github.com/informalsystems/hermes/issues/2670> for more info.
    pub fn is_out_of_order_packet_sequence_error(&self) -> bool {
        self.status
            .message()
            .contains("packet sequence is out of order")
    }

    /// Check whether this gRPC error matches:
    /// "account sequence mismatch, expected E, got G",
    /// where E < G.
    /// It is currently expected that, in the case of a match, the error message is of form:
    /// "account sequence mismatch, expected E, got G: incorrect account sequence"
    ///
    /// # Note:
    /// This predicate is tested and validated against errors
    /// that appear during the `estimate_gas` step.
    /// If it evaluates to true then the error is ignored and the transaction that caused this
    /// simulation error is still sent to mempool with `broadcast_tx_sync` allowing for potential
    /// recovery after mempool's `recheckTxs` step.
    /// More details in <https://github.com/informalsystems/hermes/issues/2249>
    pub fn is_account_sequence_mismatch_that_can_be_ignored(&self) -> bool {
        match parse_sequences_in_mismatch_error_message(self.status.message()) {
            None => false,
            Some((expected, got)) => expected < got,
        }
    }

    /// Check whether this gRPC error message contains the string "invalid empty tx".
    ///
    /// ## Note
    /// This error may happen for older chains that does not properly support simulation.
    pub fn is_empty_tx_error(&self) -> bool {
        self.status.message().contains("invalid empty tx")
    }
}

/// Assumes that the cosmos-sdk account sequence mismatch error message, that may be seen
/// during simulating or broadcasting a transaction, includes the following pattern:
/// "account sequence mismatch, expected E, got G".
/// If a match is found it extracts and returns (E, G).
fn parse_sequences_in_mismatch_error_message(message: &str) -> Option<(u64, u64)> {
    let re = Regex::new(r"account sequence mismatch, expected (?P<expected>\d+), got (?P<got>\d+)")
        .unwrap();

    match re.captures(message) {
        None => None,
        Some(captures) => match (captures["expected"].parse(), captures["got"].parse()) {
            (Ok(e), Ok(g)) => Some((e, g)),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `variant_name` must not vary with the error's payload.
    ///
    /// This is the property that keeps telemetry label cardinality bounded:
    /// `Display` embeds gas-used values, sequence numbers and chain responses,
    /// so using it as a metric label mints a fresh time-series per retry.
    #[test]
    fn variant_name_is_invariant_under_payload() {
        let mut names = std::collections::HashSet::new();

        for i in 0..1000 {
            // Same variant, payload that differs every iteration — exactly the
            // shape of a simulate failure retrying against a broken client,
            // where the response embeds `with gas used: 'NNNNNN'`.
            let e = Error::grpc_response_param(format!(
                "insufficient fees; got: 0 required: {i}uatom: with gas used: '{}'",
                100_000 + i
            ));

            assert_ne!(
                e.detail().to_string(),
                Error::grpc_response_param(String::new()).detail().to_string(),
                "Display must embed the payload, else this test proves nothing",
            );

            names.insert(e.detail().variant_name());
        }

        assert_eq!(
            names.len(),
            1,
            "1000 distinct payloads collapsed to {} labels, expected 1: {names:?}",
            names.len(),
        );
    }

    /// Distinct variants must stay distinguishable — a fix that mapped
    /// everything to a single constant would pass the test above.
    #[test]
    fn variant_name_distinguishes_variants() {
        let param = Error::grpc_response_param("x".to_string());
        let other = Error::other_with_string("x".to_string());

        assert_eq!(param.detail().variant_name(), "GrpcResponseParam");
        assert_eq!(other.detail().variant_name(), "OtherWithString");
        assert_ne!(param.detail().variant_name(), other.detail().variant_name());
    }

    #[test]
    fn test_parse_sequences_in_mismatch_error_message() {
        struct Test<'a> {
            name: &'a str,
            message: &'a str,
            result: Option<(u64, u64)>,
        }
        let tests: Vec<Test<'_>> = vec![
            Test {
                name: "good mismatch error, expected < got",
                message:
                    "account sequence mismatch, expected 100, got 200: incorrect account sequence",
                result: Some((100, 200)),
            },
            Test {
                name: "good mismatch error, expected > got",
                message:
                    "account sequence mismatch, expected 200, got 100: incorrect account sequence",
                result: Some((200, 100)),
            },
            Test {
                name: "good changed mismatch error, expected < got",
                message: "account sequence mismatch, expected 100, got 200: this part has changed",
                result: Some((100, 200)),
            },
            Test {
                name: "good changed mismatch error, expected > got",
                message:
                    "account sequence mismatch, expected 200, got 100 --> this part has changed",
                result: Some((200, 100)),
            },
            Test {
                name: "good changed mismatch error, expected > got",
                message:
                    "codespace sdk code 32: incorrect account sequence: account sequence mismatch, expected 200, got 100",
                result: Some((200, 100)),
            },
            Test {
                name: "bad mismatch error, bad expected",
                message:
                    "account sequence mismatch, expected 2a5, got 100: incorrect account sequence",
                result: None,
            },
            Test {
                name: "bad mismatch error, bad got",
                message:
                    "account sequence mismatch, expected 25, got -29: incorrect account sequence",
                result: None,
            },
            Test {
                name: "not a mismatch error",
                message: "some other error message",
                result: None,
            },
        ];

        for test in tests {
            assert_eq!(
                test.result,
                parse_sequences_in_mismatch_error_message(test.message),
                "{}",
                test.name
            )
        }
    }
}

impl ErrorDetail {
    /// Stable, bounded identifier for this error variant.
    ///
    /// Telemetry uses error descriptions as metric labels, and the
    /// OpenTelemetry SDK retains one time-series per distinct label tuple for
    /// the lifetime of the meter. `Display` output embeds variable data (gas
    /// used, sequence numbers, hashes, chain responses), so using it as a
    /// label makes cardinality unbounded — every retry mints a new
    /// time-series. This returns one of a fixed set of 108 static
    /// strings instead.
    pub fn variant_name(&self) -> &'static str {
        use ErrorDetail::*;

        match self {
            OtherWithString(_) => "OtherWithString",
            Io(_) => "Io",
            Rpc(_) => "Rpc",
            AbciQuery(_) => "AbciQuery",
            Config(_) => "Config",
            CheckTx(_) => "CheckTx",
            DeliverTx(_) => "DeliverTx",
            SendTx(_) => "SendTx",
            WebSocket(_) => "WebSocket",
            EventSource(_) => "EventSource",
            Grpc(_) => "Grpc",
            GrpcStatus(_) => "GrpcStatus",
            GrpcTransport(_) => "GrpcTransport",
            GrpcResponseParam(_) => "GrpcResponseParam",
            Decode(_) => "Decode",
            LightClientBuilder(_) => "LightClientBuilder",
            LightClientVerification(_) => "LightClientVerification",
            LightClientState(_) => "LightClientState",
            LightClientIo(_) => "LightClientIo",
            ChainNotCaughtUp(_) => "ChainNotCaughtUp",
            PrivateStore(_) => "PrivateStore",
            Event(_) => "Event",
            ConversionFromAny(_) => "ConversionFromAny",
            EmptyUpgradedClientState(_) => "EmptyUpgradedClientState",
            ConsensusStateTypeMismatch(_) => "ConsensusStateTypeMismatch",
            EmptyResponseValue(_) => "EmptyResponseValue",
            EmptyResponseProof(_) => "EmptyResponseProof",
            RpcResponse(_) => "RpcResponse",
            MalformedProof(_) => "MalformedProof",
            InvalidHeight(_) => "InvalidHeight",
            InvalidHeightNoSource(_) => "InvalidHeightNoSource",
            InvalidMetadata(_) => "InvalidMetadata",
            BuildClientStateFailure(_) => "BuildClientStateFailure",
            CreateClient(_) => "CreateClient",
            ClientStateType(_) => "ClientStateType",
            ConnectionNotFound(_) => "ConnectionNotFound",
            BadConnectionState(_) => "BadConnectionState",
            ConnOpen(_) => "ConnOpen",
            ConnOpenInit(_) => "ConnOpenInit",
            ConnOpenTry(_) => "ConnOpenTry",
            ChanOpenAck(_) => "ChanOpenAck",
            ChanOpenConfirm(_) => "ChanOpenConfirm",
            ConsensusProof(_) => "ConsensusProof",
            Packet(_) => "Packet",
            RecvPacket(_) => "RecvPacket",
            AckPacket(_) => "AckPacket",
            TimeoutPacket(_) => "TimeoutPacket",
            MessageTransaction(_) => "MessageTransaction",
            Query(_) => "Query",
            KeyBase(_) => "KeyBase",
            KeyNotFound(_) => "KeyNotFound",
            Ics02(_) => "Ics02",
            Ics03(_) => "Ics03",
            Ics07(_) => "Ics07",
            Ics23(_) => "Ics23",
            Ics29(_) => "Ics29",
            Ics31(_) => "Ics31",
            InvalidUri(_) => "InvalidUri",
            ChainIdentifier(_) => "ChainIdentifier",
            NonProvableData(_) => "NonProvableData",
            ChannelSend(_) => "ChannelSend",
            ChannelReceive(_) => "ChannelReceive",
            ChannelReceiveTimeout(_) => "ChannelReceiveTimeout",
            InvalidInputHeader(_) => "InvalidInputHeader",
            TxNoConfirmation(_) => "TxNoConfirmation",
            Misbehaviour(_) => "Misbehaviour",
            InvalidKeyAddress(_) => "InvalidKeyAddress",
            Bech32Encoding(_) => "Bech32Encoding",
            ClientTypeMismatch(_) => "ClientTypeMismatch",
            ProtobufDecode(_) => "ProtobufDecode",
            ProtobufEncode(_) => "ProtobufEncode",
            TxSimulateGasEstimateExceeded(_) => "TxSimulateGasEstimateExceeded",
            HealthCheckJsonRpc(_) => "HealthCheckJsonRpc",
            FetchVersionParsing(_) => "FetchVersionParsing",
            FetchVersionGrpcTransport(_) => "FetchVersionGrpcTransport",
            FetchVersionGrpcStatus(_) => "FetchVersionGrpcStatus",
            FetchVersionInvalidVersionResponse(_) => "FetchVersionInvalidVersionResponse",
            ConfigValidationJsonRpc(_) => "ConfigValidationJsonRpc",
            ConfigValidationTxSizeOutOfBounds(_) => "ConfigValidationTxSizeOutOfBounds",
            ConfigValidationMaxGasTooHigh(_) => "ConfigValidationMaxGasTooHigh",
            ConfigValidationTrustingPeriodSmallerThanZero(_) => "ConfigValidationTrustingPeriodSmallerThanZero",
            ConfigValidationTrustingPeriodGreaterThanUnbondingPeriod(_) => "ConfigValidationTrustingPeriodGreaterThanUnbondingPeriod",
            ConfigValidationDefaultGasTooHigh(_) => "ConfigValidationDefaultGasTooHigh",
            ConfigValidationGasMultiplierLow(_) => "ConfigValidationGasMultiplierLow",
            CompatCheckFailed(_) => "CompatCheckFailed",
            UnknownAccountType(_) => "UnknownAccountType",
            EmptyBaseAccount(_) => "EmptyBaseAccount",
            EmptyQueryAccount(_) => "EmptyQueryAccount",
            NoHistoricalEntries(_) => "NoHistoricalEntries",
            InvalidHistoricalEntries(_) => "InvalidHistoricalEntries",
            GasPriceTooLow(_) => "GasPriceTooLow",
            TxIndexingDisabled(_) => "TxIndexingDisabled",
            EmptyDenomTrace(_) => "EmptyDenomTrace",
            MessageTooBigForTx(_) => "MessageTooBigForTx",
            InvalidKeyType(_) => "InvalidKeyType",
            QueriedProofNotFound(_) => "QueriedProofNotFound",
            InvalidArchiveAddress(_) => "InvalidArchiveAddress",
            InvalidCompatMode(_) => "InvalidCompatMode",
            HttpRequest(_) => "HttpRequest",
            HttpResponse(_) => "HttpResponse",
            HttpResponseBody(_) => "HttpResponseBody",
            JsonDeserialize(_) => "JsonDeserialize",
            JsonField(_) => "JsonField",
            ParseFloat(_) => "ParseFloat",
            ParseInt(_) => "ParseInt",
            Base64Decode(_) => "Base64Decode",
            TempPenumbraError(_) => "TempPenumbraError",
            Other(_) => "Other",
        }
    }
}
