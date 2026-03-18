//! Tower-based service layer for Penumbra IBC gRPC queries.
//!
//! This module defines a unified request/response algebra (`IbcQuery` / `IbcQueryResponse`)
//! and a leaf `tower::Service` implementation (`PenumbraGrpcQueryService`) that dispatches
//! each variant to the appropriate gRPC client method.
//!
//! The service is internal to the Penumbra chain implementation — it does not alter
//! `ChainEndpoint` or any shared relayer traits.

pub mod layers;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ibc_proto::ibc::core::{
    channel::v1::query_client::QueryClient as IbcChannelQueryClient,
    client::v1::query_client::QueryClient as IbcClientQueryClient,
    connection::v1::query_client::QueryClient as IbcConnectionQueryClient,
};
use tonic::IntoRequest;

use crate::chain::requests::{
    IncludeProof, QueryChannelClientStateRequest, QueryChannelRequest, QueryChannelsRequest,
    QueryClientStateRequest, QueryClientStatesRequest, QueryConnectionChannelsRequest,
    QueryConnectionRequest, QueryConnectionsRequest, QueryConsensusStateHeightsRequest,
    QueryConsensusStateRequest, QueryNextSequenceReceiveRequest,
    QueryPacketAcknowledgementRequest, QueryPacketAcknowledgementsRequest,
    QueryPacketCommitmentRequest, QueryPacketCommitmentsRequest, QueryPacketReceiptRequest,
    QueryUnreceivedAcksRequest, QueryUnreceivedPacketsRequest,
};
use crate::chain::requests::QueryHeight;
use crate::error::Error;

use ibc_proto::ibc::core::channel::v1::{
    QueryChannelClientStateResponse, QueryChannelResponse, QueryChannelsResponse,
    QueryConnectionChannelsResponse, QueryNextSequenceReceiveResponse,
    QueryPacketAcknowledgementResponse, QueryPacketAcknowledgementsResponse,
    QueryPacketCommitmentResponse, QueryPacketCommitmentsResponse, QueryPacketReceiptResponse,
    QueryUnreceivedAcksResponse, QueryUnreceivedPacketsResponse,
};
use ibc_proto::ibc::core::client::v1::{
    QueryClientStateResponse, QueryClientStatesResponse, QueryConsensusStateHeightsResponse,
    QueryConsensusStateResponse,
};
use ibc_proto::ibc::core::connection::v1::{
    QueryConnectionResponse, QueryConnectionsResponse,
};

use ibc_proto::ibc::core::channel::v1::QueryChannelRequest as RawQueryChannelRequest;
use ibc_proto::ibc::core::channel::v1::QueryNextSequenceReceiveRequest as RawQueryNextSequenceReceiveRequest;
use ibc_proto::ibc::core::channel::v1::QueryPacketAcknowledgementRequest as RawQueryPacketAcknowledgementRequest;
use ibc_proto::ibc::core::channel::v1::QueryPacketCommitmentRequest as RawQueryPacketCommitmentRequest;
use ibc_proto::ibc::core::channel::v1::QueryPacketReceiptRequest as RawQueryPacketReceiptRequest;
use ibc_proto::ibc::core::client::v1::QueryClientStateRequest as RawQueryClientStateRequest;
use ibc_proto::ibc::core::client::v1::QueryConsensusStateRequest as RawQueryConsensusStateRequest;
use ibc_proto::ibc::core::connection::v1::QueryConnectionRequest as RawQueryConnectionRequest;

use super::error::PenumbraError;

/// Unified IBC gRPC query request type.
///
/// Each variant wraps the strongly-typed hermes request struct and, where the
/// ChainEndpoint API allows it, an `IncludeProof` flag indicating whether the
/// response must carry a Merkle proof.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum IbcQuery {
    /// List all client states.
    ClientStates(QueryClientStatesRequest),
    /// Query a single client state, optionally with proof.
    ClientState(QueryClientStateRequest, IncludeProof),
    /// Query a consensus state, optionally with proof.
    ConsensusState(QueryConsensusStateRequest, IncludeProof),
    /// Query consensus state heights for a client.
    ConsensusStateHeights(QueryConsensusStateHeightsRequest),
    /// List all connections.
    Connections(QueryConnectionsRequest),
    /// Query a single connection, optionally with proof.
    Connection(QueryConnectionRequest, IncludeProof),
    /// List channels for a given connection.
    ConnectionChannels(QueryConnectionChannelsRequest),
    /// List all channels.
    Channels(QueryChannelsRequest),
    /// Query a single channel, optionally with proof.
    Channel(QueryChannelRequest, IncludeProof),
    /// Query the client state associated with a channel.
    ChannelClientState(QueryChannelClientStateRequest),
    /// Query a packet commitment, optionally with proof.
    PacketCommitment(QueryPacketCommitmentRequest, IncludeProof),
    /// List packet commitments.
    PacketCommitments(QueryPacketCommitmentsRequest),
    /// Query a packet receipt, optionally with proof.
    PacketReceipt(QueryPacketReceiptRequest, IncludeProof),
    /// List unreceived packets.
    UnreceivedPackets(QueryUnreceivedPacketsRequest),
    /// Query a packet acknowledgement, optionally with proof.
    PacketAcknowledgement(QueryPacketAcknowledgementRequest, IncludeProof),
    /// List packet acknowledgements.
    PacketAcknowledgements(QueryPacketAcknowledgementsRequest),
    /// List unreceived acknowledgements.
    UnreceivedAcks(QueryUnreceivedAcksRequest),
    /// Query next sequence receive, optionally with proof.
    NextSequenceReceive(QueryNextSequenceReceiveRequest, IncludeProof),
}

impl IbcQuery {
    /// Returns the query height embedded in this request, if any.
    ///
    /// Variants that carry a `QueryHeight` field expose it here so that
    /// cross-cutting layers (e.g. `HeightMetadataLayer`) can extract and
    /// inject it into gRPC metadata without variant-specific logic.
    pub fn query_height(&self) -> Option<&QueryHeight> {
        match self {
            Self::ClientState(req, _) => Some(&req.height),
            Self::ConsensusState(req, _) => Some(&req.query_height),
            Self::Connection(req, _) => Some(&req.height),
            Self::Channel(req, _) => Some(&req.height),
            Self::PacketCommitment(req, _) => Some(&req.height),
            Self::PacketReceipt(req, _) => Some(&req.height),
            Self::PacketAcknowledgement(req, _) => Some(&req.height),
            Self::NextSequenceReceive(req, _) => Some(&req.height),
            // Variants without a height field.
            Self::ClientStates(_)
            | Self::ConsensusStateHeights(_)
            | Self::Connections(_)
            | Self::ConnectionChannels(_)
            | Self::Channels(_)
            | Self::ChannelClientState(_)
            | Self::PacketCommitments(_)
            | Self::UnreceivedPackets(_)
            | Self::PacketAcknowledgements(_)
            | Self::UnreceivedAcks(_) => None,
        }
    }

    /// A static label for tracing / telemetry purposes.
    pub fn name(&self) -> &'static str {
        match self {
            Self::ClientStates(_) => "query_clients",
            Self::ClientState(..) => "query_client_state",
            Self::ConsensusState(..) => "query_consensus_state",
            Self::ConsensusStateHeights(_) => "query_consensus_state_heights",
            Self::Connections(_) => "query_connections",
            Self::Connection(..) => "query_connection",
            Self::ConnectionChannels(_) => "query_connection_channels",
            Self::Channels(_) => "query_channels",
            Self::Channel(..) => "query_channel",
            Self::ChannelClientState(_) => "query_channel_client_state",
            Self::PacketCommitment(..) => "query_packet_commitment",
            Self::PacketCommitments(_) => "query_packet_commitments",
            Self::PacketReceipt(..) => "query_packet_receipt",
            Self::UnreceivedPackets(_) => "query_unreceived_packets",
            Self::PacketAcknowledgement(..) => "query_packet_acknowledgement",
            Self::PacketAcknowledgements(_) => "query_packet_acknowledgements",
            Self::UnreceivedAcks(_) => "query_unreceived_acks",
            Self::NextSequenceReceive(..) => "query_next_sequence_receive",
        }
    }
}

/// Unified IBC gRPC query response type.
///
/// Each variant wraps the raw protobuf response. Higher-level layers are
/// responsible for decoding proofs, deserializing domain types, etc.
#[non_exhaustive]
#[derive(Debug)]
pub enum IbcQueryResponse {
    ClientStates(QueryClientStatesResponse),
    ClientState(QueryClientStateResponse),
    ConsensusState(QueryConsensusStateResponse),
    ConsensusStateHeights(QueryConsensusStateHeightsResponse),
    Connections(QueryConnectionsResponse),
    Connection(QueryConnectionResponse),
    ConnectionChannels(QueryConnectionChannelsResponse),
    Channels(QueryChannelsResponse),
    Channel(QueryChannelResponse),
    ChannelClientState(QueryChannelClientStateResponse),
    PacketCommitment(QueryPacketCommitmentResponse),
    PacketCommitments(QueryPacketCommitmentsResponse),
    PacketReceipt(QueryPacketReceiptResponse),
    UnreceivedPackets(QueryUnreceivedPacketsResponse),
    PacketAcknowledgement(QueryPacketAcknowledgementResponse),
    PacketAcknowledgements(QueryPacketAcknowledgementsResponse),
    UnreceivedAcks(QueryUnreceivedAcksResponse),
    NextSequenceReceive(QueryNextSequenceReceiveResponse),
}

/// The innermost leaf service: dispatches `IbcQuery` variants to gRPC client
/// methods on the three IBC query clients (client, connection, channel).
///
/// This service is **raw** — it performs no retry, tracing, proof decoding,
/// or height-metadata injection. Those are composed as tower layers on top.
#[derive(Clone)]
pub struct PenumbraGrpcQueryService {
    ibc_client: IbcClientQueryClient<tonic::transport::Channel>,
    ibc_connection: IbcConnectionQueryClient<tonic::transport::Channel>,
    ibc_channel: IbcChannelQueryClient<tonic::transport::Channel>,
}

impl PenumbraGrpcQueryService {
    /// Construct from pre-connected gRPC clients.
    pub fn new(
        ibc_client: IbcClientQueryClient<tonic::transport::Channel>,
        ibc_connection: IbcConnectionQueryClient<tonic::transport::Channel>,
        ibc_channel: IbcChannelQueryClient<tonic::transport::Channel>,
    ) -> Self {
        Self {
            ibc_client,
            ibc_connection,
            ibc_channel,
        }
    }
}

impl tower::Service<IbcQuery> for PenumbraGrpcQueryService {
    type Response = IbcQueryResponse;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // gRPC clients are always ready (they manage their own connection pool).
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: IbcQuery) -> Self::Future {
        // Clone the clients for the spawned future — tonic clients are cheap
        // Arc-based handles.
        let mut client_svc = self.ibc_client.clone();
        let mut conn_svc = self.ibc_connection.clone();
        let mut chan_svc = self.ibc_channel.clone();

        Box::pin(async move {
            match req {
                IbcQuery::ClientStates(r) => {
                    let request = tonic::Request::new(r.into());
                    let resp = client_svc
                        .client_states(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_clients",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::ClientStates(resp.into_inner()))
                }

                IbcQuery::ClientState(r, _include_proof) => {
                    let proto: RawQueryClientStateRequest = r.into();
                    let request = proto.into_request();
                    let resp = client_svc
                        .client_state(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_client_state",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::ClientState(resp.into_inner()))
                }

                IbcQuery::ConsensusState(r, _include_proof) => {
                    let mut proto: RawQueryConsensusStateRequest = r.into();
                    // Workaround: connection handshake fails with latest_height=true.
                    proto.latest_height = false;
                    let request = proto.into_request();
                    let resp = client_svc
                        .consensus_state(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_consensus_state",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::ConsensusState(resp.into_inner()))
                }

                IbcQuery::ConsensusStateHeights(r) => {
                    let proto =
                        ibc_proto::ibc::core::client::v1::QueryConsensusStateHeightsRequest {
                            client_id: r.client_id.to_string(),
                            pagination: Default::default(),
                        };
                    let resp = client_svc
                        .consensus_state_heights(proto)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_consensus_state_heights",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::ConsensusStateHeights(resp.into_inner()))
                }

                IbcQuery::Connections(r) => {
                    let request = tonic::Request::new(r.into());
                    let resp = conn_svc
                        .connections(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_connections",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::Connections(resp.into_inner()))
                }

                IbcQuery::Connection(r, _include_proof) => {
                    let proto: RawQueryConnectionRequest = r.into();
                    let request = proto.into_request();
                    let resp = conn_svc
                        .connection(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_connection",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::Connection(resp.into_inner()))
                }

                IbcQuery::ConnectionChannels(r) => {
                    let request = tonic::Request::new(r.into());
                    let resp = chan_svc
                        .connection_channels(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_connection_channels",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::ConnectionChannels(resp.into_inner()))
                }

                IbcQuery::Channels(r) => {
                    let request = tonic::Request::new(r.into());
                    let resp = chan_svc
                        .channels(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_channels",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::Channels(resp.into_inner()))
                }

                IbcQuery::Channel(r, _include_proof) => {
                    let proto: RawQueryChannelRequest = r.into();
                    let request = proto.into_request();
                    let resp = chan_svc
                        .channel(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_channel",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::Channel(resp.into_inner()))
                }

                IbcQuery::ChannelClientState(r) => {
                    let request = tonic::Request::new(r.into());
                    let resp = chan_svc
                        .channel_client_state(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_channel_client_state",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::ChannelClientState(resp.into_inner()))
                }

                IbcQuery::PacketCommitment(r, _include_proof) => {
                    let proto: RawQueryPacketCommitmentRequest = r.into();
                    let request = proto.into_request();
                    let resp = chan_svc
                        .packet_commitment(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_packet_commitment",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::PacketCommitment(resp.into_inner()))
                }

                IbcQuery::PacketCommitments(r) => {
                    let request = tonic::Request::new(r.into());
                    let resp = chan_svc
                        .packet_commitments(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_packet_commitments",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::PacketCommitments(resp.into_inner()))
                }

                IbcQuery::PacketReceipt(r, _include_proof) => {
                    let proto: RawQueryPacketReceiptRequest = r.into();
                    let request = proto.into_request();
                    let resp = chan_svc
                        .packet_receipt(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_packet_receipt",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::PacketReceipt(resp.into_inner()))
                }

                IbcQuery::UnreceivedPackets(r) => {
                    let request = tonic::Request::new(r.into());
                    let resp = chan_svc
                        .unreceived_packets(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_unreceived_packets",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::UnreceivedPackets(resp.into_inner()))
                }

                IbcQuery::PacketAcknowledgement(r, _include_proof) => {
                    let proto: RawQueryPacketAcknowledgementRequest = r.into();
                    let request = proto.into_request();
                    let resp = chan_svc
                        .packet_acknowledgement(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_packet_acknowledgement",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::PacketAcknowledgement(resp.into_inner()))
                }

                IbcQuery::PacketAcknowledgements(r) => {
                    let request = tonic::Request::new(r.into());
                    let resp = chan_svc
                        .packet_acknowledgements(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_packet_acknowledgements",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::PacketAcknowledgements(resp.into_inner()))
                }

                IbcQuery::UnreceivedAcks(r) => {
                    let request = tonic::Request::new(r.into());
                    let resp = chan_svc
                        .unreceived_acks(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_unreceived_acks",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::UnreceivedAcks(resp.into_inner()))
                }

                IbcQuery::NextSequenceReceive(r, _include_proof) => {
                    let proto: RawQueryNextSequenceReceiveRequest = r.into();
                    let request = proto.into_request();
                    let resp = chan_svc
                        .next_sequence_receive(request)
                        .await
                        .map_err(|e| PenumbraError::GrpcStatus {
                            method: "query_next_sequence_receive",
                            source: e,
                        })?;
                    Ok(IbcQueryResponse::NextSequenceReceive(resp.into_inner()))
                }
            }
        })
    }
}
