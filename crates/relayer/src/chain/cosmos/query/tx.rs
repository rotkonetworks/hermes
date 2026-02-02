use ibc_relayer_types::core::ics02_client::height::Height;
use ibc_relayer_types::core::ics04_channel::packet::{Packet, Sequence};
use ibc_relayer_types::core::ics24_host::identifier::ChainId;
use ibc_relayer_types::events::IbcEvent;
use ibc_relayer_types::Height as ICSHeight;
use tendermint::abci::Event;
use tendermint::Hash as TxHash;
use tendermint_rpc::endpoint::tx::Response as TxResponse;
use tendermint_rpc::{Client, HttpClient, Order, Url};
use tracing::warn;

use crate::chain::cosmos::query::{header_query, packet_query, tx_hash_query};
use crate::chain::cosmos::types::events;
use crate::chain::requests::{
    QueryClientEventRequest, QueryHeight, QueryPacketEventDataRequest, QueryTxHash, QueryTxRequest,
};
use crate::error::Error;
use crate::event::{ibc_event_try_from_abci_event, IbcEventWithHeight};

/// This function queries transactions for events matching certain criteria.
/// 1. Client Update request - returns a vector with at most one update client event
/// 2. Transaction event request - returns all IBC events resulted from a Tx execution
pub async fn query_txs(
    chain_id: &ChainId,
    rpc_client: &HttpClient,
    rpc_address: &Url,
    request: QueryTxRequest,
) -> Result<Vec<IbcEventWithHeight>, Error> {
    crate::time!("query_txs",
    {
        "src_chain": chain_id,
    });
    crate::telemetry!(query, chain_id, "query_txs");

    match request {
        QueryTxRequest::Client(request) => {
            crate::time!(
                "query_txs: single client update event",
                {
                    "src_chain": chain_id,
                }
            );

            // query the first Tx that includes the event matching the client request
            // Note: it is possible to have multiple Tx-es for same client and consensus height.
            // In this case it must be true that the client updates were performed with the
            // same header as the first one, otherwise a subsequent transaction would have
            // failed on chain. Therefore only one Tx is of interest and current API returns
            // the first one.
            let mut response = rpc_client
                .tx_search(
                    header_query(&request),
                    false,
                    1,
                    1, // get only the first Tx matching the query
                    Order::Ascending,
                )
                .await
                .map_err(|e| Error::rpc(rpc_address.clone(), e))?;

            if response.txs.is_empty() {
                return Ok(vec![]);
            }

            // the response must include a single Tx as specified in the query.
            assert!(
                response.txs.len() <= 1,
                "packet_from_tx_search_response: unexpected number of txs"
            );

            let tx = response.txs.remove(0);
            let event = update_client_from_tx_search_response(chain_id, &request, tx)?;

            Ok(event.into_iter().collect())
        }

        QueryTxRequest::Transaction(tx) => {
            crate::time!(
                "query_txs: transaction hash",
                {
                    "src_chain": chain_id,
                }
            );
            let mut response = rpc_client
                .tx_search(
                    tx_hash_query(&tx),
                    false,
                    1,
                    1, // get only the first Tx matching the query
                    Order::Ascending,
                )
                .await
                .map_err(|e| Error::rpc(rpc_address.clone(), e))?;

            if response.txs.is_empty() {
                Ok(vec![])
            } else {
                let tx = response.txs.remove(0);
                Ok(all_ibc_events_from_tx_search_response(chain_id, tx))
            }
        }
    }
}

/// This function queries transactions for packet events matching certain criteria.
///
/// It returns at most one packet event for each sequence specified in the request.
///    Note - there is no way to format the packet query such that it asks for Tx-es with either
///    sequence (the query conditions can only be AND-ed).
///    There is a possibility to include "<=" and ">=" conditions but it doesn't work with
///    string attributes (sequence is emitted as a string).
///    Therefore, for packets we perform one tx_search for each sequence.
///    Alternatively, a single query for all packets could be performed but it would return all
///    packets ever sent.
pub async fn query_packets_from_txs(
    chain_id: &ChainId,
    rpc_client: &HttpClient,
    rpc_address: &Url,
    request: &QueryPacketEventDataRequest,
) -> Result<Vec<IbcEventWithHeight>, Error> {
    crate::time!(
        "query_packets_from_txs",
        {
            "src_chain": chain_id,
        }
    );
    crate::telemetry!(query, chain_id, "query_packets_from_txs");

    let mut result: Vec<IbcEventWithHeight> = vec![];

    for seq in &request.sequences {
        // Query the latest 10 txs which include the event specified in the query request
        let query = packet_query(request, *seq);
        tracing::debug!(
            %query,
            chain_id = %chain_id,
            sequence = %seq,
            "executing packet tx_search query"
        );
        let response = rpc_client
            .tx_search(query, false, 1, 10, Order::Descending)
            .await
            .map_err(|e| Error::rpc(rpc_address.clone(), e))?;

        tracing::debug!(
            chain_id = %chain_id,
            sequence = %seq,
            tx_count = response.txs.len(),
            total_count = response.total_count,
            "tx_search response received"
        );

        if response.txs.is_empty() {
            tracing::debug!(
                chain_id = %chain_id,
                sequence = %seq,
                "tx_search returned no transactions"
            );
            continue;
        }

        let mut tx_events = vec![];

        // Process each tx in descending order
        for tx in response.txs {
            tracing::debug!(
                chain_id = %chain_id,
                sequence = %seq,
                tx_hash = %tx.hash,
                event_count = tx.tx_result.events.len(),
                event_types = ?tx.tx_result.events.iter().map(|e| &e.kind).collect::<Vec<_>>(),
                "processing tx from tx_search"
            );
            // Check if the tx contains and event which matches the query
            if let Some(event) = packet_from_tx_search_response(chain_id, request, *seq, &tx)? {
                // We found the event
                tx_events.push((event, tx.hash, tx.height));
            }
        }

        // If no event was found for this sequence, continue to the next sequence
        if tx_events.is_empty() {
            continue;
        }

        // If more than one event was found for this sequence, log a warning
        if tx_events.len() > 1 {
            warn!("more than one packet event found for sequence {seq}, this should not happen",);

            for (event, hash, height) in &tx_events {
                warn!("seq: {seq}, tx hash: {hash}, tx height: {height}, event: {event}",);
            }
        }

        // In either case, use the first (latest) event found for this sequence
        let (first_event, _, _) = tx_events.remove(0);
        result.push(first_event);
    }

    Ok(result)
}

/// This function queries packet events from a block at a specific height.
/// It returns packet events that match certain criteria (see [`filter_matching_event`]).
/// It returns at most one packet event for each sequence specified in the request.
pub async fn query_packets_from_block(
    chain_id: &ChainId,
    rpc_client: &HttpClient,
    rpc_address: &Url,
    request: &QueryPacketEventDataRequest,
) -> Result<Vec<IbcEventWithHeight>, Error> {
    crate::time!(
        "query_packets_from_block",
        {
            "src_chain": chain_id,
        }
    );
    crate::telemetry!(query, chain_id, "query_packets_from_block");

    let tm_height = match request.height.get() {
        QueryHeight::Latest => tendermint::block::Height::default(),
        QueryHeight::Specific(h) => {
            tendermint::block::Height::try_from(h.revision_height()).unwrap()
        }
    };

    let height = Height::new(chain_id.version(), u64::from(tm_height))
        .map_err(|_| Error::invalid_height_no_source())?;

    let block_results = rpc_client
        .block_results(tm_height)
        .await
        .map_err(|e| Error::rpc(rpc_address.clone(), e))?;

    let mut events: Vec<_> = block_results
        .begin_block_events
        .unwrap_or_default()
        .iter()
        .filter_map(|ev| filter_matching_event(ev, request, &request.sequences))
        .map(|ev| IbcEventWithHeight::new(ev, height))
        .collect();

    if let Some(txs) = block_results.txs_results {
        for tx in txs {
            events.extend(
                tx.events
                    .iter()
                    .filter_map(|ev| filter_matching_event(ev, request, &request.sequences))
                    .map(|ev| IbcEventWithHeight::new(ev, height)),
            )
        }
    }

    events.extend(
        block_results
            .end_block_events
            .unwrap_or_default()
            .iter()
            .filter_map(|ev| filter_matching_event(ev, request, &request.sequences))
            .map(|ev| IbcEventWithHeight::new(ev, height)),
    );

    // Since CometBFT 0.38, block events are returned in the
    // finalize_block_events field and the other *_block_events fields
    // are no longer present. We put these in place of the end_block_events
    // in older protocol.
    events.extend(
        block_results
            .finalize_block_events
            .iter()
            .filter_map(|ev| filter_matching_event(ev, request, &request.sequences))
            .map(|ev| IbcEventWithHeight::new(ev, height)),
    );

    Ok(events)
}

// Extracts from the Tx the update client event for the requested client and height.
// Note: in the Tx, there may have been multiple events, some of them may be
// for update of other clients that are not relevant to the request.
// For example, if we're querying for a transaction that includes the update for client X at
// consensus height H, it is possible that the transaction also includes an update client
// for client Y at consensus height H'. This is the reason the code iterates all event fields in the
// returned Tx to retrieve the relevant ones.
// Returns `None` if no matching event was found.
fn update_client_from_tx_search_response(
    chain_id: &ChainId,
    request: &QueryClientEventRequest,
    response: TxResponse,
) -> Result<Option<IbcEventWithHeight>, Error> {
    let height = ICSHeight::new(chain_id.version(), u64::from(response.height))
        .map_err(|_| Error::invalid_height_no_source())?;

    if let QueryHeight::Specific(specific_query_height) = request.query_height {
        if height > specific_query_height {
            return Ok(None);
        }
    };

    Ok(response
        .tx_result
        .events
        .into_iter()
        .filter(|event| event.kind == request.event_id.as_str())
        .flat_map(|event| ibc_event_try_from_abci_event(&event).ok())
        .flat_map(|event| match event {
            IbcEvent::UpdateClient(update) => Some(update),
            _ => None,
        })
        .find(|update| {
            update.common.client_id == request.client_id
                && update.common.consensus_height == request.consensus_height
        })
        .map(|update| IbcEventWithHeight::new(IbcEvent::UpdateClient(update), height)))
}

// Extract the packet events from the query_txs RPC response. For any given
// packet query, there is at most one Tx matching such query. Moreover, a Tx may
// contain several events, but a single one must match the packet query.
// For example, if we're querying for the packet with sequence 3 and this packet
// was committed in some Tx along with the packet with sequence 4, the response
// will include both packets. For this reason, we iterate all packets in the Tx,
// searching for those that match (which must be a single one).
fn packet_from_tx_search_response(
    chain_id: &ChainId,
    request: &QueryPacketEventDataRequest,
    seq: Sequence,
    response: &TxResponse,
) -> Result<Option<IbcEventWithHeight>, Error> {
    let height = ICSHeight::new(chain_id.version(), u64::from(response.height))
        .map_err(|_| Error::invalid_height_no_source())?;

    eprintln!("DEBUG packet_from_tx: chain={} seq={} tx_height={} events_count={}",
        chain_id, seq, height, response.tx_result.events.len());

    if let QueryHeight::Specific(query_height) = request.height.get() {
        eprintln!("DEBUG packet_from_tx: comparing height {} vs query_height {}", height, query_height);
        if height > query_height {
            eprintln!("DEBUG packet_from_tx: SKIPPING - height > query_height");
            return Ok(None);
        }
    }

    eprintln!("DEBUG packet_from_tx: iterating {} events", response.tx_result.events.len());
    Ok(response
        .tx_result
        .events
        .iter()
        .find_map(|ev| filter_matching_event(ev, request, &[seq]))
        .map(|ibc_event| IbcEventWithHeight::new(ibc_event, height)))
}

/// Returns the given event wrapped in `Some` if the event data
/// is consistent with the request parameters.
/// Returns `None` otherwise.
pub fn filter_matching_event(
    event: &Event,
    request: &QueryPacketEventDataRequest,
    seqs: &[Sequence],
) -> Option<IbcEvent> {
    fn matches_packet(
        request: &QueryPacketEventDataRequest,
        seqs: Vec<Sequence>,
        packet: &Packet,
    ) -> bool {
        packet.source_port == request.source_port_id
            && packet.source_channel == request.source_channel_id
            && packet.destination_port == request.destination_port_id
            && packet.destination_channel == request.destination_channel_id
            && seqs.contains(&packet.sequence)
    }

    eprintln!("DEBUG filter_matching_event: event_kind={} expected={}", event.kind, request.event_id.as_str());
    tracing::debug!(
        event_kind = %event.kind,
        expected_event_id = %request.event_id.as_str(),
        "filter_matching_event called"
    );

    if event.kind != request.event_id.as_str() {
        return None;
    }

    let ibc_event = match ibc_event_try_from_abci_event(event) {
        Ok(ev) => {
            tracing::debug!(
                event_kind = %event.kind,
                ibc_event_type = ?std::mem::discriminant(&ev),
                "successfully parsed abci event"
            );
            ev
        }
        Err(e) => {
            tracing::debug!(
                event_kind = %event.kind,
                error = %e,
                "failed to parse abci event into ibc event"
            );
            return None;
        }
    };

    match ibc_event {
        IbcEvent::SendPacket(ref send_ev) => {
            if matches_packet(request, seqs.to_vec(), &send_ev.packet) {
                Some(ibc_event)
            } else {
                tracing::debug!(
                    packet_src_port = %send_ev.packet.source_port,
                    packet_src_channel = %send_ev.packet.source_channel,
                    packet_dst_port = %send_ev.packet.destination_port,
                    packet_dst_channel = %send_ev.packet.destination_channel,
                    packet_sequence = %send_ev.packet.sequence,
                    request_src_port = %request.source_port_id,
                    request_src_channel = %request.source_channel_id,
                    request_dst_port = %request.destination_port_id,
                    request_dst_channel = %request.destination_channel_id,
                    "SendPacket event did not match request"
                );
                None
            }
        }
        IbcEvent::WriteAcknowledgement(ref ack_ev) => {
            if matches_packet(request, seqs.to_vec(), &ack_ev.packet) {
                Some(ibc_event)
            } else {
                tracing::debug!(
                    packet_src_port = %ack_ev.packet.source_port,
                    packet_src_channel = %ack_ev.packet.source_channel,
                    "WriteAck event did not match request"
                );
                None
            }
        }
        other => {
            tracing::debug!(
                event_kind = %event.kind,
                ibc_event = ?other,
                "ibc event type not SendPacket or WriteAck, ignoring"
            );
            None
        }
    }
}

pub async fn query_tx_response(
    rpc_client: &HttpClient,
    rpc_address: &Url,
    tx_hash: &TxHash,
) -> Result<Option<TxResponse>, Error> {
    let response = rpc_client
        .tx_search(
            tx_hash_query(&QueryTxHash(*tx_hash)),
            false,
            1,
            1, // get only the first Tx matching the query
            Order::Ascending,
        )
        .await
        .map_err(|e| Error::rpc(rpc_address.clone(), e))?;

    Ok(response.txs.into_iter().next())
}

pub fn all_ibc_events_from_tx_search_response(
    chain_id: &ChainId,
    response: TxResponse,
) -> Vec<IbcEventWithHeight> {
    let height = ICSHeight::new(chain_id.version(), u64::from(response.height)).unwrap();
    let deliver_tx_result = response.tx_result;

    if deliver_tx_result.code.is_err() {
        // We can only return a single ChainError here because at this point
        // we have lost information about how many messages were in the transaction
        vec![IbcEventWithHeight::new(
            IbcEvent::ChainError(format!(
                "deliver_tx for {} reports error: code={:?}, log={:?}",
                response.hash, deliver_tx_result.code, deliver_tx_result.log
            )),
            height,
        )]
    } else {
        let result = deliver_tx_result
            .events
            .iter()
            .flat_map(|event| events::from_tx_response_event(height, event).into_iter())
            .collect::<Vec<_>>();

        result
    }
}
