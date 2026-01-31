use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Maximum number of events to store in memory
const MAX_EVENTS: usize = 1000;

/// A single relay event capturing packet relay details
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelayEvent {
    /// Unix timestamp when the packet was created on source chain
    pub initiated_at: u64,
    /// Unix timestamp when the packet was relayed
    pub relayed_at: u64,
    /// Source chain ID
    pub src_chain: String,
    /// Destination chain ID
    pub dst_chain: String,
    /// Source channel
    pub src_channel: String,
    /// Destination channel
    pub dst_channel: String,
    /// Packet sequence number
    pub sequence: u64,
    /// Packet type: "recv", "ack", "timeout"
    pub packet_type: String,
    /// Transaction hash on destination chain (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
}

impl RelayEvent {
    pub fn new(
        src_chain: String,
        dst_chain: String,
        src_channel: String,
        dst_channel: String,
        sequence: u64,
        packet_type: &str,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            initiated_at: now, // Will be updated if we have packet timeout info
            relayed_at: now,
            src_chain,
            dst_chain,
            src_channel,
            dst_channel,
            sequence,
            packet_type: packet_type.to_string(),
            tx_hash: None,
        }
    }

    pub fn with_initiated_at(mut self, ts: u64) -> Self {
        self.initiated_at = ts;
        self
    }

    pub fn with_tx_hash(mut self, hash: String) -> Self {
        self.tx_hash = Some(hash);
        self
    }
}

/// Aggregated statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelayStats {
    /// Total events in last 24 hours
    pub packets_24h: usize,
    /// Events by chain in last 24h
    pub by_chain_24h: Vec<(String, usize)>,
    /// Total events stored
    pub total_stored: usize,
    /// Uptime in seconds (since store creation)
    pub uptime_secs: u64,
}

/// Thread-safe store for relay events
#[derive(Clone)]
pub struct RelayEventStore {
    inner: Arc<RwLock<RelayEventStoreInner>>,
}

struct RelayEventStoreInner {
    events: VecDeque<RelayEvent>,
    created_at: u64,
}

impl Default for RelayEventStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RelayEventStore {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            inner: Arc::new(RwLock::new(RelayEventStoreInner {
                events: VecDeque::with_capacity(MAX_EVENTS),
                created_at: now,
            })),
        }
    }

    /// Record a new relay event
    pub fn record(&self, event: RelayEvent) {
        if let Ok(mut inner) = self.inner.write() {
            // Remove oldest if at capacity
            if inner.events.len() >= MAX_EVENTS {
                inner.events.pop_front();
            }
            inner.events.push_back(event);
        }
    }

    /// Get recent events, optionally filtered by chain
    pub fn get_history(&self, limit: usize, chain_filter: Option<&str>) -> Vec<RelayEvent> {
        if let Ok(inner) = self.inner.read() {
            let iter = inner.events.iter().rev();

            let filtered: Vec<_> = if let Some(chain) = chain_filter {
                iter.filter(|e| e.src_chain == chain || e.dst_chain == chain)
                    .take(limit)
                    .cloned()
                    .collect()
            } else {
                iter.take(limit).cloned().collect()
            };

            filtered
        } else {
            Vec::new()
        }
    }

    /// Get aggregated statistics
    pub fn get_stats(&self) -> RelayStats {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let day_ago = now.saturating_sub(86400);

        if let Ok(inner) = self.inner.read() {
            // Count events in last 24h
            let events_24h: Vec<_> = inner
                .events
                .iter()
                .filter(|e| e.relayed_at >= day_ago)
                .collect();

            // Group by destination chain
            let mut by_chain: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for e in &events_24h {
                *by_chain.entry(e.dst_chain.clone()).or_insert(0) += 1;
            }

            let mut by_chain_vec: Vec<_> = by_chain.into_iter().collect();
            by_chain_vec.sort_by(|a, b| b.1.cmp(&a.1));

            RelayStats {
                packets_24h: events_24h.len(),
                by_chain_24h: by_chain_vec,
                total_stored: inner.events.len(),
                uptime_secs: now.saturating_sub(inner.created_at),
            }
        } else {
            RelayStats {
                packets_24h: 0,
                by_chain_24h: Vec::new(),
                total_stored: 0,
                uptime_secs: 0,
            }
        }
    }
}

// Global event store - initialized once
use once_cell::sync::Lazy;
pub static RELAY_EVENT_STORE: Lazy<RelayEventStore> = Lazy::new(RelayEventStore::new);

/// Convenience function to record an event
pub fn record_relay_event(event: RelayEvent) {
    RELAY_EVENT_STORE.record(event);
}

/// Convenience function to get history
pub fn get_relay_history(limit: usize, chain_filter: Option<&str>) -> Vec<RelayEvent> {
    RELAY_EVENT_STORE.get_history(limit, chain_filter)
}

/// Convenience function to get stats
pub fn get_relay_stats() -> RelayStats {
    RELAY_EVENT_STORE.get_stats()
}
