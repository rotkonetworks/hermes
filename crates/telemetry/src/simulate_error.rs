//! Classifier for `simulate_errors` telemetry labels.
//!
//! Without classification, `simulate_errors` would emit one Prometheus
//! time-series per unique error description string. Several `ErrorDetail`
//! variants reachable from `send_tx_simulate` use a `Display` impl that
//! embeds variable data (gas-used values, account sequences, packet bytes),
//! so the description set grows unboundedly under sustained simulate
//! failures — most notably during expired-client retry storms.
//!
//! `classify_simulate_error` maps any incoming description to one of a
//! fixed set of `&'static str` labels. See
//! `LEAK_ANALYSIS_ADDENDUM_simulate_errors.md` for the diagnosis.

pub fn classify_simulate_error(text: &str) -> &'static str {
    if text.contains("status Expired") || text.contains("client is expired") {
        "client expired"
    } else if text.contains("status Frozen") || text.contains("client is frozen") {
        "client frozen"
    } else if text.contains("account sequence mismatch") {
        "account sequence mismatch"
    } else if text.contains("insufficient funds") || text.contains("insufficient fee") {
        "insufficient funds"
    } else if text.contains("out of gas") {
        "out of gas"
    } else if text.contains("tx already in mempool") {
        "tx already in mempool"
    } else if text.contains("mempool is full") {
        "mempool full"
    } else if text.contains("invalid packet") {
        "invalid packet"
    } else if text.contains("packet timeout") {
        "packet timeout"
    } else if text.contains("packet already received") {
        "packet already received"
    } else if text.contains("acknowledgement for packet already exists") {
        "ack already exists"
    } else if text.contains("packet commitment not found") {
        "packet commitment not found"
    } else if text.contains("connection not found") {
        "connection not found"
    } else if text.contains("channel not found") {
        "channel not found"
    } else if text.contains("light client verification failed") {
        "light client verification failed"
    } else {
        "other"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn classify_is_bounded_across_varying_gas_used() {
        // Mirrors the production leak: the cosmoshub-4 expired-client path
        // produces descriptions that differ only by an embedded `gas used`
        // value, which would otherwise leak one new time-series per retry.
        let mut seen = HashSet::new();
        for gas in 0..10_000 {
            let desc = format!(
                "failed to execute message; message index: 0: client (07-tendermint-1317) \
                 status Expired: client is not active with gas used: '{}'",
                gas
            );
            seen.insert(classify_simulate_error(&desc));
        }
        assert_eq!(seen.len(), 1);
        assert!(seen.contains("client expired"));
    }

    #[test]
    fn classify_known_variants() {
        assert_eq!(
            classify_simulate_error("client (07-tendermint-1317) status Frozen"),
            "client frozen"
        );
        assert_eq!(
            classify_simulate_error("account sequence mismatch, expected 5, got 4"),
            "account sequence mismatch"
        );
        assert_eq!(
            classify_simulate_error("out of gas in location: ReadFlat"),
            "out of gas"
        );
        assert_eq!(
            classify_simulate_error("tx already in mempool"),
            "tx already in mempool"
        );
        assert_eq!(
            classify_simulate_error("packet already received"),
            "packet already received"
        );
        assert_eq!(
            classify_simulate_error("connection not found: connection-42"),
            "connection not found"
        );
    }

    #[test]
    fn classify_unknown_falls_back_to_other() {
        assert_eq!(
            classify_simulate_error("some entirely novel error text"),
            "other"
        );
    }

    #[test]
    fn classify_returns_static_lifetime() {
        // Static-lifetime return guarantees zero allocation per emit and
        // is what bounds Prometheus label cardinality at runtime.
        let s: &'static str = classify_simulate_error("anything");
        assert!(!s.is_empty());
    }
}
