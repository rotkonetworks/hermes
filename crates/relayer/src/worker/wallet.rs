use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{error, error_span, trace, warn};

use crate::{
    chain::handle::ChainHandle,
    telemetry,
    util::task::{spawn_background_task, Next, TaskError, TaskHandle},
};

/// After this many consecutive `chain.config()` failures, escalate the log
/// level from WARN to ERROR so that monitoring can reliably detect a wedged
/// chain handle. The handle's inter-thread channel can stay broken
/// indefinitely without crashing hermes, but every retry will fail with the
/// same "timeout when waiting for response over inter-thread channel" error;
/// in that state, relaying through this chain is silently broken even though
/// the process appears alive.
const WEDGED_THRESHOLD: usize = 3;

pub fn spawn_wallet_worker<Chain: ChainHandle>(chain: Chain) -> TaskHandle {
    let span = error_span!("wallet", chain = %chain.id());
    let chain_id_str = chain.id().to_string();
    let consecutive_failures = Arc::new(AtomicUsize::new(0));

    spawn_background_task(span, Some(Duration::from_secs(5)), move || {
        // Use Ignore instead of Fatal for config/key errors to survive connection issues.
        // The wallet worker should keep retrying rather than dying on transient failures.
        // We track consecutive failures so monitoring can detect a wedged handle.
        let chain_config = match chain.config() {
            Ok(c) => {
                consecutive_failures.store(0, Ordering::Relaxed);
                c
            }
            Err(e) => {
                let n = consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
                let msg = format!("failed to get chain config: {e}");
                if n >= WEDGED_THRESHOLD {
                    error!(
                        chain = %chain_id_str, consecutive = n,
                        "wallet worker wedged: {msg} (relaying through this chain is broken until hermes is restarted)"
                    );
                }
                return Err(TaskError::Ignore(msg.into()));
            }
        };

        let account = if let Some(addr) = chain_config.relayer_account() {
            addr
        } else if chain_config.keyring_support() {
            chain.get_key().map_err(|e| {
                TaskError::Ignore(Box::new(format!(
                    "failed to get key in use by the relayer: {e}"
                )))
            })?.account()
        } else {
            chain.id().to_string()
        };

        let balance = chain.query_balance(None, None).map_err(|e| {
            TaskError::Ignore(Box::new(format!(
                "failed to query balance for the account: {e}"
            )))
        })?;

        match balance.amount.parse::<f64>() {
            Ok(amount) => {
                telemetry!(
                    wallet_balance,
                    &chain.id(),
                    &account,
                    amount,
                    &balance.denom,
                );
                trace!(%amount, denom = %balance.denom, %account, "wallet balance");
                telemetry!(
                    update_period_fees,
                    &chain.id(),
                    &account,
                    &balance.denom
                );
            }
            Err(e) => {
                warn!(
                    %balance.amount, denom = %balance.denom, %account,
                    "unable to parse the wallet balance into a f64, the balance will therefore not be reported to telemetry. Reason: {}", e
                );
            }
        }
        Ok(Next::Continue)
    })
}

#[cfg(test)]
mod tests {
    use ibc_relayer_types::bigint::U256;

    // Test to confirm that any u256 fits in f64
    #[test]
    fn compare_f64_max_to_u256_max() {
        let f64_max = f64::MAX;
        let u256_max = U256::MAX;

        assert!(f64_max > u256_max.to_string().parse::<f64>().unwrap());
    }
}
