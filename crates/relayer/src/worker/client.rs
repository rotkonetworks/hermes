use core::{convert::Infallible, time::Duration};

use crossbeam_channel::Receiver;
use ibc_relayer_types::{core::ics02_client::events::UpdateClient, events::IbcEvent};
use retry::{delay::Fibonacci, retry_with_index, OperationResult};
use tracing::{debug, debug_span, error_span, trace, warn};

use super::WorkerCmd;
use crate::{
    chain::handle::ChainHandle,
    foreign_client::{ForeignClient, HasExpiredOrFrozenError, MisbehaviourResults},
    util::{
        retry::clamp_total,
        task::{spawn_background_task, Next, TaskError, TaskHandle},
    },
};

const REFRESH_CHECK_INTERVAL: Duration = Duration::from_secs(5); // 5 seconds
const INITIAL_BACKOFF: Duration = Duration::from_secs(5); // 5 seconds
const MAX_REFRESH_DELAY: Duration = Duration::from_secs(60 * 60); // 1 hour
const MAX_REFRESH_TOTAL_DELAY: Duration = Duration::from_secs(60 * 60 * 24); // 1 day

pub fn spawn_refresh_client<ChainA: ChainHandle, ChainB: ChainHandle>(
    mut client: ForeignClient<ChainA, ChainB>,
) -> Option<TaskHandle> {
    if client.is_expired_or_frozen() {
        warn!(
            client = %client.id,
            "skipping refresh client task on frozen client",
        );

        return None;
    }

    Some(spawn_background_task(
        error_span!(
            "worker.client.refresh",
            client = %client.id,
            src_chain = %client.src_chain.id(),
            dst_chain = %client.dst_chain.id(),
        ),
        Some(REFRESH_CHECK_INTERVAL),
        move || {
            // Try to refresh the client, but only if the refresh window has expired.
            // If the refresh fails, retry according to the given strategy.
            // Short-circuit on an expired or frozen client instead of
            // retrying: `refresh()` -> `validated_client_state` issues three
            // dst-chain queries per attempt, and a client that is expired or
            // frozen cannot be recovered by retrying — only by governance
            // (`MsgRecoverClient`). Retrying burns ~24 attempts per spawn,
            // each opening fresh gRPC connections and formatting a fresh
            // error, for no possible benefit. Terminating the worker stops
            // that churn; the supervisor re-spawns it on the next scan once
            // the client is recovered.
            let res = retry_with_index(refresh_strategy(), |_| match client.refresh() {
                Ok(events) => OperationResult::Ok(events),
                Err(e) if e.is_expired_or_frozen_error() => OperationResult::Err(e),
                Err(e) => OperationResult::Retry(e),
            });

            match res {
                // If `client.refresh()` was successful, continue
                Ok(_) => Ok(Next::Continue),

                // If `client.refresh()` failed and the retry mechanism
                // exceeded the maximum delay, or we short-circuited on an
                // expired/frozen client, return a fatal error.
                Err(e) => Err(TaskError::Fatal(e)),
            }
        },
    ))
}

pub fn detect_misbehavior_task<ChainA: ChainHandle, ChainB: ChainHandle>(
    receiver: Receiver<WorkerCmd>,
    client: ForeignClient<ChainB, ChainA>,
) -> Option<TaskHandle> {
    if client.is_expired_or_frozen() {
        warn!(
            client = %client.id(),
            src_chain = %client.src_chain.id(),
            dst_chain = %client.dst_chain.id(),
            "skipping detect misbehavior task on frozen client",
        );

        return None;
    }

    let mut initial_check_done = false;

    let handle = spawn_background_task(
        error_span!(
            "worker.client.misbehaviour",
            client = %client.id,
            src_chain = %client.src_chain.id(),
            dst_chain = %client.dst_chain.id(),
        ),
        Some(Duration::from_millis(600)),
        move || -> Result<Next, TaskError<Infallible>> {
            if !initial_check_done {
                initial_check_done = true;

                debug!("doing initial misbehavior check");
                let result = client.detect_misbehaviour_and_submit_evidence(None);
                debug!("misbehavior detection result: {:?}", result);
            }

            if let Ok(WorkerCmd::IbcEvents { batch }) = receiver.try_recv() {
                trace!("received batch: {:?}", batch);

                for event_with_height in batch.events {
                    if let IbcEvent::UpdateClient(update) = event_with_height.event {
                        match on_client_update(&client, update) {
                            Next::Continue => continue,
                            Next::Abort => return Ok(Next::Abort),
                        }
                    }
                }
            }

            Ok(Next::Continue)
        },
    );

    Some(handle)
}

fn on_client_update<ChainA: ChainHandle, ChainB: ChainHandle>(
    client: &ForeignClient<ChainB, ChainA>,
    update: UpdateClient,
) -> Next {
    let _span = debug_span!(
        "on_client_update",
        client = %update.client_id(),
        client_type = %update.client_type(),
        height = %update.consensus_height(),
    );

    debug!("checking misbehavior for updated client");

    let result = client.detect_misbehaviour_and_submit_evidence(Some(update));

    trace!("misbehavior detection result: {:?}", result);

    match result {
        MisbehaviourResults::ValidClient => {
            debug!("client is valid");

            Next::Continue
        }
        MisbehaviourResults::VerificationError => {
            // can retry in next call
            debug!("client verification error, will retry in next call");

            Next::Continue
        }
        MisbehaviourResults::EvidenceSubmitted(_) => {
            // if evidence was submitted successfully then exit
            debug!("misbehavior detected! Evidence successfully submitted, exiting");

            Next::Abort
        }
        MisbehaviourResults::CannotExecute => {
            // skip misbehaviour checking if chain does not have support for it (i.e. client
            // update event does not include the header)
            debug!("cannot execute misbehavior detection, exiting");

            Next::Abort
        }
    }
}

fn refresh_strategy() -> impl Iterator<Item = Duration> {
    clamp_total(
        Fibonacci::from(INITIAL_BACKOFF),
        MAX_REFRESH_DELAY,
        MAX_REFRESH_TOTAL_DELAY,
    )
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use ibc_relayer_types::core::ics24_host::identifier::{ChainId, ClientId};
    use retry::{delay::Fixed, retry_with_index, OperationResult};

    use super::refresh_strategy;
    use crate::foreign_client::{
        ExpiredOrFrozen, ForeignClientError, HasExpiredOrFrozenError,
    };

    fn expired() -> ForeignClientError {
        ForeignClientError::expired_or_frozen(
            ExpiredOrFrozen::Expired,
            ClientId::default(),
            ChainId::new("test".to_string(), 0),
            "time elapsed since last client update: 1209600s".to_string(),
        )
    }

    fn frozen() -> ForeignClientError {
        ForeignClientError::expired_or_frozen(
            ExpiredOrFrozen::Frozen,
            ClientId::default(),
            ChainId::new("test".to_string(), 0),
            "client state reports that client is frozen".to_string(),
        )
    }

    fn transient() -> ForeignClientError {
        ForeignClientError::chain_error_event(
            ChainId::new("test".to_string(), 0),
            ibc_relayer_types::events::IbcEvent::ChainError("boom".to_string()),
        )
    }

    /// Mirrors the mapping in `spawn_refresh_client`. Kept in sync by eye —
    /// the point is to catch the `OperationResult::Err` arm regressing back
    /// to `Retry`, which would silently turn the fix into a no-op.
    fn drive<S, F>(strategy: S, mut refresh: F) -> usize
    where
        S: IntoIterator<Item = Duration>,
        F: FnMut() -> Result<(), ForeignClientError>,
    {
        let mut attempts = 0;

        let _ = retry_with_index(strategy, |_| {
            attempts += 1;

            match refresh() {
                Ok(()) => OperationResult::Ok(()),
                Err(e) if e.is_expired_or_frozen_error() => OperationResult::Err(e),
                Err(e) => OperationResult::Retry(e),
            }
        });

        attempts
    }

    #[test]
    fn short_circuits_on_expired_client() {
        // The real strategy: 5s initial backoff, clamped to 1h / 1 day. If the
        // short-circuit regressed, this test would hang rather than fail —
        // which is itself the signal.
        assert_eq!(drive(refresh_strategy(), || Err(expired())), 1);
    }

    #[test]
    fn short_circuits_on_frozen_client() {
        assert_eq!(drive(refresh_strategy(), || Err(frozen())), 1);
    }

    /// Guards against an over-eager short-circuit: ordinary failures must
    /// still retry, or recovery from a transient RPC blip would break.
    #[test]
    fn still_retries_on_transient_errors() {
        let strategy = Fixed::from_millis(1).take(4);
        assert_eq!(drive(strategy, || Err(transient())), 5);
    }

    /// An expired error surfacing anywhere in the refresh path must be
    /// recognised by the trait, not just one constructed in this test.
    /// `validated_client_state` returns it unwrapped (foreign_client.rs:761,784)
    /// and `try_refresh`/`refresh` propagate it with `?`.
    #[test]
    fn expired_error_is_recognised_by_the_trait() {
        assert!(expired().is_expired_or_frozen_error());
        assert!(frozen().is_expired_or_frozen_error());
        assert!(!transient().is_expired_or_frozen_error());
    }
}
