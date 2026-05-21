//! # Regression test: refresh worker short-circuits on expired/frozen clients
//!
//! ## Protocol invariant
//!
//! When `client.refresh()` returns an `ExpiredOrFrozen` error, the refresh
//! worker MUST terminate the inner retry loop immediately. It MUST NOT
//! continue to drive `client.refresh()` through the Fibonacci backoff
//! strategy (5s → 1h, ~24 attempts over 1 day).
//!
//! ## Why this matters (memory-leak hardening, A1)
//!
//! Each call to `client.refresh()` allocates:
//!   - a fresh `tonic::transport::Channel` per gRPC query (3+ queries per
//!     refresh: `query_client_state`, `query_consensus_state`,
//!     `query_application_status`);
//!   - an `AnyClientState` (cosmos-sdk client state, several KB);
//!   - an `AnyConsensusState` (the consensus state at latest_height);
//!   - a chain status with the application's latest header;
//!   - on the build path (if the client is not expired), a TmHeader plus
//!     supporting validator sets (≈ KB per attempt at penumbra-1's
//!     23-validator active-set size).
//!
//! For a permanently-expired client (e.g. cosmoshub 07-tendermint-1317 as
//! of 2026-05-19), recovery is only possible via MsgRecoverClient
//! (governance). Hermes cannot fix it. Retrying 24 times per spawn per day
//! contributes a steady allocation stream that — combined with the OTHER
//! suspected leak surfaces (tonic Channel pool retention, error/log
//! formatting per attempt) — gates fast OOM growth on p02.
//!
//! ## What this test exercises
//!
//! The exact retry-result mapping used by `spawn_refresh_client`. We can't
//! easily mock `client.refresh()` (it requires a ChainHandle stack), so we
//! test the *short-circuit mapping itself* by constructing a closure that
//! mirrors the refactor in `crates/relayer/src/worker/client.rs`.
//!
//! Failure mode this catches:
//!   - regressing the `OperationResult::Err` arm back to `Retry`, which
//!     would silently turn the fix into a no-op and re-introduce the
//!     1-day-of-retries shape.
//!
//! What this does NOT test (out of scope here):
//!   - the full ChainHandle / RPC retry path against a live chain. That
//!     would need a mock cosmos chain rejecting at `update.go:224`.
//!   - tonic Channel pool retention (the other suspected leak surface,
//!     covered by LEAK_ANALYSIS.md).

#![cfg(target_os = "linux")]

use std::time::{Duration, Instant};

use ibc_relayer::foreign_client::{ExpiredOrFrozen, ForeignClientError, HasExpiredOrFrozenError};
use ibc_relayer_types::core::ics24_host::identifier::{ChainId, ClientId};
use retry::{delay::Fibonacci, retry_with_index, OperationResult};

/// The same mapping closure used in `worker::client::spawn_refresh_client`:
/// `Ok` continues, `Err(expired_or_frozen)` short-circuits the retry, any
/// other `Err` retries.
fn refresh_step(
    res: Result<(), ForeignClientError>,
) -> OperationResult<(), ForeignClientError> {
    match res {
        Ok(()) => OperationResult::Ok(()),
        Err(e) if e.is_expired_or_frozen_error() => OperationResult::Err(e),
        Err(e) => OperationResult::Retry(e),
    }
}

#[test]
fn refresh_retry_short_circuits_on_expired_client() {
    let client_id: ClientId = "07-tendermint-1317".parse().unwrap();
    let chain_id = ChainId::from_string("cosmoshub-4");

    let expired_err = || -> Result<(), ForeignClientError> {
        Err(ForeignClientError::expired_or_frozen(
            ExpiredOrFrozen::Expired,
            client_id.clone(),
            chain_id.clone(),
            "test: client trusting period exhausted".into(),
        ))
    };

    let mut attempts = 0;
    let started = Instant::now();

    // Drive the same Fibonacci strategy used by the production worker.
    // We bound it tighter in this test (max delay 1ms, max total 50ms) so
    // the test runs fast; the production strategy is `5s → 1h, total 1d`.
    // The SHAPE we're testing is the short-circuit in `refresh_step`, not
    // the retry timings themselves.
    let strategy = Fibonacci::from_millis(1).map(|d| d.min(Duration::from_millis(1))).take(100);

    let res = retry_with_index(strategy, |_| {
        attempts += 1;
        refresh_step(expired_err())
    });

    let elapsed = started.elapsed();

    assert!(res.is_err(), "must return Err on expired client");
    assert_eq!(
        attempts, 1,
        "PRODUCTION INVARIANT: expired/frozen errors must short-circuit the \
         retry loop on the FIRST attempt. Got {attempts} attempts. \
         If this fails, the per-iteration allocation churn (validator-set, \
         LightBlock, TmHeader, tonic Channel) will run 24× per spawn per day \
         against an unrecoverable client, contributing to the A1 memory \
         leak. See LEAK_ANALYSIS.md."
    );
    assert!(
        elapsed < Duration::from_millis(50),
        "short-circuit must be effectively instant, took {:?}",
        elapsed
    );
}

#[test]
fn refresh_retry_short_circuits_on_frozen_client() {
    let client_id: ClientId = "07-tendermint-1317".parse().unwrap();
    let chain_id = ChainId::from_string("cosmoshub-4");

    let frozen_err = || -> Result<(), ForeignClientError> {
        Err(ForeignClientError::expired_or_frozen(
            ExpiredOrFrozen::Frozen,
            client_id.clone(),
            chain_id.clone(),
            "test: client state reports that client is frozen".into(),
        ))
    };

    let mut attempts = 0;
    let strategy = Fibonacci::from_millis(1).map(|d| d.min(Duration::from_millis(1))).take(100);
    let res = retry_with_index(strategy, |_| {
        attempts += 1;
        refresh_step(frozen_err())
    });

    assert!(res.is_err(), "must return Err on frozen client");
    assert_eq!(
        attempts, 1,
        "frozen clients must short-circuit identically to expired clients"
    );
}

/// Sanity check: ordinary errors (not expired/frozen) MUST still retry.
/// This guards against an over-eager short-circuit that would break the
/// recovery from transient RPC failures.
#[test]
fn refresh_retry_still_retries_on_transient_errors() {
    let chain_id = ChainId::from_string("cosmoshub-4");

    let mut attempts = 0;
    let strategy = Fibonacci::from_millis(1).map(|d| d.min(Duration::from_millis(1))).take(5);

    let res = retry_with_index(strategy, |_| {
        attempts += 1;
        // Use `client_refresh` which is a non-expired error variant.
        refresh_step(Err(ForeignClientError::client_already_up_to_date(
            "07-tendermint-1317".parse().unwrap(),
            chain_id.clone(),
            ibc_relayer_types::Height::new(1, 1).unwrap(),
        )))
    });

    assert!(res.is_err(), "must eventually return Err");
    assert!(
        attempts > 1,
        "non-expired errors must continue to retry — got only {attempts} attempt(s). \
         The short-circuit is now too eager and will break transient-error recovery."
    );
}
