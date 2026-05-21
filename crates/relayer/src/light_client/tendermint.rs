mod detector;

use std::time::Duration;

use itertools::Itertools;
use tendermint::validator::Set as ValidatorSet;
use tendermint::Time;
use tracing::{debug, error, trace, warn};

use tendermint_light_client::{
    components::{
        self,
        io::{AtHeight, Io, ProdIo},
    },
    light_client::LightClient as TmLightClient,
    state::State as LightClientState,
    store::{memory::MemoryStore, LightStore},
    verifier::types::{Height as TMHeight, LightBlock, PeerId, Status},
    verifier::ProdVerifier,
};
use tendermint_light_client_detector::Divergence;
use tendermint_rpc as rpc;

use ibc_relayer_types::clients::ics07_tendermint::header::Header as TmHeader;
use ibc_relayer_types::clients::ics07_tendermint::misbehaviour::Misbehaviour as TmMisbehaviour;
use ibc_relayer_types::core::ics02_client::events::UpdateClient;
use ibc_relayer_types::core::ics02_client::header::AnyHeader;
use ibc_relayer_types::core::ics24_host::identifier::ChainId;
use ibc_relayer_types::Height as ICSHeight;

use crate::{
    chain::cosmos::{config::CosmosSdkConfig, CosmosSdkChain},
    chain::penumbra::config::PenumbraConfig,
    client_state::AnyClientState,
    error::Error,
    misbehaviour::{AnyMisbehaviour, MisbehaviourEvidence},
    HERMES_VERSION,
};

use super::{
    io::{AnyIo, RestartAwareIo},
    Verified,
};

pub struct LightClient {
    chain_id: ChainId,
    peer_id: PeerId,
    io: AnyIo,
    enable_verification: bool,
}

impl super::LightClient<CosmosSdkChain> for LightClient {
    fn header_and_minimal_set(
        &mut self,
        trusted_height: ICSHeight,
        target_height: ICSHeight,
        client_state: &AnyClientState,
        now: Time,
    ) -> Result<Verified<TmHeader>, Error> {
        crate::time!(
            "light_client.tendermint.header_and_minimal_set",
            {
                "src_chain": self.chain_id.to_string(),
            }
        );
        let Verified { target, supporting } =
            self.verify(trusted_height, target_height, client_state, now)?;

        // If verify() returned an empty supporting set AND the client allows
        // update after expiry, this is an unverified fallback for an expired
        // client. Skip adjust_headers (which fetches trusted_height+1 that
        // may be pruned) and build the header directly.
        if supporting.is_empty() && client_state.allow_update_after_expiry() {
            // For expired client recovery, build the header directly without
            // running adjust_headers. See `expired_fallback_with_fetcher`
            // doc for the protocol invariant; the regression test
            // `trusted_vals_uses_trusted_height_plus_one_on_expired_fallback`
            // pins the height we fetch.
            //
            // If `trusted_height + 1` is pruned on the source RPC, this fetch
            // will fail and the caller should use --archive-address to point
            // at an archive node. Mismatched validators is never acceptable.
            let mut fetch_vals = |h: ICSHeight| -> Result<ValidatorSet, Error> {
                Ok(self.fetch(h)?.validators)
            };
            let header = expired_fallback_with_fetcher(
                target,
                trusted_height,
                target_height,
                &mut fetch_vals,
            )?;
            return Ok(Verified {
                target: header,
                supporting: vec![],
            });
        }

        // Omit the trusted header from the minimal supporting set, as it is not
        // needed when submitting the update client message.
        let supporting = {
            let trusted_height = TMHeight::from(trusted_height);

            supporting
                .into_iter()
                .filter(|lb| lb.height() != trusted_height)
                .collect()
        };

        let (target, supporting) = self.adjust_headers(trusted_height, target, supporting)?;

        Ok(Verified { target, supporting })
    }

    fn verify(
        &mut self,
        trusted_height: ICSHeight,
        target_height: ICSHeight,
        client_state: &AnyClientState,
        now: Time,
    ) -> Result<Verified<LightBlock>, Error> {
        crate::time!(
            "light_client.tendermint.verify",
            {
                "src_chain": self.chain_id.to_string(),
            }
        );
        trace!(%trusted_height, %target_height, "light client verification");

        if !self.enable_verification {
            let target = self.fetch(target_height)?;

            return Ok(Verified {
                target,
                supporting: vec![],
            });
        }

        let client = self.prepare_client(client_state, now)?;

        // Try the full verification path: prepare trusted state, then verify.
        // If any step fails and the client allows update after expiry, fall
        // back to fetching the target header without local verification.
        // This handles both pruned RPCs (prepare_state fails) and expired
        // trusting periods (verify_to_target fails).
        let verified = (|| -> Result<Verified<LightBlock>, Error> {
            let mut state = self.prepare_state(trusted_height)?;
            let target = client
                .verify_to_target(target_height.into(), &mut state)
                .map_err(|e| Error::light_client_verification(self.chain_id.to_string(), e))?;
            let target_trace = state.get_trace(target.height());
            let supporting = target_trace
                .into_iter()
                .unique_by(LightBlock::height)
                .sorted_by_key(LightBlock::height)
                .filter(|lb| lb.height() != target.height())
                .collect_vec();
            Ok(Verified { target, supporting })
        })();

        match verified {
            Ok(v) => Ok(v),
            Err(e) if client_state.allow_update_after_expiry() => {
                warn!(
                    %trusted_height,
                    %target_height,
                    error = %e,
                    "light client verification failed but client allows update after \
                     expiry — fetching header without local verification (chain will validate)",
                );
                let target = self.fetch(target_height)?;
                Ok(Verified {
                    target,
                    supporting: vec![],
                })
            }
            Err(e) => Err(e),
        }
    }

    fn fetch(&mut self, height: ICSHeight) -> Result<LightBlock, Error> {
        trace!(%height, "fetching header");

        self.fetch_light_block(AtHeight::At(height.into()))
    }

    /// Perform misbehavior detection on the given client state and update client event.
    fn detect_misbehaviour(
        &mut self,
        update: &UpdateClient,
        client_state: &AnyClientState,
        now: Time,
    ) -> Result<Option<MisbehaviourEvidence>, Error> {
        crate::time!(
            "light client check_misbehaviour",
            {
                "src_chain": self.chain_id,
            }
        );

        let any_header = update.header.as_ref().ok_or_else(|| {
            Error::misbehaviour(format!(
                "missing header in update client event {}",
                self.chain_id
            ))
        })?;

        let update_header = match any_header {
            AnyHeader::Tendermint(header) => Ok::<_, Error>(header),
        }?;

        let client_state = match client_state {
            AnyClientState::Tendermint(client_state) => Ok::<_, Error>(client_state),
        }?;

        let next_validators = self
            .io
            .fetch_validator_set(
                AtHeight::At(update_header.signed_header.header.height.increment()),
                Some(update_header.signed_header.header.proposer_address),
            )
            .map_err(|e| Error::light_client_io(self.chain_id.to_string(), e))?;

        let target_block: LightBlock = LightBlock {
            signed_header: update_header.signed_header.clone(),
            validators: update_header.validator_set.clone(),
            next_validators,
            provider: self.peer_id,
        };

        // Get the light block at trusted_height + 1 from chain.
        let trusted_block = self.fetch(update_header.trusted_height.increment())?;
        if trusted_block.validators.hash() != update_header.trusted_validator_set.hash() {
            return Err(Error::misbehaviour(format!(
                "mismatch between the trusted validator set of the update \
                header ({}) and that of the trusted block that was fetched ({}), \
                aborting misbehaviour detection.",
                trusted_block.validators.hash(),
                update_header.trusted_validator_set.hash()
            )));
        }

        let divergence = detector::detect(
            self.peer_id,
            self.io.rpc_client().clone(),
            target_block,
            trusted_block,
            client_state,
            now,
        );

        match divergence {
            Ok(None) => {
                debug!("no misbehavior detected");
                Ok(None)
            }
            Ok(Some(Divergence {
                evidence,
                challenging_block,
            })) => {
                warn!("misbehavior detected, reporting evidence to RPC witness node and primary chain");
                debug!("evidence: {evidence:#?}");
                debug!("challenging block: {challenging_block:#?}");

                warn!("waiting 5 seconds before reporting evidence to RPC witness node");
                std::thread::sleep(Duration::from_secs(5));

                match detector::report_evidence(
                    self.io.rpc_client().clone(),
                    evidence.against_primary,
                ) {
                    Ok(hash) => warn!("evidence reported to RPC witness node with hash: {hash}"),
                    Err(e) => error!("failed to report evidence to RPC witness node: {e}"),
                }

                let target_block = self.fetch(update_header.height())?;
                let trusted_height = TMHeight::from(update_header.trusted_height);
                let trace = evidence
                    .witness_trace
                    .into_vec()
                    .into_iter()
                    .filter(|lb| {
                        lb.height() != target_block.height() && lb.height() != trusted_height
                    })
                    .collect();

                let (target_header, supporting_headers) =
                    self.adjust_headers(update_header.trusted_height, target_block, trace)?;

                let evidence = MisbehaviourEvidence {
                    misbehaviour: AnyMisbehaviour::Tendermint(TmMisbehaviour {
                        client_id: update.client_id().clone(),
                        header1: update_header.clone(),
                        header2: TmHeader {
                            signed_header: challenging_block.signed_header,
                            validator_set: challenging_block.validators,
                            trusted_height: target_header.trusted_height,
                            trusted_validator_set: target_header.trusted_validator_set,
                        },
                    }),
                    supporting_headers: supporting_headers
                        .into_iter()
                        .map(AnyHeader::Tendermint)
                        .collect(),
                };

                Ok(Some(evidence))
            }
            Err(e) => {
                error!("could not detect misbehavior: {}", e);
                Err(e)
            }
        }
    }
}

fn io_for_addr(
    addr: &rpc::Url,
    peer_id: PeerId,
    timeout: Option<Duration>,
) -> Result<ProdIo, Error> {
    let rpc_client = rpc::HttpClient::builder(addr.clone().try_into().unwrap())
        .user_agent(format!("hermes/{}", HERMES_VERSION))
        .build()
        .map_err(|e| Error::rpc(addr.clone(), e))?;
    Ok(ProdIo::new(peer_id, rpc_client, timeout))
}

impl LightClient {
    pub fn from_penumbra_config(config: &PenumbraConfig, peer_id: PeerId) -> Result<Self, Error> {
        let live_io = io_for_addr(&config.rpc_addr, peer_id, Some(config.rpc_timeout))?;

        let io = match &config.genesis_restart {
            None => AnyIo::Prod(live_io),
            Some(genesis_restart) => {
                let archive_io = io_for_addr(
                    &genesis_restart.archive_addr,
                    peer_id,
                    Some(config.rpc_timeout),
                )?;

                AnyIo::RestartAware(RestartAwareIo::new(
                    genesis_restart.restart_height,
                    live_io,
                    archive_io,
                ))
            }
        };

        // If the full node is configured as trusted then, in addition to headers not being verified,
        // the verification traces will not be provided. This may cause failure in client
        // updates after significant change in validator sets.
        let enable_verification = false;

        Ok(Self {
            chain_id: config.id.clone(),
            peer_id,
            io,

            enable_verification,
        })
    }

    pub fn from_cosmos_sdk_config(
        config: &CosmosSdkConfig,
        peer_id: PeerId,
    ) -> Result<Self, Error> {
        let live_io = io_for_addr(&config.rpc_addr, peer_id, Some(config.rpc_timeout))?;

        let io = match &config.genesis_restart {
            None => AnyIo::Prod(live_io),
            Some(genesis_restart) => {
                let archive_io = io_for_addr(
                    &genesis_restart.archive_addr,
                    peer_id,
                    Some(config.rpc_timeout),
                )?;

                AnyIo::RestartAware(RestartAwareIo::new(
                    genesis_restart.restart_height,
                    live_io,
                    archive_io,
                ))
            }
        };

        // If the full node is configured as trusted then, in addition to headers not being verified,
        // the verification traces will not be provided. This may cause failure in client
        // updates after significant change in validator sets.
        let enable_verification = !config.trusted_node;

        Ok(Self {
            chain_id: config.id.clone(),
            peer_id,
            io,

            enable_verification,
        })
    }

    fn prepare_client(
        &self,
        client_state: &AnyClientState,
        now: Time,
    ) -> Result<TmLightClient, Error> {
        let clock = components::clock::FixedClock::new(now);
        let verifier = ProdVerifier::default();
        let scheduler = components::scheduler::basic_bisecting_schedule;

        let client_state = match client_state {
            AnyClientState::Tendermint(client_state) => Ok::<_, Error>(client_state),
        }?;

        Ok(TmLightClient::new(
            self.peer_id,
            client_state.as_light_client_options(),
            clock,
            scheduler,
            verifier,
            self.io.clone(),
        ))
    }

    fn prepare_state(&self, trusted_height: ICSHeight) -> Result<LightClientState, Error> {
        let trusted_block = self.fetch_light_block(AtHeight::At(trusted_height.into()))?;

        let mut store = MemoryStore::new();
        store.insert(trusted_block, Status::Trusted);

        Ok(LightClientState::new(store))
    }

    fn fetch_light_block(&self, height: AtHeight) -> Result<LightBlock, Error> {
        self.io
            .fetch_light_block(height)
            .map_err(|e| Error::light_client_io(self.chain_id.to_string(), e))
    }

    fn adjust_headers(
        &mut self,
        trusted_height: ICSHeight,
        target: LightBlock,
        supporting: Vec<LightBlock>,
    ) -> Result<(TmHeader, Vec<TmHeader>), Error> {
        use super::LightClient;

        trace!(
            trusted = %trusted_height, target = %target.height(),
            "adjusting headers with {} supporting headers", supporting.len()
        );

        // Use a fetcher closure that pulls the validator set for a given
        // height from the source chain. All validator-set selection logic is
        // factored into the pure helper below so tests can exercise it
        // hermetically (see the `tests` module at the bottom of this file).
        let mut fetch_vals = |h: ICSHeight| -> Result<ValidatorSet, Error> {
            Ok(self.fetch(h)?.validators)
        };
        adjust_headers_with_fetcher(trusted_height, target, supporting, &mut fetch_vals)
    }
}

/// Pure construction of the `MsgUpdateClient` header for the
/// **expired-client fallback** path, given an already-resolved
/// `trusted_validator_set`.
///
/// Most callers (and the regression test) should use
/// [`expired_fallback_with_fetcher`] instead, which pins the **height** the
/// validators are fetched at. The split-out form is kept because it also
/// expresses the protocol invariant statically: this function CANNOT
/// produce a wrong-height validator set — that responsibility is in the
/// fetcher form below.
#[doc(hidden)]
pub fn build_expired_fallback_header(
    target: LightBlock,
    trusted_height: ICSHeight,
    vals_at_trusted_height_plus_one: ValidatorSet,
) -> TmHeader {
    TmHeader {
        signed_header: target.signed_header,
        validator_set: target.validators,
        trusted_height,
        trusted_validator_set: vals_at_trusted_height_plus_one,
    }
}

/// Expired-client fallback header construction with an injectable fetcher.
///
/// PROTOCOL INVARIANT: `header.trusted_validator_set.hash()` must equal the
/// `next_validators_hash` that the chain stored at `trusted_height`. The
/// chain side checks this in 07-tendermint `update.go:224` (`does not hash
/// to latest trusted validators`). On Tendermint, `next_validators_hash@H`
/// is definitionally `hash(vals@H+1)`. So this function fetches validators
/// at `trusted_height + 1` — NEVER at `target_height + 1`.
///
/// `_target_height` is accepted (and ignored) only so that the call-site
/// signature documents the heights in play: if a future regression tries
/// to "simplify" by using the target height it must explicitly change
/// this signature, and the regression test below will fail.
///
/// Regression bug history (commit c0e16b07): the original expired-fallback
/// added in `b61e31e1` used `vals@target_height+1`, which silently breaks
/// across any validator rotation between trusted and target heights.
#[doc(hidden)]
pub fn expired_fallback_with_fetcher<F>(
    target: LightBlock,
    trusted_height: ICSHeight,
    _target_height: ICSHeight,
    fetch_vals: &mut F,
) -> Result<TmHeader, Error>
where
    F: FnMut(ICSHeight) -> Result<ValidatorSet, Error>,
{
    // INVARIANT: fetch validators at trusted_height + 1, never target+1.
    let vals = fetch_vals(trusted_height.increment())?;
    Ok(build_expired_fallback_header(target, trusted_height, vals))
}

/// Pure form of `LightClient::adjust_headers`.
///
/// `fetch_vals(h)` must return the validator set at height `h`.
///
/// PROTOCOL INVARIANT (for every `TmHeader` produced):
/// `header.trusted_validator_set == vals_at(header.trusted_height + 1)`.
/// The chain hashes this against the stored `next_validators_hash@trusted`,
/// so any deviation rejects the update on chain.
#[doc(hidden)]
pub fn adjust_headers_with_fetcher<F>(
    trusted_height: ICSHeight,
    target: LightBlock,
    supporting: Vec<LightBlock>,
    fetch_vals: &mut F,
) -> Result<(TmHeader, Vec<TmHeader>), Error>
where
    F: FnMut(ICSHeight) -> Result<ValidatorSet, Error>,
{
    // Get the validator set at trusted_height + 1 from chain.
    //
    // NOTE: This is needed to get the next validator set. While there is a next
    //       validator set in the light block at trusted height, the proposer is
    //       not known/set in this set.
    let trusted_validator_set = fetch_vals(trusted_height.increment())?;

    let mut supporting_headers = Vec::with_capacity(supporting.len());

    let mut current_trusted_height = trusted_height;
    let mut current_trusted_validators = trusted_validator_set.clone();

    for support in supporting {
        let header = TmHeader {
            signed_header: support.signed_header.clone(),
            validator_set: support.validators,
            trusted_height: current_trusted_height,
            trusted_validator_set: current_trusted_validators,
        };

        // This header is now considered to be the currently trusted header
        current_trusted_height = header.height();

        // Therefore we can now trust the next validator set, see NOTE above.
        current_trusted_validators = fetch_vals(header.height().increment())?;

        supporting_headers.push(header);
    }

    // a) Set the trusted height of the target header to the height of the previous
    // supporting header if any, or to the initial trusting height otherwise.
    //
    // b) Set the trusted validators of the target header to the validators of the
    // successor to the last supporting header if any, or to the initial trusted
    // validators otherwise.
    let (latest_trusted_height, latest_trusted_validator_set) = match supporting_headers.last() {
        Some(prev_header) => {
            let prev_succ = fetch_vals(prev_header.height().increment())?;
            (prev_header.height(), prev_succ)
        }
        None => (trusted_height, trusted_validator_set),
    };

    let target_header = TmHeader {
        signed_header: target.signed_header,
        validator_set: target.validators,
        trusted_height: latest_trusted_height,
        trusted_validator_set: latest_trusted_validator_set,
    };

    Ok((target_header, supporting_headers))
}

// =============================================================================
// Regression tests for the validator-set-selection bug classes.
//
// These tests are the SPECIFICATION of the protocol invariant the cosmos chain
// enforces in `07-tendermint/update.go:224`:
//
//     hash(header.trusted_validator_set) == stored_consensus_state.next_validators_hash
//
// Because `next_validators_hash` recorded at height H is definitionally
// `hash(vals_at(H+1))`, hermes MUST set `header.trusted_validator_set` to
// `vals_at(trusted_height + 1)`. Using any other height (notably
// `target_height + 1`) silently breaks across any validator rotation between
// trusted and target.
//
// Bug history:
//   - b61e31e1 introduced an expired-client recovery fallback that used
//     `target_height.increment()`.
//   - c0e16b07 corrected it to `trusted_height.increment()`. These tests are
//     the regression gate.
// =============================================================================
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ibc_relayer_types::Height as ICSHeight;
    use tendermint::validator::Set as ValidatorSet;
    use tendermint_light_client::verifier::types::LightBlock;
    use tendermint_testgen::{
        light_block::TmLightBlock, Commit, Generator, Header as TgHeader, LightBlock as TgLightBlock,
        Validator,
    };

    use super::{adjust_headers_with_fetcher, expired_fallback_with_fetcher};
    use crate::error::Error;

    /// Build a vector of N validators with deterministic ed25519 keys derived
    /// from a seed prefix. ~23 validators matches the penumbra-1 mainnet
    /// active set size in the rotation block (10846002).
    fn make_validators(seed_prefix: &str, n: usize, voting_power: u64) -> Vec<Validator> {
        (0..n)
            .map(|i| Validator::new(&format!("{seed_prefix}-{i}")).voting_power(voting_power))
            .collect()
    }

    /// Generate a `LightBlock` at `height` whose own validator set is `vals`
    /// and whose `next_validators_hash` field encodes `next_vals`.
    fn make_light_block(
        chain_id: &str,
        height: u64,
        vals: &[Validator],
        next_vals: &[Validator],
    ) -> LightBlock {
        let header = TgHeader::new(vals)
            .height(height)
            .chain_id(chain_id)
            .next_validators(next_vals)
            .time(
                tendermint::Time::from_unix_timestamp(1_700_000_000 + height as i64, 0).unwrap(),
            );
        let commit = Commit::new(header.clone(), 1);
        let tg = TgLightBlock::new(header, commit).validators(vals).next_validators(next_vals);
        let tm: TmLightBlock = tg.generate().expect("testgen LightBlock");
        LightBlock {
            signed_header: tm.signed_header,
            validators: tm.validators,
            next_validators: tm.next_validators,
            provider: tm.provider,
        }
    }

    /// Compute the validator-set hash that a testgen-generated header at
    /// the given height would store, given the validator definitions.
    fn val_set_hash(vals: &[Validator]) -> tendermint::Hash {
        let infos =
            tendermint_testgen::validator::generate_validators(vals).expect("generate validators");
        ValidatorSet::without_proposer(infos).hash()
    }

    fn h(rev: u64, height: u64) -> ICSHeight {
        ICSHeight::new(rev, height).unwrap()
    }

    /// In-memory fetcher backed by a HashMap<height, ValidatorSet>.
    /// Returns a function suitable for `adjust_headers_with_fetcher`.
    fn vals_fetcher(
        map: HashMap<u64, ValidatorSet>,
    ) -> impl FnMut(ICSHeight) -> Result<ValidatorSet, Error> {
        move |req: ICSHeight| {
            map.get(&req.revision_height())
                .cloned()
                .ok_or_else(|| Error::other(format!("mock: no validators at h={req}")))
        }
    }

    // -------------------------------------------------------------------------
    // TEST 1: trusted_validator_set is taken from trusted_height+1, NOT
    //         target_height+1 — even on the expired-client fallback path.
    //
    // Setup mirrors the penumbra-1 → noble-1 production failure
    // (07-tendermint-109, trusted=10846001, target=10846002):
    //   - validator set A at trusted_height+1     (i.e. vals@H+1 = SET_A)
    //   - validator set B at target_height+1      (i.e. vals@H+2 = SET_B)
    //   - the rotation announce block is target_height = H+1
    //     (validators_hash@H+1 = SET_A, next_validators_hash@H+1 = SET_B)
    //
    // Invariant: `update_header.trusted_validator_set.hash() == hash(SET_A)`.
    // Pre-c0e16b07 the expired-fallback produced `hash(SET_B)` and the chain
    // rejected the update at update.go:224 with
    //   `does not hash to latest trusted validators`.
    // -------------------------------------------------------------------------

    /// The expired-client fallback at tendermint.rs:72-86 MUST use
    /// `vals_at(trusted_height + 1)` — proven by the c0e16b07 regression.
    ///
    /// This test drives [`expired_fallback_with_fetcher`] — the same function
    /// the production fallback branch in `header_and_minimal_set` calls —
    /// with a fetcher whose returned validator set depends on the requested
    /// height. If the production code regresses to fetching at
    /// `target_height + 1` (as it did pre-c0e16b07), the fetcher returns
    /// SET_B and this test fails.
    #[test]
    fn trusted_vals_uses_trusted_height_plus_one_on_expired_fallback() {
        // ~23 validators matches penumbra-1 mainnet sizes.
        let set_a = make_validators("set-a", 23, 100);
        let set_b = make_validators("set-b", 23, 100);

        let trusted_height = h(1, 10_846_001);
        let target_height = h(1, 10_846_002);

        // Build the target light block (the rotation announce block at H+1):
        //   validators_hash@target      = hash(SET_A)
        //   next_validators_hash@target = hash(SET_B)
        let target_lb = make_light_block(
            "penumbra-1",
            target_height.revision_height(),
            &set_a,
            &set_b,
        );

        // Synthesize the source-chain perspective:
        //   vals @ trusted_height + 1 (= H+1) = SET_A
        //   vals @ target_height  + 1 (= H+2) = SET_B
        let set_a_vs = ValidatorSet::without_proposer(
            tendermint_testgen::validator::generate_validators(&set_a).unwrap(),
        );
        let set_b_vs = ValidatorSet::without_proposer(
            tendermint_testgen::validator::generate_validators(&set_b).unwrap(),
        );

        // Track which heights the fetcher was called with. The protocol
        // invariant is that the fallback path queries trusted+1 and ONLY
        // trusted+1 — never target+1.
        let trusted_plus_one = trusted_height.increment().revision_height();
        let target_plus_one = target_height.increment().revision_height();

        let requested_heights: std::cell::RefCell<Vec<u64>> =
            std::cell::RefCell::new(Vec::new());

        let mut fetcher = |req: ICSHeight| -> Result<ValidatorSet, Error> {
            requested_heights.borrow_mut().push(req.revision_height());
            let height_v = req.revision_height();
            if height_v == trusted_plus_one {
                Ok(set_a_vs.clone())
            } else if height_v == target_plus_one {
                Ok(set_b_vs.clone())
            } else {
                Err(Error::other(format!("unexpected fetch height {height_v}")))
            }
        };

        let header = expired_fallback_with_fetcher(
            target_lb,
            trusted_height,
            target_height,
            &mut fetcher,
        )
        .expect("expired fallback must succeed");

        // ---------- Behavioral invariants ----------

        // (1) The fetcher was called for trusted_height + 1, exactly once,
        //     and was NOT called for target_height + 1.
        let reqs = requested_heights.borrow().clone();
        assert!(
            reqs.contains(&trusted_plus_one),
            "fallback must fetch vals at trusted_height+1 ({trusted_plus_one}); fetched: {reqs:?}"
        );
        assert!(
            !reqs.contains(&target_plus_one),
            "REGRESSION: fallback fetched vals at target_height+1 ({target_plus_one}); \
             this is the pre-c0e16b07 bug shape. Requested heights: {reqs:?}"
        );
        assert_eq!(
            reqs,
            vec![trusted_plus_one],
            "fallback must make exactly one validator-set fetch, at trusted_height+1"
        );

        // (2) PROTOCOL INVARIANT (chain update.go:224):
        //     hash(header.trusted_validator_set) == hash(vals@(trusted+1)) == hash(SET_A)
        assert_eq!(
            header.trusted_validator_set.hash(),
            val_set_hash(&set_a),
            "trusted_validator_set must hash to vals@(trusted+1)"
        );
        assert_ne!(
            header.trusted_validator_set.hash(),
            val_set_hash(&set_b),
            "regression: trusted_validator_set hashed to vals@(target+1)=SET_B"
        );

        // (3) The trusted_height on the produced header is the input
        //     trusted_height (not silently bumped to target).
        assert_eq!(header.trusted_height, trusted_height);

        // (4) The header is built from the supplied target's signed_header
        //     and its own validator_set (not the trusted one).
        assert_eq!(
            header.validator_set.hash(),
            val_set_hash(&set_a),
            "target's own validator_set should be vals@target (still SET_A here)"
        );
    }

    /// The non-expired path (`adjust_headers_with_fetcher` with empty
    /// supporting set) MUST also use `vals_at(trusted_height + 1)`. This is
    /// the analogue of the fallback test for the standard code path and
    /// guards against a future copy-paste regression on the other branch.
    #[test]
    fn trusted_vals_uses_trusted_height_plus_one_on_adjust_headers_path() {
        let set_a = make_validators("set-a", 23, 100);
        let set_b = make_validators("set-b", 23, 100);

        let trusted_height = h(1, 10_846_001);
        let target_height = h(1, 10_846_002);

        let target_lb = make_light_block(
            "penumbra-1",
            target_height.revision_height(),
            &set_a,
            &set_b,
        );

        // Pre-populate the fetcher with vals@(trusted+1) = SET_A only — if
        // adjust_headers (in the no-supporting case) wrongly asked for
        // vals@(target+1) the fetcher would error.
        let mut map = HashMap::new();
        map.insert(
            trusted_height.revision_height() + 1,
            ValidatorSet::without_proposer(
                tendermint_testgen::validator::generate_validators(&set_a).unwrap(),
            ),
        );
        let mut fetcher = vals_fetcher(map);

        let (header, supporting) =
            adjust_headers_with_fetcher(trusted_height, target_lb, vec![], &mut fetcher)
                .expect("adjust_headers must only fetch trusted_height+1 with empty supporting");

        assert!(supporting.is_empty());
        assert_eq!(header.trusted_height, trusted_height);
        assert_eq!(
            header.trusted_validator_set.hash(),
            val_set_hash(&set_a),
            "adjust_headers must seed trusted_validator_set from trusted_height+1"
        );
    }

    // -------------------------------------------------------------------------
    // TEST 2: Validator-rotation spanning update.
    //
    // Setup: trusted_height = H, supporting = [H+1], target = H+2. Validator
    // rotation at H+1 (announce). Each `TmHeader` produced must satisfy
    //   hash(header.trusted_validator_set) == hash(vals_at(header.trusted_height + 1))
    // because that's exactly what ibc-go/modules/light-clients/07-tendermint
    // `update.go:224` checks against the stored consensus_state.
    // -------------------------------------------------------------------------
    #[test]
    fn validator_rotation_spanning_update_preserves_chain_check_invariant() {
        let set_a = make_validators("set-a", 23, 100); // active at h..=H+1
        let set_b = make_validators("set-b", 23, 100); // active from H+2

        let h_trusted = h(1, 1_000_000);
        let h_support = h(1, 1_000_001); // rotation announce: vals=A, next_vals=B
        let h_target = h(1, 1_000_002);

        // Build supporting and target light blocks.
        let support_lb = make_light_block("test-chain-1", h_support.revision_height(), &set_a, &set_b);
        let target_lb = make_light_block("test-chain-1", h_target.revision_height(), &set_b, &set_b);

        // Fetcher returns vals_at(req_height). Specifically:
        //   vals@(H+1) = SET_A (since rotation is announced at H+1 itself)
        //   vals@(H+2) = SET_B
        //   vals@(H+3) = SET_B
        let set_a_vs =
            ValidatorSet::without_proposer(tendermint_testgen::validator::generate_validators(&set_a).unwrap());
        let set_b_vs =
            ValidatorSet::without_proposer(tendermint_testgen::validator::generate_validators(&set_b).unwrap());
        let mut map = HashMap::new();
        map.insert(h_trusted.revision_height() + 1, set_a_vs.clone());
        map.insert(h_support.revision_height() + 1, set_b_vs.clone());
        map.insert(h_target.revision_height() + 1, set_b_vs.clone());
        let mut fetcher = vals_fetcher(map);

        let (target_header, supporting_headers) = adjust_headers_with_fetcher(
            h_trusted,
            target_lb,
            vec![support_lb],
            &mut fetcher,
        )
        .expect("adjust_headers");

        // For EACH header (target + every supporting), enforce
        //   header.trusted_validator_set.hash() == hash(vals_at(header.trusted_height + 1))
        // which is the exact ibc-go update.go:224 check semantics.
        assert_eq!(supporting_headers.len(), 1);
        let supp = &supporting_headers[0];

        // First supporting: trusted_height = H, so expects vals@(H+1) = SET_A.
        assert_eq!(supp.trusted_height, h_trusted);
        assert_eq!(
            supp.trusted_validator_set.hash(),
            set_a_vs.hash(),
            "supporting header's trusted_validator_set must hash to vals@(trusted+1)"
        );

        // Target: trusted_height = H+1, so expects vals@(H+2) = SET_B.
        assert_eq!(target_header.trusted_height, h_support);
        assert_eq!(
            target_header.trusted_validator_set.hash(),
            set_b_vs.hash(),
            "target header's trusted_validator_set must hash to vals@(trusted+1) \
             where trusted is the previous supporting header's height"
        );

        // Sanity: the target's OWN validator_set is vals@target = SET_B
        // (post-rotation). This documents the rotation boundary so future
        // readers see the test's structural assumption clearly.
        assert_eq!(
            target_header.validator_set.hash(),
            set_b_vs.hash(),
            "target's own validator_set should be the post-rotation set"
        );
    }
}
