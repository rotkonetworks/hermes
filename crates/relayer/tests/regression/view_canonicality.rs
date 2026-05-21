//! # Regression test #3: Penumbra view-service canonicality on warm operation
//!
//! ## Protocol invariant
//!
//! After applying every `CompactBlock` from height `H_genesis..=H_now` to the
//! local view's SCT (Sparse Commitment Tree), the local
//! `sct.root()` MUST equal the `anchor` field that pd recorded at the end of
//! that block via its own `write_sct` call. In other words, the **view's
//! tree state must equal the chain's tree state at the same height** — not
//! "for transactions we know about", but **canonically**.
//!
//! This is the property the canonical-anchor gate (89fe4d00 +
//! c620122c) defends at tx-submit time:
//!
//! > before broadcast, fetch the chain's canonical anchor at our height and
//! > assert it equals the anchor we just built into the proof.
//!
//! But the *root cause* of the production divergence is upstream: the view
//! service worker mutates the SCT on a per-block basis, and any deviation —
//! a re-applied block, a dropped commitment, a missed nullifier — produces
//! a permanent fork between view-root and chain-root that no later
//! reconciliation can heal without a wipe.
//!
//! ## Why this test is `#[ignore]`
//!
//! Task A2 ("Make cold-restart view canonical (SCT reconstruction from
//! sqlite)") is not yet landed. Until A2 ships:
//!   - the relayer crate does not depend on `penumbra-sdk-tct`, so we
//!     cannot construct an `sct::Tree` in-process here without pulling in
//!     a large, non-trivial dep with native-build requirements that the
//!     rest of hermes does not currently need;
//!   - the hermes-side view worker still has the cold-restart non-canonical
//!     path (the very thing A2 fixes).
//!
//! Writing this test as a *weakened* shape (e.g. "assert the view roots
//! between two arbitrary heights are not equal", which is trivially true)
//! would make a passing test that does not enforce the invariant. The
//! task instructions explicitly forbid that. So the body below is the
//! correct shape, gated, and ready to be enabled when A2 lands.
//!
//! When A2 is in place, remove the `#[ignore]` attribute, add
//! `penumbra-sdk-tct` to `[dev-dependencies]`, and replace the
//! `unimplemented!()` bodies below with calls into the real worker entry
//! points.

/// SPEC: for every applied block, view.sct.root() == chain.write_sct_root.
///
/// Step-by-step:
///   1. Seed an `sct::Tree` (the view's local copy) and a parallel
///      `sct::Tree` (the chain's authoritative copy) with the same
///      empty initial state.
///   2. For each `block_height` in `1..=N`:
///        - generate a deterministic set of `state_payloads` for that
///          block (use a fixed `rand_chacha::ChaCha20Rng` seed so the
///          payloads are reproducible across runs);
///        - drive them through the hermes view-service worker (the same
///          code path that processes a CompactBlock subscription);
///        - have the parallel "chain" tree apply the same payloads via
///          its own `end_block` equivalent and capture its `root()`;
///        - assert `view.sct.root() == chain.sct.root()` AFTER the
///          worker's `end_block` returns.
///   3. The assertion MUST hold for every height, not just the last:
///      any single divergence is a permanent fork.
///
/// Acceptance criteria (DO NOT WEAKEN):
///   - Use `tct::Tree::new()` for both trees (canonical empty state).
///   - Use a fixed RNG seed for `state_payloads`.
///   - Run for N >= 200 blocks. Production has shown divergences that
///     only manifest after long sequences (the SCT-flap shape).
///   - Compare full `sct::Tree::root()`, not a derived/truncated hash.
#[test]
#[ignore = "unblocks when A2 lands — requires SCT reconstruction code & penumbra-sdk-tct dev-dep"]
fn view_canonicality_on_warm_operation_matches_chain_root() {
    // Intentionally left unimplemented. See module-level docstring for the
    // exact shape this must take when A2 lands; do NOT inline a weakened
    // assertion to make this pass.
    unimplemented!(
        "Implement when A2 lands. The SPEC is documented above; \
         do not weaken the equality assertion."
    );
}

/// SPEC: cold-restart from sqlite reproduces the same SCT root as warm.
///
/// This is the actual A2 acceptance test, as a sibling to the warm test:
///   1. Run the warm test up to height N, capture `root_warm = view.sct.root()`.
///   2. Drop the in-memory tree.
///   3. Re-instantiate the view from the persisted sqlite store using
///      the new A2 reconstruction path.
///   4. Capture `root_cold = reconstructed.sct.root()`.
///   5. Assert `root_warm == root_cold`.
///
/// Pre-A2: cold reconstruction is non-canonical (the bug A2 fixes), so
/// this test is expected to FAIL when first enabled. That's the point.
#[test]
#[ignore = "unblocks when A2 lands — this is the A2 acceptance test"]
fn cold_restart_view_root_equals_warm_root() {
    unimplemented!(
        "Implement when A2 lands. This test is expected to fail \
         pre-A2 (that's the bug A2 fixes), and pass post-A2."
    );
}
