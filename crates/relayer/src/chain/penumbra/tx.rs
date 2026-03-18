//! Penumbra transaction building and submission.
//!
//! Separates transaction lifecycle (plan → build → sign → submit → confirm)
//! from the ChainEndpoint impl, making each step independently testable
//! and composable.

// This module will be populated as we migrate transaction methods
// from the monolithic penumbra.rs in subsequent phases.
//
// Phase 1: Define the module structure.
// Phase 2: Move build_penumbra_tx, send_messages_in_penumbratx,
//          submit_transaction here as standalone async functions.
