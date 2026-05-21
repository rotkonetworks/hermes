//! Top-level integration-test entry for the regression suite.
//!
//! Each module here owns one of the bug classes we hit in production
//! (see `RegressionTests.md` / commit message of the introducing commit).

mod regression {
    pub mod memory_non_growth;
    pub mod view_canonicality;
}
