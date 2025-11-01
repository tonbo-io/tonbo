//! Record-oriented helpers.
//!
//! Compile-time typed record support has been removed for now. The dynamic
//! extraction APIs remain available under `record::extract`, and the `Mode`
//! trait still leaves room to bring typed dispatch back in the future.

pub mod extract;

pub use extract::{BatchKeyExtractor, DynKeyExtractor, KeyExtractError, dyn_extractor_for_field};
