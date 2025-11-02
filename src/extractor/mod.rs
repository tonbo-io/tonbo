//! Arrow `RecordBatch` key extraction helpers.
//!
//! These APIs are Tonbo-specific shims that turn Arrow batches into the
//! zero-copy key views defined under [`crate::key`]. The compile-time typed
//! record support that previously lived here has been removed; keeping the module
//! focused on extraction keeps the ingest path clear.

mod errors;
mod extractors;
mod traits;

pub use errors::KeyExtractError;
pub use extractors::{
    BinaryKeyExtractor, CompositeProjection, F32KeyExtractor, F64KeyExtractor, I32KeyExtractor,
    I64KeyExtractor, U32KeyExtractor, U64KeyExtractor, Utf8KeyExtractor, projection_for_field,
    row_from_batch,
};
pub use traits::KeyProjection;
