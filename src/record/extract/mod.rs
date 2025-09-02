//! Batch-oriented key extraction for Arrow `RecordBatch`.
//!
//! These helpers are Tonbo-local and intentionally keep `typed-arrow-unified`
//! free of any DB-specific "key" concept.

mod errors;
mod extractors;
mod key_dyn;
mod traits;

pub use errors::KeyExtractError;
pub use extractors::{
    BinaryKeyExtractor, CompositeDynExtractor, F32KeyExtractor, F64KeyExtractor, I32KeyExtractor,
    I64KeyExtractor, U32KeyExtractor, U64KeyExtractor, Utf8KeyExtractor, dyn_extractor_for_field,
    row_from_batch,
};
pub use key_dyn::KeyDyn;
pub use traits::{BatchKeyExtractor, DynFromBatch, DynKeyExtractor};
