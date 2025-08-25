// Public bridge for typed-arrow integration within Tonbo.
// - `typed::Record`: re-export typed-arrow's derive macro (optional when using `typed::record`).
// - `typed::record`: attribute macro that injects Tonbo PK metadata and ensures
//   `#[derive(typed_arrow::Record)]` is present exactly once. This enables a single-annotation
//   usage style for typed records.

pub use tonbo_macros::typed_record as record;
pub use typed_arrow::Record;

// Common metadata keys specific to typed integration.
pub mod meta {
    pub const PK_USER_INDICES: &str = "tonbo.primary_key_user_indices";
}
