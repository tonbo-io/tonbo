//! Record abstractions for typed-arrow backed rows using a unified key type.

use typed_arrow::schema::{BuildRows, RowBuilder, SchemaMeta};

// Compile extension macros for typed-arrow derive so they are available when
// consumers use `#[record(...)]` hooks.
mod ext_macros;
pub mod extract;

/// A typed record that defines its logical key and how to derive
/// keys directly from typed-arrow arrays.
pub trait Record: SchemaMeta + BuildRows + 'static {
    /// Logical key type. For string/binary keys this is an owning, zero-copy
    /// handle (e.g., `StrKey`, `BinKey`) that can also be compared against
    /// borrowed forms via `Borrow`.
    type Key: Ord + Clone;

    /// Payload stored alongside the key in the mutable memtable.
    /// Defaults to the whole record in macro-generated impls.
    type Payload;

    /// Compute the owned logical key from a row.
    fn key(&self) -> Self::Key;

    /// Consume `self` into `(Key, Payload)` for storage in the mutable memtable.
    fn split(self) -> (Self::Key, Self::Payload);

    /// Reconstruct a full record from `(Key, Payload)`.
    fn join(key: Self::Key, payload: Self::Payload) -> Self;

    /// Produce a key for the given `row` by borrowing from the typed arrays.
    /// For primitive types this reads the value; for string/binary types this
    /// creates a zero-copy key that references the underlying Arrow buffers.
    fn key_at(
        arrays: &<<Self as BuildRows>::Builders as RowBuilder<Self>>::Arrays,
        row: usize,
    ) -> Self::Key;
}
