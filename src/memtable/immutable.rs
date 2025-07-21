use std::collections::BTreeMap;

use crate::{
    memtable::mutable::MutableMemTable,
    schema::{ArrowArrays, Key, Schema},
    version::Ts,
};

/// Immutable memtable with columnar storage
///
/// SAFETY: The index contains references to data in `arrays`. Since both fields
/// are owned by the struct and never moved after construction, these references
/// remain valid for the lifetime of the ImmutableMemTable.
#[allow(dead_code)]
pub(crate) struct ImmutableMemTable<A: ArrowArrays> {
    /// Schema defining the structure and primary key
    schema: A::Schema,
    /// Columnar data storage - must be listed before index to ensure
    /// it's dropped after the index (which contains references to it)
    arrays: A,
    /// Index mapping key references to row positions
    index: BTreeMap<Ts<<<A::Schema as Schema>::Key as Key>::Ref<'static>>, usize>,
}

impl<A: ArrowArrays> ImmutableMemTable<A> {
    /// Create an immutable memtable from a mutable one
    ///
    /// This consumes the mutable memtable and converts its row-based storage
    /// to columnar storage with an index for efficient key lookups.
    #[allow(dead_code)]
    pub(crate) fn from_mutable(mutable: MutableMemTable<A::Row>, schema: A::Schema) -> Self {
        // Type alias for clarity
        type Entry<K, R> = (Ts<K>, Option<R>);

        // Collect all entries from the mutable memtable
        let entries: Vec<Entry<<A::Schema as Schema>::Key, A::Row>> = mutable.into_iter().collect();

        // Separate entries into those with rows and their timestamps
        let mut rows = Vec::new();
        let mut ts_keys = Vec::new();

        for (ts_key, opt_row) in entries {
            if let Some(row) = opt_row {
                ts_keys.push(ts_key);
                rows.push(row);
            }
        }

        // Convert rows to columnar format
        let arrays = A::from_rows(rows, &schema);

        // Build the index
        let mut index = BTreeMap::new();

        for (row_idx, ts_key) in ts_keys.into_iter().enumerate() {
            // Extract key reference from the arrays
            let key_ref = arrays.key_ref_at(row_idx, &schema);

            // SAFETY: We're transmuting the lifetime to 'static. This is safe because:
            // 1. The arrays field owns the data that key_ref points to
            // 2. The arrays field is never moved after construction
            // 3. The index is dropped before arrays (due to field order)
            let static_key_ref = unsafe {
                std::mem::transmute::<
                    <<A::Schema as Schema>::Key as Key>::Ref<'_>,
                    <<A::Schema as Schema>::Key as Key>::Ref<'static>,
                >(key_ref)
            };

            let ts_key_ref = Ts::new(static_key_ref, ts_key.ts);
            index.insert(ts_key_ref, row_idx);
        }

        Self {
            schema,
            arrays,
            index,
        }
    }

    #[cfg(test)]
    pub(crate) fn arrays(&self) -> &A {
        &self.arrays
    }

    #[cfg(test)]
    pub(crate) fn index_len(&self) -> usize {
        self.index.len()
    }
}
