use typed_arrow_dyn::DynCell;

use super::{KeyOwned, KeyRow};

/// Estimate heap usage of key types used across memtables and indexes.
/// Primitives and booleans report zero; buffer-backed components return their
/// byte length; tuples sum their parts.
pub trait KeyHeapSize {
    /// Approximate heap bytes consumed by the key representation.
    fn key_heap_size(&self) -> usize;
}

macro_rules! impl_key_size_prim {
    ($($t:ty),* $(,)?) => {
        $(
            impl KeyHeapSize for $t {
                fn key_heap_size(&self) -> usize {
                    0
                }
            }
        )*
    };
}

impl_key_size_prim!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, bool);

impl<A: KeyHeapSize, B: KeyHeapSize> KeyHeapSize for (A, B) {
    fn key_heap_size(&self) -> usize {
        self.0.key_heap_size() + self.1.key_heap_size()
    }
}

impl<A: KeyHeapSize, B: KeyHeapSize, C: KeyHeapSize> KeyHeapSize for (A, B, C) {
    fn key_heap_size(&self) -> usize {
        self.0.key_heap_size() + self.1.key_heap_size() + self.2.key_heap_size()
    }
}

impl KeyHeapSize for KeyOwned {
    fn key_heap_size(&self) -> usize {
        self.as_row()
            .cells()
            .iter()
            .filter_map(|cell| cell.as_ref())
            .map(dyn_cell_owned_heap_size)
            .sum()
    }
}

fn dyn_cell_owned_heap_size(cell: &DynCell) -> usize {
    match cell {
        DynCell::Str(value) => value.len(),
        DynCell::Bin(bytes) => bytes.len(),
        DynCell::Struct(values) => values
            .iter()
            .filter_map(|cell| cell.as_ref())
            .map(dyn_cell_owned_heap_size)
            .sum(),
        DynCell::List(values) | DynCell::FixedSizeList(values) => values
            .iter()
            .filter_map(|cell| cell.as_ref())
            .map(dyn_cell_owned_heap_size)
            .sum(),
        DynCell::Map(entries) => entries
            .iter()
            .map(|(key, value)| {
                dyn_cell_owned_heap_size(key)
                    + value
                        .as_ref()
                        .map(dyn_cell_owned_heap_size)
                        .unwrap_or_default()
            })
            .sum(),
        DynCell::Union { value, .. } => value
            .as_deref()
            .map(dyn_cell_owned_heap_size)
            .unwrap_or_default(),
        _ => 0,
    }
}

impl KeyHeapSize for KeyRow {
    fn key_heap_size(&self) -> usize {
        self.heap_size()
    }
}
