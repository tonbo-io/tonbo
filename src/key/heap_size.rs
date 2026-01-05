#![allow(dead_code)]

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitive_key_heap_size_is_zero() {
        assert_eq!(0u64.key_heap_size(), 0);
        assert_eq!(false.key_heap_size(), 0);
    }

    #[test]
    fn tuple_key_heap_size_sums_components() {
        let key = (KeyOwned::from("ab"), KeyOwned::from(vec![1u8, 2, 3]));
        assert_eq!(key.key_heap_size(), 2 + 3);
    }

    #[test]
    fn key_owned_heap_size_counts_buffers() {
        let key = KeyOwned::from("alpha");
        assert_eq!(key.key_heap_size(), "alpha".len());
    }

    #[test]
    fn dyn_cell_heap_size_covers_nested_variants() {
        let struct_cell = DynCell::Struct(vec![
            Some(DynCell::Str("hi".to_string())),
            None,
            Some(DynCell::Bin(vec![1, 2])),
        ]);
        assert_eq!(dyn_cell_owned_heap_size(&struct_cell), 2 + 2);

        let list_cell = DynCell::List(vec![
            Some(DynCell::Str("a".to_string())),
            Some(DynCell::Bin(vec![3, 4, 5])),
        ]);
        assert_eq!(dyn_cell_owned_heap_size(&list_cell), 1 + 3);

        let fixed_list_cell = DynCell::FixedSizeList(vec![Some(DynCell::Str("zz".to_string()))]);
        assert_eq!(dyn_cell_owned_heap_size(&fixed_list_cell), 2);

        let map_cell = DynCell::Map(vec![
            (
                DynCell::Str("key".to_string()),
                Some(DynCell::Bin(vec![9, 8])),
            ),
            (DynCell::Str("k2".to_string()), None),
        ]);
        assert_eq!(dyn_cell_owned_heap_size(&map_cell), 3 + 2 + 2);

        let union_cell = DynCell::Union {
            type_id: 1,
            value: Some(Box::new(DynCell::Str("u".to_string()))),
        };
        assert_eq!(dyn_cell_owned_heap_size(&union_cell), 1);

        let union_null = DynCell::Union {
            type_id: 2,
            value: None,
        };
        assert_eq!(dyn_cell_owned_heap_size(&union_null), 0);
    }
}
