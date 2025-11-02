use crate::key::{KeyComponentOwned, KeyOwned, KeyViewRaw};

/// Estimate heap usage of key types used in the mutable memtable.
/// For primitives and bools returns 0; for buffer-backed keys returns the byte length;
/// for tuples returns the sum of parts.
pub trait KeyHeapSize {
    fn key_heap_size(&self) -> usize;
}

macro_rules! impl_key_size_prim {
    ($($t:ty),* $(,)?) => { $( impl KeyHeapSize for $t { fn key_heap_size(&self) -> usize { 0 } } )* };
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
        key_owned_heap_size(self.component())
    }
}

fn key_owned_heap_size(component: &KeyComponentOwned) -> usize {
    match component {
        KeyComponentOwned::Utf8(s) | KeyComponentOwned::LargeUtf8(s) => s.len(),
        KeyComponentOwned::Binary(b)
        | KeyComponentOwned::LargeBinary(b)
        | KeyComponentOwned::FixedSizeBinary(b) => b.len(),
        KeyComponentOwned::Dictionary(inner) => key_owned_heap_size(inner),
        KeyComponentOwned::Struct(parts) => parts.iter().map(key_owned_heap_size).sum(),
        _ => 0,
    }
}

impl KeyHeapSize for KeyViewRaw {
    fn key_heap_size(&self) -> usize {
        self.heap_size()
    }
}
