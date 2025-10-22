use crate::{
    inmem::immutable::keys::{BinKey, StrKey},
    record::extract::KeyDyn,
};

/// Estimate heap usage of key types used in the mutable memtable.
/// For primitives and bools returns 0; for buffer-backed keys returns the byte length;
/// for tuples returns the sum of parts.
pub trait KeyHeapSize {
    fn key_heap_size(&self) -> usize;
}

impl KeyHeapSize for StrKey {
    fn key_heap_size(&self) -> usize {
        self.as_bytes().len()
    }
}

impl KeyHeapSize for BinKey {
    fn key_heap_size(&self) -> usize {
        self.as_bytes().len()
    }
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

impl KeyHeapSize for KeyDyn {
    fn key_heap_size(&self) -> usize {
        match self {
            KeyDyn::Str(s) => s.as_bytes().len(),
            KeyDyn::Bin(b) => b.as_bytes().len(),
            KeyDyn::Tuple(parts) => parts.iter().map(|k| k.key_heap_size()).sum(),
            _ => 0,
        }
    }
}
