//! Raw key component storage used to back a borrowed key view.
//!
//! These types intentionally contain no lifetimes: they store raw pointers into
//! Arrow buffers plus the metadata required to rehydrate safe references once
//! the owning `RecordBatch` is pinned. The safe façade lives in
//! [`super::view::KeyView`].

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ptr::NonNull,
    sync::Arc,
};

use super::{KeyComponentOwned, KeyOwned};

/// Pointer + length describing a byte slice inside an Arrow buffer.
#[derive(Clone, Copy, Eq)]
pub struct SlicePtr {
    ptr: NonNull<u8>,
    len: usize,
}

impl SlicePtr {
    /// # Safety
    ///
    /// Caller must guarantee that `ptr` is valid for `len` bytes for the
    /// lifetime enforced by the eventual safe view. The buffer must remain
    /// pinned so the memory location does not move.
    pub unsafe fn from_raw_parts(ptr: *const u8, len: usize) -> Self {
        debug_assert!(!ptr.is_null());
        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr as *mut u8) },
            len,
        }
    }

    /// Pointer to the first byte.
    pub fn as_ptr(self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Length in bytes.
    pub fn len(self) -> usize {
        self.len
    }

    /// Whether the slice is empty.
    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    /// # Safety
    ///
    /// Caller must uphold the lifetime invariants described in [`from_raw_parts`].
    pub unsafe fn as_slice<'a>(self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len) }
    }
}

impl PartialEq for SlicePtr {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr && self.len == other.len
    }
}

impl Hash for SlicePtr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ptr.hash(state);
        self.len.hash(state);
    }
}

impl fmt::Debug for SlicePtr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlicePtr")
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .finish()
    }
}

/// Raw representation for a single key component.
///
/// Variants cover the Arrow logical types we plan to admit as key columns. List
/// and map types remain unsupported; struct/tuple components recurse via
/// `Struct`.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum KeyComponentRaw {
    /// Boolean component.
    Bool(bool),
    /// 32-bit signed integer component.
    I32(i32),
    /// 64-bit signed integer component.
    I64(i64),
    /// 32-bit unsigned integer component.
    U32(u32),
    /// 64-bit unsigned integer component.
    U64(u64),
    /// 32-bit floating-point component stored as IEEE bits.
    F32(u32),
    /// 64-bit floating-point component stored as IEEE bits.
    F64(u64),
    /// UTF-8 slice borrowed from an Arrow buffer.
    Utf8(SlicePtr),
    /// Large UTF-8 slice borrowed from an Arrow buffer.
    LargeUtf8(SlicePtr),
    /// Binary slice borrowed from an Arrow buffer.
    Binary(SlicePtr),
    /// Large binary slice borrowed from an Arrow buffer.
    LargeBinary(SlicePtr),
    /// Fixed-size binary slice borrowed from an Arrow buffer.
    FixedSizeBinary(SlicePtr),
    /// Dictionary component resolved to its value.
    Dictionary(Box<KeyComponentRaw>),
    /// Nested struct/tuple component.
    Struct(Vec<KeyComponentRaw>),
}

/// Collection of raw components backing a key view.
#[derive(Clone, Debug, Default)]
pub struct KeyViewRaw {
    components: Vec<KeyComponentRaw>,
}

impl KeyViewRaw {
    /// Construct an empty raw view.
    pub fn new() -> Self {
        Self {
            components: Vec::new(),
        }
    }

    /// Push a component captured during extraction.
    pub fn push(&mut self, component: KeyComponentRaw) {
        self.components.push(component);
    }

    /// Borrow captured components as a slice.
    pub fn as_slice(&self) -> &[KeyComponentRaw] {
        &self.components
    }

    /// Extend the raw view with additional components.
    pub fn extend_from_slice(&mut self, components: &[KeyComponentRaw]) {
        self.components.extend_from_slice(components);
    }

    /// Return number of components held.
    pub fn len(&self) -> usize {
        self.components.len()
    }

    /// Whether the raw view has no components.
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    /// Clear all captured components.
    pub fn clear(&mut self) {
        self.components.clear();
    }

    /// Borrow components mutably as a slice.
    pub fn as_mut_slice(&mut self) -> &mut [KeyComponentRaw] {
        &mut self.components
    }

    /// Estimate heap usage for the key (borrowed slices count towards their length).
    pub fn heap_size(&self) -> usize {
        self.components.iter().map(component_heap_size).sum()
    }

    /// Convert the raw key into an owned representation.
    pub fn to_owned(&self) -> KeyOwned {
        owned_from_components(self.as_slice())
    }

    /// Build a raw key that borrows from an owned key's buffers.
    pub fn from_owned(key: &KeyOwned) -> Self {
        KeyViewRaw {
            components: components_from_owned(key.component()),
        }
    }
}

fn owned_from_components(components: &[KeyComponentRaw]) -> KeyOwned {
    match components {
        [] => KeyOwned::new(KeyComponentOwned::Struct(Vec::new())),
        [single] => KeyOwned::new(component_to_owned(single)),
        many => {
            let parts = many.iter().map(component_to_owned).collect();
            KeyOwned::new(KeyComponentOwned::Struct(parts))
        }
    }
}

fn component_to_owned(component: &KeyComponentRaw) -> KeyComponentOwned {
    use KeyComponentOwned as O;
    use KeyComponentRaw::*;
    match component {
        Bool(v) => O::Bool(*v),
        I32(v) => O::I32(*v),
        I64(v) => O::I64(*v),
        U32(v) => O::U32(*v),
        U64(v) => O::U64(*v),
        F32(bits) => O::F32(*bits),
        F64(bits) => O::F64(*bits),
        Utf8(ptr) => {
            let slice = unsafe { ptr.as_slice() };
            let s = std::str::from_utf8(slice).expect("utf8 validated");
            O::Utf8(Arc::new(s.to_owned()))
        }
        LargeUtf8(ptr) => {
            let slice = unsafe { ptr.as_slice() };
            let s = std::str::from_utf8(slice).expect("utf8 validated");
            O::LargeUtf8(Arc::new(s.to_owned()))
        }
        Binary(ptr) => {
            let slice = unsafe { ptr.as_slice() };
            O::Binary(Arc::new(slice.to_vec()))
        }
        LargeBinary(ptr) => {
            let slice = unsafe { ptr.as_slice() };
            O::LargeBinary(Arc::new(slice.to_vec()))
        }
        FixedSizeBinary(ptr) => {
            let slice = unsafe { ptr.as_slice() };
            O::FixedSizeBinary(Arc::new(slice.to_vec()))
        }
        Dictionary(inner) => O::Dictionary(Box::new(component_to_owned(inner))),
        Struct(parts) => {
            let owned_parts = parts.iter().map(component_to_owned).collect();
            O::Struct(owned_parts)
        }
    }
}

fn components_from_owned(component: &KeyComponentOwned) -> Vec<KeyComponentRaw> {
    match component {
        KeyComponentOwned::Struct(parts) => {
            let mut flat = Vec::new();
            for part in parts {
                match component_from_owned(part) {
                    KeyComponentRaw::Struct(mut nested) => flat.append(&mut nested),
                    other => flat.push(other),
                }
            }
            vec![KeyComponentRaw::Struct(flat)]
        }
        other => vec![component_from_owned(other)],
    }
}

pub(crate) fn component_from_owned(component: &KeyComponentOwned) -> KeyComponentRaw {
    use KeyComponentOwned as O;
    use KeyComponentRaw as R;

    match component {
        O::Bool(v) => R::Bool(*v),
        O::I32(v) => R::I32(*v),
        O::I64(v) => R::I64(*v),
        O::U32(v) => R::U32(*v),
        O::U64(v) => R::U64(*v),
        O::F32(bits) => R::F32(*bits),
        O::F64(bits) => R::F64(*bits),
        O::Utf8(value) => slice_ptr_from_str(value, R::Utf8),
        O::LargeUtf8(value) => slice_ptr_from_str(value, R::LargeUtf8),
        O::Binary(bytes) => slice_ptr_from_bytes(bytes, R::Binary),
        O::LargeBinary(bytes) => slice_ptr_from_bytes(bytes, R::LargeBinary),
        O::FixedSizeBinary(bytes) => slice_ptr_from_bytes(bytes, R::FixedSizeBinary),
        O::Dictionary(inner) => R::Dictionary(Box::new(component_from_owned(inner))),
        O::Struct(parts) => {
            let mut flat = Vec::new();
            for part in parts {
                match component_from_owned(part) {
                    R::Struct(mut nested) => flat.append(&mut nested),
                    other => flat.push(other),
                }
            }
            R::Struct(flat)
        }
    }
}

fn slice_ptr_from_str<F>(value: &Arc<String>, make: F) -> KeyComponentRaw
where
    F: FnOnce(SlicePtr) -> KeyComponentRaw,
{
    let bytes = value.as_bytes();
    let ptr = unsafe { SlicePtr::from_raw_parts(bytes.as_ptr(), bytes.len()) };
    make(ptr)
}

fn slice_ptr_from_bytes<F>(value: &Arc<Vec<u8>>, make: F) -> KeyComponentRaw
where
    F: FnOnce(SlicePtr) -> KeyComponentRaw,
{
    let slice = value.as_slice();
    let ptr = unsafe { SlicePtr::from_raw_parts(slice.as_ptr(), slice.len()) };
    make(ptr)
}

pub(crate) fn components_equal(a: &KeyComponentRaw, b: &KeyComponentRaw) -> bool {
    use KeyComponentRaw::*;
    match (a, b) {
        (Bool(lhs), Bool(rhs)) => lhs == rhs,
        (I32(lhs), I32(rhs)) => lhs == rhs,
        (I64(lhs), I64(rhs)) => lhs == rhs,
        (U32(lhs), U32(rhs)) => lhs == rhs,
        (U64(lhs), U64(rhs)) => lhs == rhs,
        (F32(lhs), F32(rhs)) => floats_equal_f32(*lhs, *rhs),
        (F64(lhs), F64(rhs)) => floats_equal_f64(*lhs, *rhs),
        (Utf8(lhs), Utf8(rhs))
        | (Utf8(lhs), LargeUtf8(rhs))
        | (LargeUtf8(lhs), Utf8(rhs))
        | (LargeUtf8(lhs), LargeUtf8(rhs)) => slice_bytes_equal(*lhs, *rhs),
        (Binary(lhs), Binary(rhs))
        | (Binary(lhs), LargeBinary(rhs))
        | (Binary(lhs), FixedSizeBinary(rhs))
        | (LargeBinary(lhs), Binary(rhs))
        | (LargeBinary(lhs), LargeBinary(rhs))
        | (LargeBinary(lhs), FixedSizeBinary(rhs))
        | (FixedSizeBinary(lhs), Binary(rhs))
        | (FixedSizeBinary(lhs), LargeBinary(rhs))
        | (FixedSizeBinary(lhs), FixedSizeBinary(rhs)) => slice_bytes_equal(*lhs, *rhs),
        (Dictionary(lhs), Dictionary(rhs)) => components_equal(lhs, rhs),
        (Struct(lhs), Struct(rhs)) => {
            lhs.len() == rhs.len()
                && lhs
                    .iter()
                    .zip(rhs.iter())
                    .all(|(l, r)| components_equal(l, r))
        }
        _ => false,
    }
}

pub(crate) fn components_cmp(a: &KeyComponentRaw, b: &KeyComponentRaw) -> Ordering {
    use KeyComponentRaw::*;
    match (a, b) {
        (Bool(lhs), Bool(rhs)) => lhs.cmp(rhs),
        (I32(lhs), I32(rhs)) => lhs.cmp(rhs),
        (I64(lhs), I64(rhs)) => lhs.cmp(rhs),
        (U32(lhs), U32(rhs)) => lhs.cmp(rhs),
        (U64(lhs), U64(rhs)) => lhs.cmp(rhs),
        (F32(lhs), F32(rhs)) => floats_cmp_f32(*lhs, *rhs),
        (F64(lhs), F64(rhs)) => floats_cmp_f64(*lhs, *rhs),
        (Utf8(lhs), Utf8(rhs))
        | (Utf8(lhs), LargeUtf8(rhs))
        | (LargeUtf8(lhs), Utf8(rhs))
        | (LargeUtf8(lhs), LargeUtf8(rhs)) => slice_bytes_cmp(*lhs, *rhs),
        (Binary(lhs), Binary(rhs))
        | (Binary(lhs), LargeBinary(rhs))
        | (Binary(lhs), FixedSizeBinary(rhs))
        | (LargeBinary(lhs), Binary(rhs))
        | (LargeBinary(lhs), LargeBinary(rhs))
        | (LargeBinary(lhs), FixedSizeBinary(rhs))
        | (FixedSizeBinary(lhs), Binary(rhs))
        | (FixedSizeBinary(lhs), LargeBinary(rhs))
        | (FixedSizeBinary(lhs), FixedSizeBinary(rhs)) => slice_bytes_cmp(*lhs, *rhs),
        (Dictionary(lhs), Dictionary(rhs)) => components_cmp(lhs, rhs),
        (Struct(lhs), Struct(rhs)) => {
            for (l, r) in lhs.iter().zip(rhs.iter()) {
                let ord = components_cmp(l, r);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            lhs.len().cmp(&rhs.len())
        }
        _ => component_tag(a).cmp(&component_tag(b)),
    }
}

pub(crate) fn hash_component<H: Hasher>(component: &KeyComponentRaw, state: &mut H) {
    use KeyComponentRaw::*;
    component_tag(component).hash(state);
    match component {
        Bool(v) => v.hash(state),
        I32(v) => v.hash(state),
        I64(v) => v.hash(state),
        U32(v) => v.hash(state),
        U64(v) => v.hash(state),
        F32(bits) => hash_float_f32(*bits, state),
        F64(bits) => hash_float_f64(*bits, state),
        Utf8(ptr) | LargeUtf8(ptr) | Binary(ptr) | LargeBinary(ptr) | FixedSizeBinary(ptr) => {
            hash_slice_bytes(*ptr, state)
        }
        Dictionary(inner) => hash_component(inner, state),
        Struct(parts) => {
            parts.len().hash(state);
            for part in parts {
                hash_component(part, state);
            }
        }
    }
}

fn component_tag(component: &KeyComponentRaw) -> u8 {
    use KeyComponentRaw::*;
    match component {
        Utf8(_) | LargeUtf8(_) => 0,
        Binary(_) | LargeBinary(_) | FixedSizeBinary(_) => 1,
        U64(_) => 2,
        U32(_) => 3,
        I64(_) => 4,
        I32(_) => 5,
        F64(_) => 6,
        F32(_) => 7,
        Bool(_) => 8,
        Dictionary(inner) => component_tag(inner),
        Struct(_) => 9,
    }
}

fn floats_equal_f32(lhs: u32, rhs: u32) -> bool {
    let a = f32::from_bits(lhs);
    let b = f32::from_bits(rhs);
    (a.is_nan() && b.is_nan()) || a == b
}

fn floats_equal_f64(lhs: u64, rhs: u64) -> bool {
    let a = f64::from_bits(lhs);
    let b = f64::from_bits(rhs);
    (a.is_nan() && b.is_nan()) || a == b
}

fn floats_cmp_f32(lhs: u32, rhs: u32) -> Ordering {
    let a = f32::from_bits(lhs);
    let b = f32::from_bits(rhs);
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

fn floats_cmp_f64(lhs: u64, rhs: u64) -> Ordering {
    let a = f64::from_bits(lhs);
    let b = f64::from_bits(rhs);
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

fn hash_float_f32<H: Hasher>(bits: u32, state: &mut H) {
    if f32::from_bits(bits).is_nan() {
        0xFFu8.hash(state);
    } else {
        bits.hash(state);
    }
}

fn hash_float_f64<H: Hasher>(bits: u64, state: &mut H) {
    if f64::from_bits(bits).is_nan() {
        0xFFu8.hash(state);
    } else {
        bits.hash(state);
    }
}

fn slice_bytes_equal(lhs: SlicePtr, rhs: SlicePtr) -> bool {
    unsafe { lhs.as_slice() == rhs.as_slice() }
}

fn slice_bytes_cmp(lhs: SlicePtr, rhs: SlicePtr) -> Ordering {
    unsafe { lhs.as_slice().cmp(rhs.as_slice()) }
}

fn hash_slice_bytes<H: Hasher>(ptr: SlicePtr, state: &mut H) {
    let bytes = unsafe { ptr.as_slice() };
    bytes.len().hash(state);
    state.write(bytes);
}

fn component_heap_size(component: &KeyComponentRaw) -> usize {
    use KeyComponentRaw::*;
    match component {
        Utf8(ptr) | LargeUtf8(ptr) | Binary(ptr) | LargeBinary(ptr) | FixedSizeBinary(ptr) => {
            ptr.len()
        }
        Dictionary(inner) => component_heap_size(inner),
        Struct(parts) => parts.iter().map(component_heap_size).sum(),
        _ => 0,
    }
}

impl PartialEq for KeyViewRaw {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        self.as_slice()
            .iter()
            .zip(other.as_slice())
            .all(|(a, b)| components_equal(a, b))
    }
}

impl Eq for KeyViewRaw {}

impl PartialOrd for KeyViewRaw {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyViewRaw {
    fn cmp(&self, other: &Self) -> Ordering {
        for (a, b) in self.as_slice().iter().zip(other.as_slice()) {
            let ord = components_cmp(a, b);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        self.len().cmp(&other.len())
    }
}

impl Hash for KeyViewRaw {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for component in self.as_slice() {
            hash_component(component, state);
        }
    }
}
