//! Zero-copy owning keys for string and binary Arrow columns.
//!
//! These keys hold a ref-counted handle to the Arrow values buffer and
//! offsets/lengths into that buffer. They implement `Ord` and `Borrow`
//! so they can be used as `BTreeMap` keys and queried with borrowed
//! forms like `&str` or `&[u8]`.

use std::{
    borrow::Borrow,
    cmp::Ordering,
    hash::{Hash, Hasher},
};

use arrow_array::{BinaryArray, StringArray};
use arrow_buffer::Buffer;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone)]
pub struct StrKey {
    data: Buffer,
    start: i32,
    len: i32,
}

impl StrKey {
    pub(crate) fn from_string_array(arr: &StringArray, row: usize) -> Self {
        // Offsets are i32 for Utf8
        let offsets = arr.value_offsets();
        // Safe indexing: `row+1` is in bounds for 0..num_rows
        let start = offsets[row];
        let end = offsets[row + 1];
        let len = end - start;
        let data = arr.values().clone();
        Self { data, start, len }
    }

    pub(crate) fn from_str(s: &str) -> Self {
        let data = Buffer::from(s.as_bytes());
        let start = 0i32;
        let len = s.len() as i32;
        Self { data, start, len }
    }

    /// Construct from an owned `String` without an intermediate copy.
    pub(crate) fn from_string_owned(s: String) -> Self {
        let len = s.len() as i32;
        let data: Buffer = Buffer::from(s.into_bytes());
        let start = 0i32;
        Self { data, start, len }
    }

    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        let s = self.start as usize;
        let e = (self.start + self.len) as usize;
        // Safety: indices were derived from Arrow's string offsets
        // and the buffer points to a valid contiguous region of bytes.
        unsafe { std::slice::from_raw_parts(self.data.as_ptr().add(s), e - s) }
    }

    #[inline]
    pub(crate) fn as_str(&self) -> &str {
        // Safety: Arrow guarantees UTF-8 validity for Utf8 arrays
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    /// Convert into an owned `String` (copies bytes).
    #[allow(dead_code)]
    pub(crate) fn into_string(self) -> String {
        self.as_str().to_owned()
    }
}

impl std::fmt::Debug for StrKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("StrKey").field(&self.as_str()).finish()
    }
}

impl PartialEq for StrKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl Eq for StrKey {}

impl PartialOrd for StrKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for StrKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl Borrow<str> for StrKey {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Hash for StrKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl Serialize for StrKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for StrKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(StrKey::from_string_owned(s))
    }
}

#[derive(Clone)]
pub struct BinKey {
    data: Buffer,
    start: i64,
    len: i64,
}

impl BinKey {
    #[allow(dead_code)]
    pub(crate) fn from_raw(data: Buffer, start: i64, len: i64) -> Self {
        Self { data, start, len }
    }
    pub(crate) fn from_binary_array(arr: &BinaryArray, row: usize) -> Self {
        // Offsets are i32 for BinaryArray
        let offsets = arr.value_offsets();
        let start = offsets[row] as i64;
        let end = offsets[row + 1] as i64;
        let len = end - start;
        let data = arr.values().clone();
        Self { data, start, len }
    }

    pub(crate) fn from_bytes(b: &[u8]) -> Self {
        let data: Buffer = Buffer::from(b.to_vec());
        let start = 0i64;
        let len = b.len() as i64;
        Self { data, start, len }
    }

    /// Construct from an owned `Vec<u8>` without an intermediate copy.
    pub(crate) fn from_vec_owned(b: Vec<u8>) -> Self {
        let len = b.len() as i64;
        let data: Buffer = Buffer::from(b);
        let start = 0i64;
        Self { data, start, len }
    }
    #[inline]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        let s = self.start as usize;
        let e = (self.start + self.len) as usize;
        unsafe { std::slice::from_raw_parts(self.data.as_ptr().add(s), e - s) }
    }

    /// Convert into an owned `Vec<u8>` (copies bytes).
    #[allow(dead_code)]
    pub(crate) fn into_vec(self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl std::fmt::Debug for BinKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BinKey").field(&self.as_bytes()).finish()
    }
}

impl PartialEq for BinKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl Eq for BinKey {}

impl PartialOrd for BinKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for BinKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl Borrow<[u8]> for BinKey {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Hash for BinKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl Serialize for BinKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self.as_bytes())
    }
}

impl<'de> Deserialize<'de> for BinKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let b = Vec::<u8>::deserialize(deserializer)?;
        Ok(BinKey::from_vec_owned(b))
    }
}
