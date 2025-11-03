//! Owned key representation aligned with the zero-copy `KeyView` façade.
//!
//! `KeyOwned` stores a hierarchy of `KeyComponentOwned` values that mirror the
//! borrowed structure captured by [`crate::key::raw::KeyViewRaw`]. Variable-width
//! components retain their bytes in reference-counted buffers so clones remain
//! cheap while keeping equality, ordering, and hashing semantics identical to the
//! borrowed view.

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use super::raw::{
    KeyViewRaw, component_from_owned, components_cmp, components_equal, hash_component,
};

/// Owned key component used to build durable copies of borrowed key views.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KeyComponentOwned {
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
    /// UTF-8 slice owned as a reference-counted `String`.
    Utf8(Arc<String>),
    /// Large UTF-8 slice owned as a reference-counted `String`.
    LargeUtf8(Arc<String>),
    /// Binary slice stored in a shared `Vec<u8>` buffer.
    Binary(Arc<Vec<u8>>),
    /// Large binary slice stored in a shared `Vec<u8>` buffer.
    LargeBinary(Arc<Vec<u8>>),
    /// Fixed-size binary slice stored in a shared `Vec<u8>` buffer.
    FixedSizeBinary(Arc<Vec<u8>>),
    /// Dictionary-resolved component.
    Dictionary(Box<KeyComponentOwned>),
    /// Struct/tuple component consisting of nested parts.
    Struct(Vec<KeyComponentOwned>),
}

impl KeyComponentOwned {
    /// Returns a view over the component interpreted as UTF-8 if applicable.
    pub fn as_utf8(&self) -> Option<&str> {
        match self {
            KeyComponentOwned::Utf8(v) | KeyComponentOwned::LargeUtf8(v) => Some(v.as_str()),
            _ => None,
        }
    }

    /// Returns a view over the component interpreted as binary if applicable.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            KeyComponentOwned::Binary(v)
            | KeyComponentOwned::LargeBinary(v)
            | KeyComponentOwned::FixedSizeBinary(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

impl PartialEq for KeyComponentOwned {
    fn eq(&self, other: &Self) -> bool {
        let lhs = component_from_owned(self);
        let rhs = component_from_owned(other);
        components_equal(&lhs, &rhs)
    }
}

impl Eq for KeyComponentOwned {}

impl PartialOrd for KeyComponentOwned {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyComponentOwned {
    fn cmp(&self, other: &Self) -> Ordering {
        let lhs = component_from_owned(self);
        let rhs = component_from_owned(other);
        components_cmp(&lhs, &rhs)
    }
}

impl Hash for KeyComponentOwned {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let raw = component_from_owned(self);
        hash_component(&raw, state);
    }
}

/// Owned key that can outlive the `RecordBatch` it originated from.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyOwned {
    component: KeyComponentOwned,
}

impl KeyOwned {
    /// Create an owned key from a component.
    pub fn new(component: KeyComponentOwned) -> Self {
        Self { component }
    }

    /// Access the component backing this key.
    pub fn component(&self) -> &KeyComponentOwned {
        &self.component
    }

    /// Consume the key and return its component.
    pub fn into_component(self) -> KeyComponentOwned {
        self.component
    }

    /// Build a composite key from owned parts.
    pub fn tuple(parts: Vec<Self>) -> Self {
        let comps = parts
            .into_iter()
            .map(KeyOwned::into_component)
            .collect::<Vec<_>>();
        KeyOwned::new(KeyComponentOwned::Struct(comps))
    }

    /// Returns the key interpreted as UTF-8 if applicable.
    pub fn as_utf8(&self) -> Option<&str> {
        self.component.as_utf8()
    }

    /// Returns the key interpreted as raw bytes if applicable.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.component.as_bytes()
    }
}

impl From<&str> for KeyOwned {
    fn from(value: &str) -> Self {
        KeyOwned::new(KeyComponentOwned::Utf8(Arc::new(value.to_owned())))
    }
}

impl From<String> for KeyOwned {
    fn from(value: String) -> Self {
        KeyOwned::new(KeyComponentOwned::Utf8(Arc::new(value)))
    }
}

impl From<&[u8]> for KeyOwned {
    fn from(value: &[u8]) -> Self {
        KeyOwned::new(KeyComponentOwned::Binary(Arc::new(value.to_vec())))
    }
}

impl From<Vec<u8>> for KeyOwned {
    fn from(value: Vec<u8>) -> Self {
        KeyOwned::new(KeyComponentOwned::Binary(Arc::new(value)))
    }
}

macro_rules! impl_from_scalar {
    ($variant:ident, $t:ty, $map:expr) => {
        impl From<$t> for KeyOwned {
            fn from(value: $t) -> Self {
                KeyOwned::new(KeyComponentOwned::$variant($map(value)))
            }
        }
    };
}

impl_from_scalar!(Bool, bool, |v| v);
impl_from_scalar!(I32, i32, |v| v);
impl_from_scalar!(I64, i64, |v| v);
impl_from_scalar!(U32, u32, |v| v);
impl_from_scalar!(U64, u64, |v| v);
impl_from_scalar!(F32, f32, |v: f32| v.to_bits());
impl_from_scalar!(F64, f64, |v: f64| v.to_bits());

impl PartialEq for KeyOwned {
    fn eq(&self, other: &Self) -> bool {
        let lhs = KeyViewRaw::from_owned(self);
        let rhs = KeyViewRaw::from_owned(other);
        lhs == rhs
    }
}

impl Eq for KeyOwned {}

impl PartialOrd for KeyOwned {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyOwned {
    fn cmp(&self, other: &Self) -> Ordering {
        let lhs = KeyViewRaw::from_owned(self);
        let rhs = KeyViewRaw::from_owned(other);
        lhs.cmp(&rhs)
    }
}

impl Hash for KeyOwned {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let raw = KeyViewRaw::from_owned(self);
        raw.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use super::*;
    use crate::key::raw::{KeyComponentRaw, KeyViewRaw};

    fn hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn utf8_variants_compare_identically() {
        let small = KeyOwned::new(KeyComponentOwned::Utf8(Arc::new("hello".to_string())));
        let large = KeyOwned::new(KeyComponentOwned::LargeUtf8(Arc::new("hello".to_string())));

        assert_eq!(small, large);
        assert_eq!(small.cmp(&large), Ordering::Equal);
        assert_eq!(hash(&small), hash(&large));
    }

    #[test]
    fn binary_variants_compare_identically() {
        let bytes = vec![1, 2, 3, 4];
        let binary = KeyOwned::new(KeyComponentOwned::Binary(Arc::new(bytes.clone())));
        let large = KeyOwned::new(KeyComponentOwned::LargeBinary(Arc::new(bytes.clone())));
        let fixed = KeyOwned::new(KeyComponentOwned::FixedSizeBinary(Arc::new(bytes)));

        assert_eq!(binary, large);
        assert_eq!(binary, fixed);
        assert_eq!(binary.cmp(&large), Ordering::Equal);
        assert_eq!(hash(&binary), hash(&fixed));
    }

    #[test]
    fn tuple_owned_matches_components() {
        let part_a = KeyOwned::from("alpha");
        let part_b = KeyOwned::from(42u64);
        let tuple = KeyOwned::tuple(vec![part_a.clone(), part_b.clone()]);

        let raw = KeyViewRaw::from_owned(&tuple);
        assert!(matches!(
            raw.as_slice(),
            [KeyComponentRaw::Utf8(_), KeyComponentRaw::U64(_)]
        ));
        assert_eq!(raw.as_slice().len(), 2);

        let rebuilt = KeyOwned::tuple(vec![part_a, part_b]);
        assert_eq!(tuple, rebuilt);
    }

    #[test]
    fn nan_components_compare_equal() {
        let nan_bits = f32::NAN.to_bits();
        let left = KeyComponentOwned::F32(nan_bits);
        let right = KeyComponentOwned::F32(nan_bits);

        assert_eq!(left, right);
        assert_eq!(left.cmp(&right), Ordering::Equal);
        assert_eq!(hash(&left), hash(&right));
    }
}
