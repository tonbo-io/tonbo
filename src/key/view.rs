//! Safe borrowed key view backed by [`crate::key::raw::KeyViewRaw`].
//!
//! This defines the outer façade we intend to hand to mutable indexes and lock
//! managers. The view today simply wraps the raw components and exposes helper
//! iterators; comparison, hashing, and Arrow-aware extraction logic will be
//! layered on in follow-up steps.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use super::raw::{KeyComponentRaw, KeyViewRaw};

/// Borrowed key view tied to the lifetime of the batch that owns the buffers.
#[derive(Clone)]
pub struct KeyView<'batch> {
    raw: KeyViewRaw,
    _marker: PhantomData<&'batch ()>,
}

impl<'batch> KeyView<'batch> {
    /// Create a borrowed view from raw components captured elsewhere.
    pub fn from_raw(raw: KeyViewRaw) -> Self {
        Self {
            raw,
            _marker: PhantomData,
        }
    }

    /// Access the underlying raw components.
    pub fn components(&self) -> &[KeyComponentRaw] {
        self.raw.as_slice()
    }

    /// Number of components recorded in this key.
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// Whether this view contains no components.
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    /// Convert into the owned representation.
    pub fn to_owned(&self) -> super::KeyOwned {
        self.raw.to_owned()
    }

    /// Expose the raw storage for callers that need to build new views.
    pub fn into_raw(self) -> KeyViewRaw {
        self.raw
    }
}

impl<'batch> fmt::Debug for KeyView<'batch> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyView").field("len", &self.len()).finish()
    }
}

impl<'batch> PartialEq for KeyView<'batch> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl<'batch> Eq for KeyView<'batch> {}

impl<'batch> PartialOrd for KeyView<'batch> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'batch> Ord for KeyView<'batch> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.raw.cmp(&other.raw)
    }
}

impl<'batch> Hash for KeyView<'batch> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}
