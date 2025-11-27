//! Shared row-set abstractions built on top of roaring bitmaps.

use std::convert::TryFrom;

use roaring::RoaringBitmap;

/// Unique identifier for a row referenced by the planner.
pub type RowId = u32;

/// Borrowed iterator that yields [`RowId`] values.
pub type RowIdIter<'a> = Box<dyn Iterator<Item = RowId> + Send + 'a>;

/// Abstract set of row identifiers that supports basic set algebra.
pub trait RowSet: Send + Sync {
    /// Returns the number of rows tracked by the set.
    fn len(&self) -> usize;

    /// Returns true when the set is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true when the set represents the whole universe of rows.
    fn is_full(&self) -> bool;

    /// Returns an iterator over row identifiers.
    fn iter(&self) -> RowIdIter<'_>;

    /// Returns the intersection between this set and `other`.
    fn intersect(&self, other: &Self) -> Self
    where
        Self: Sized;

    /// Returns the union between this set and `other`.
    fn union(&self, other: &Self) -> Self
    where
        Self: Sized;

    /// Returns the relative complement (`self \ other`).
    fn difference(&self, other: &Self) -> Self
    where
        Self: Sized;
}

/// [`RowSet`] implementation backed by a roaring bitmap.
#[derive(Clone, Debug, Default)]
pub struct BitmapRowSet {
    bitmap: RoaringBitmap,
}

impl BitmapRowSet {
    /// Creates an empty bitmap-backed row set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a row identifier into the set.
    pub fn insert(&mut self, row: RowId) {
        self.bitmap.insert(row);
    }

    /// Returns true when the set contains the provided row identifier.
    #[must_use]
    pub fn contains(&self, row: RowId) -> bool {
        self.bitmap.contains(row)
    }
}

impl RowSet for BitmapRowSet {
    fn len(&self) -> usize {
        usize::try_from(self.bitmap.len()).unwrap_or(usize::MAX)
    }

    fn is_full(&self) -> bool {
        self.bitmap.is_full()
    }

    fn iter(&self) -> RowIdIter<'_> {
        Box::new(self.bitmap.iter())
    }

    fn intersect(&self, other: &Self) -> Self {
        let bitmap = &self.bitmap & &other.bitmap;
        Self { bitmap }
    }

    fn union(&self, other: &Self) -> Self {
        let bitmap = &self.bitmap | &other.bitmap;
        Self { bitmap }
    }

    fn difference(&self, other: &Self) -> Self {
        let bitmap = &self.bitmap - &other.bitmap;
        Self { bitmap }
    }
}
