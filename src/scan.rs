//! Shared scan utilities: key ranges and range sets.
//!
//! These types are intentionally small and generic so both mutable and immutable
//! memtables can share the same range definitions. The immutable path scans an
//! index over typed arrays; the mutable path scans a BTreeMap of `(Key, Payload)`.

use std::{cmp::Ordering, ops::Bound};

// No additional crate imports needed here.

/// A key range with owned bounds to allow flexible scanning lifetimes.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyRange<K> {
    /// Start bound (inclusive/exclusive/unbounded).
    pub start: Bound<K>,
    /// End bound (inclusive/exclusive/unbounded).
    pub end: Bound<K>,
}

impl<K> KeyRange<K> {
    /// Create an unbounded range (all keys).
    pub fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    /// Create a new range from explicit bounds.
    pub fn new(start: Bound<K>, end: Bound<K>) -> Self {
        Self { start, end }
    }

    /// Borrowed view of the bounds suitable for `BTreeMap::range`.
    pub(crate) fn as_borrowed_bounds(&self) -> (Bound<&K>, Bound<&K>) {
        let start = match &self.start {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(k) => Bound::Included(k),
            Bound::Excluded(k) => Bound::Excluded(k),
        };
        let end = match &self.end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(k) => Bound::Included(k),
            Bound::Excluded(k) => Bound::Excluded(k),
        };
        (start, end)
    }
}

impl<K: Ord> KeyRange<K> {
    /// Whether this range contains `key`.
    pub fn contains(&self, key: &K) -> bool {
        let start_ok = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(bound) => key >= bound,
            Bound::Excluded(bound) => key > bound,
        };
        if !start_ok {
            return false;
        }
        match &self.end {
            Bound::Unbounded => true,
            Bound::Included(bound) => key <= bound,
            Bound::Excluded(bound) => key < bound,
        }
    }
}

/// A normalized set of disjoint, sorted key ranges.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct RangeSet<K> {
    ranges: Vec<KeyRange<K>>,
}

impl<K: Ord> RangeSet<K> {
    /// Create an empty range set (matches nothing).
    pub fn empty() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Create a set containing a single unbounded range (matches all keys).
    pub fn all() -> Self {
        Self {
            ranges: vec![KeyRange::all()],
        }
    }

    /// Construct from raw ranges and normalize (sort + merge overlaps/adjacents).
    pub fn from_ranges(mut ranges: Vec<KeyRange<K>>) -> Self {
        // Sort by start bound
        ranges.sort_by(|a, b| cmp_lower(&a.start, &b.start));
        let mut out: Vec<KeyRange<K>> = Vec::new();
        for r in ranges.into_iter() {
            if out.is_empty() {
                out.push(r);
                continue;
            }
            let last = out.last_mut().unwrap();
            if overlaps_or_adjacent(&last.end, &r.start) {
                // Merge by extending the end to the max of both ends
                last.end =
                    max_upper_owned(std::mem::replace(&mut last.end, Bound::Unbounded), r.end);
            } else {
                out.push(r);
            }
        }
        Self { ranges: out }
    }

    /// Whether the set has no ranges.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Iterate over contained ranges.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &KeyRange<K>> {
        self.ranges.iter()
    }

    /// Borrow the underlying normalized ranges as a slice.
    pub fn as_slice(&self) -> &[KeyRange<K>] {
        &self.ranges
    }

    /// Whether the set contains `key`.
    pub fn contains(&self, key: &K) -> bool {
        self.ranges.iter().any(|range| range.contains(key))
    }

    /// Union with another set, returning a normalized result.
    pub fn union(self, other: RangeSet<K>) -> RangeSet<K> {
        if self.ranges.is_empty() {
            return other;
        }
        if other.ranges.is_empty() {
            return self;
        }
        let mut all = self.ranges;
        all.extend(other.ranges);
        RangeSet::from_ranges(all)
    }

    /// Intersection with another set, returning a normalized result.
    pub fn intersect(self, other: RangeSet<K>) -> RangeSet<K>
    where
        K: Clone,
    {
        if self.ranges.is_empty() || other.ranges.is_empty() {
            return RangeSet::empty();
        }
        let mut res: Vec<KeyRange<K>> = Vec::new();
        for a in &self.ranges {
            for b in &other.ranges {
                let start = max_lower_owned(a.start.clone(), b.start.clone());
                let end = min_upper_owned(a.end.clone(), b.end.clone());
                if !is_empty_range(&start, &end) {
                    res.push(KeyRange::new(start, end));
                }
            }
        }
        RangeSet::from_ranges(res)
    }

    /// Complement of this set over the full key domain `(-∞, +∞)`.
    pub fn complement(self) -> RangeSet<K>
    where
        K: Clone,
    {
        if self.ranges.is_empty() {
            return RangeSet::all();
        }
        // If this covers the entire domain, complement is empty.
        if self.ranges.len() == 1 {
            let r = &self.ranges[0];
            if matches!(r.start, Bound::Unbounded) && matches!(r.end, Bound::Unbounded) {
                return RangeSet::empty();
            }
        }

        let mut gaps: Vec<KeyRange<K>> = Vec::new();
        let mut prev_end: Option<&Bound<K>> = None;
        for (i, r) in self.ranges.iter().enumerate() {
            if i == 0 {
                // Initial gap to the first range start
                match &r.start {
                    Bound::Unbounded => {}
                    Bound::Included(v) => {
                        gaps.push(KeyRange::new(Bound::Unbounded, Bound::Excluded(v.clone())));
                    }
                    Bound::Excluded(v) => {
                        gaps.push(KeyRange::new(Bound::Unbounded, Bound::Included(v.clone())));
                    }
                }
            } else if let Some(pe) = prev_end {
                // Gap between previous end and this start
                let start = match pe {
                    Bound::Included(v) => Bound::Excluded(v.clone()),
                    Bound::Excluded(v) => Bound::Included(v.clone()),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let end = match &r.start {
                    Bound::Unbounded => Bound::Unbounded,
                    Bound::Included(v) => Bound::Excluded(v.clone()),
                    Bound::Excluded(v) => Bound::Included(v.clone()),
                };
                if !is_empty_range(&start, &end) {
                    gaps.push(KeyRange::new(start, end));
                }
            }
            prev_end = Some(&r.end);
        }
        // Final gap from last end to +inf
        if let Some(pe) = prev_end {
            match pe {
                Bound::Unbounded => {}
                Bound::Included(v) => {
                    gaps.push(KeyRange::new(Bound::Excluded(v.clone()), Bound::Unbounded));
                }
                Bound::Excluded(v) => {
                    gaps.push(KeyRange::new(Bound::Included(v.clone()), Bound::Unbounded));
                }
            }
        }
        RangeSet::from_ranges(gaps)
    }
}

// Lower-bound comparator: Included(x) < Excluded(x) < Included(y) if x<y
fn cmp_lower<K: Ord>(a: &Bound<K>, b: &Bound<K>) -> Ordering {
    use Bound as B;
    match (a, b) {
        (B::Unbounded, B::Unbounded) => Ordering::Equal,
        (B::Unbounded, _) => Ordering::Less,
        (_, B::Unbounded) => Ordering::Greater,
        (B::Included(x), B::Included(y)) => x.cmp(y),
        (B::Included(x), B::Excluded(y)) => match x.cmp(y) {
            Ordering::Equal => Ordering::Less,
            other => other,
        },
        (B::Excluded(x), B::Included(y)) => match x.cmp(y) {
            Ordering::Equal => Ordering::Greater,
            other => other,
        },
        (B::Excluded(x), B::Excluded(y)) => x.cmp(y),
    }
}

// Whether two ranges touch at the boundary (end meets start) or overlap.
fn overlaps_or_adjacent<K: Ord>(end: &Bound<K>, start: &Bound<K>) -> bool {
    use Bound as B;
    match (end, start) {
        (B::Unbounded, _) | (_, B::Unbounded) => true,
        (B::Included(x), B::Included(y)) => x >= y,
        (B::Included(x), B::Excluded(y)) => x >= y,
        (B::Excluded(x), B::Included(y)) => x >= y,
        (B::Excluded(x), B::Excluded(y)) => x > y,
    }
}

// Helpers to compare/choose bounds for union/intersection
fn ord_lower<K: Ord>(a: &K, inc_a: bool, b: &K, inc_b: bool) -> Ordering {
    match a.cmp(b) {
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
        Ordering::Equal => match (inc_a, inc_b) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => Ordering::Equal,
        },
    }
}

fn ord_upper<K: Ord>(a: &K, inc_a: bool, b: &K, inc_b: bool) -> Ordering {
    match a.cmp(b) {
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
        Ordering::Equal => match (inc_a, inc_b) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => Ordering::Equal,
        },
    }
}

fn min_upper_owned<K: Ord>(a: Bound<K>, b: Bound<K>) -> Bound<K> {
    use Bound as B;
    match (&a, &b) {
        (B::Unbounded, _) => b,
        (_, B::Unbounded) => a,
        (B::Included(x), B::Included(y)) => {
            if ord_upper(x, true, y, true) == Ordering::Less {
                a
            } else {
                b
            }
        }
        (B::Included(x), B::Excluded(y)) => {
            if ord_upper(x, true, y, false) == Ordering::Less {
                a
            } else {
                b
            }
        }
        (B::Excluded(x), B::Included(y)) => {
            if ord_upper(x, false, y, true) == Ordering::Less {
                a
            } else {
                b
            }
        }
        (B::Excluded(x), B::Excluded(y)) => {
            if ord_upper(x, false, y, false) == Ordering::Less {
                a
            } else {
                b
            }
        }
    }
}

fn max_lower_owned<K: Ord>(a: Bound<K>, b: Bound<K>) -> Bound<K> {
    use Bound as B;
    match (&a, &b) {
        (B::Unbounded, _) => b,
        (_, B::Unbounded) => a,
        (B::Included(x), B::Included(y)) => {
            if ord_lower(x, true, y, true) == Ordering::Greater {
                a
            } else {
                b
            }
        }
        (B::Included(x), B::Excluded(y)) => {
            if ord_lower(x, true, y, false) == Ordering::Greater {
                a
            } else {
                b
            }
        }
        (B::Excluded(x), B::Included(y)) => {
            if ord_lower(x, false, y, true) == Ordering::Greater {
                a
            } else {
                b
            }
        }
        (B::Excluded(x), B::Excluded(y)) => {
            if ord_lower(x, false, y, false) == Ordering::Greater {
                a
            } else {
                b
            }
        }
    }
}

fn max_upper_owned<K: Ord>(a: Bound<K>, b: Bound<K>) -> Bound<K> {
    use Bound as B;
    match (&a, &b) {
        (B::Unbounded, _) | (_, B::Unbounded) => B::Unbounded,
        (B::Included(x), B::Included(y)) => {
            if ord_upper(x, true, y, true) == Ordering::Greater {
                a
            } else {
                b
            }
        }
        (B::Included(x), B::Excluded(y)) => {
            if ord_upper(x, true, y, false) == Ordering::Greater {
                a
            } else {
                b
            }
        }
        (B::Excluded(x), B::Included(y)) => {
            if ord_upper(x, false, y, true) == Ordering::Greater {
                a
            } else {
                b
            }
        }
        (B::Excluded(x), B::Excluded(y)) => {
            if ord_upper(x, false, y, false) == Ordering::Greater {
                a
            } else {
                b
            }
        }
    }
}

//

fn is_empty_range<K: Ord>(start: &Bound<K>, end: &Bound<K>) -> bool {
    use Bound as B;
    match (start, end) {
        (B::Unbounded, _) | (_, B::Unbounded) => false,
        (B::Included(a), B::Included(b)) => a > b,
        (B::Included(a), B::Excluded(b)) => a >= b,
        (B::Excluded(a), B::Included(b)) => a >= b,
        (B::Excluded(a), B::Excluded(b)) => a >= b,
    }
}
