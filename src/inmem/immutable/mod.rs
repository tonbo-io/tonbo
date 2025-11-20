//! Read-only immutable memtables.
//!
//! Currently only the dynamic runtime-schema layout is wired through. The
//! `Mode` trait keeps room to add typed immutable storage again later.

pub(crate) mod memtable;

/// Convenience alias for the immutable segment associated with a `Mode`.
pub(crate) type Immutable<M> = memtable::ImmutableMemTable<<M as crate::mode::Mode>::ImmLayout>;

use crate::{
    key::{self, KeyOwned, RangeSet},
    mode::Mode,
};

/// Lightweight pruning helper that selects segment indexes overlapping `range_set`.
pub(crate) fn prune_segments<M: Mode>(
    segments: &[Immutable<M>],
    range_set: &RangeSet<KeyOwned>,
) -> Vec<usize> {
    segments
        .iter()
        .enumerate()
        .filter_map(|(idx, segment)| {
            let min = segment.min_key()?;
            let max = segment.max_key()?;
            if key::range_set_overlaps_bounds(range_set, &min, &max) {
                Some(idx)
            } else {
                None
            }
        })
        .collect()
}
