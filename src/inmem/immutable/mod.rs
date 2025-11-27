//! Read-only immutable memtables.
//!
//! Currently only the dynamic runtime-schema layout is wired through. The
//! `Mode` trait keeps room to add typed immutable storage again later.

pub(crate) mod memtable;

/// Convenience alias for the immutable segment associated with a `Mode`.
pub(crate) type Immutable<M> = memtable::ImmutableMemTable<<M as crate::mode::Mode>::ImmLayout>;

use crate::mode::Mode;

/// Lightweight pruning helper; currently returns all segment indexes.
pub(crate) fn prune_segments<M: Mode>(segments: &[Immutable<M>]) -> Vec<usize> {
    (0..segments.len()).collect()
}
