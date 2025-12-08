//! Read-only immutable memtables.
//!
//! Currently only the dynamic runtime-schema layout is wired through.

pub(crate) mod memtable;

/// Immutable segment emitted by sealing the dynamic mutable memtable.
pub(crate) type ImmutableSegment = memtable::ImmutableMemTable;

/// Lightweight pruning helper; currently returns all segment indexes.
pub(crate) fn prune_segments(segments: &[&ImmutableSegment]) -> Vec<usize> {
    (0..segments.len()).collect()
}
