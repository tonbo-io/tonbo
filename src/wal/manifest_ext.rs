//! Temporary bridge helpers between the WAL subsystem and the manifest.

use crate::{fs::FileId, manifest::WalSegmentRef};

/// Temporary helper that fabricates WAL segment references until real manifest plumbing lands.
pub(crate) fn mock_wal_segments(ids: &[FileId]) -> Vec<WalSegmentRef> {
    ids.iter()
        .enumerate()
        .map(|(idx, &id)| WalSegmentRef::new(idx as u64, id, 0, 0))
        .collect()
}
