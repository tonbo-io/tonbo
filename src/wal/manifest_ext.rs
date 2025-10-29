//! Temporary bridge helpers between the WAL subsystem and the manifest.

use crate::{fs::FileId, manifest::WalSegmentRef};

/// Temporary helper that fabricates WAL segment references until real manifest plumbing lands.
pub fn mock_wal_segments(ids: &[FileId]) -> Vec<WalSegmentRef> {
    ids.iter()
        .enumerate()
        .map(|(idx, &id)| WalSegmentRef {
            // TODO: Fetch the actual value from WAL frame
            seq: idx as u64,
            file_id: id,
            first_frame: 0,
            last_frame: 0,
        })
        .collect()
}
