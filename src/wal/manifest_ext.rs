//! Helpers that bridge WAL metadata into manifest structures.

use std::sync::Arc;

use ulid::Ulid;

use crate::{
    fs::FileId,
    manifest::WalSegmentRef,
    wal::{
        WalConfig, WalError,
        storage::{SegmentDescriptor, SegmentFrameBounds, WalStorage},
    },
};

/// Collect WAL segment references using the configuration supplied to the writer.
pub(crate) async fn collect_wal_segment_refs(
    cfg: &WalConfig,
) -> Result<Vec<WalSegmentRef>, WalError> {
    let storage = WalStorage::new(Arc::clone(&cfg.filesystem), cfg.dir.clone());
    let Some(tail) = storage.tail_metadata().await? else {
        return Ok(Vec::new());
    };

    let mut refs = Vec::with_capacity(tail.completed.len() + 1);
    for descriptor in &tail.completed {
        if descriptor.bytes == 0 {
            continue;
        }
        let bounds = storage
            .segment_frame_bounds(&descriptor.path)
            .await?
            .ok_or_else(|| {
                WalError::Corrupt("wal segment contained no frames despite non-zero length")
            })?;
        refs.push(wal_segment_ref_from_descriptor(descriptor, bounds));
    }

    if tail.active.bytes > 0 {
        let bounds = storage
            .segment_frame_bounds(&tail.active.path)
            .await?
            .ok_or_else(|| {
                WalError::Corrupt("active wal segment contained no frames despite non-zero length")
            })?;
        refs.push(wal_segment_ref_from_descriptor(&tail.active, bounds));
    }

    refs.sort_by_key(|segment| segment.seq());
    refs.dedup_by_key(|segment| segment.seq());

    Ok(refs)
}

fn wal_segment_ref_from_descriptor(
    descriptor: &SegmentDescriptor,
    bounds: SegmentFrameBounds,
) -> WalSegmentRef {
    let file_id = deterministic_file_id(descriptor.seq);
    WalSegmentRef::new(descriptor.seq, file_id, bounds.first_seq, bounds.last_seq)
}

fn deterministic_file_id(seq: u64) -> FileId {
    let mut bytes = [0u8; 16];
    bytes[8..16].copy_from_slice(&seq.to_be_bytes());
    Ulid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fusio::{DynFs, Write, impls::mem::fs::InMemoryFs, path::Path};
    use futures::executor::block_on;

    use super::*;
    use crate::wal::{
        WalCommand,
        frame::{self, encode_command},
    };

    #[test]
    fn collect_wal_segment_refs_reports_frame_bounds() {
        block_on(async {
            let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
            let dir = Path::parse("wal-test").expect("valid wal dir");
            let storage = WalStorage::new(Arc::clone(&fs), dir.clone());

            let mut next_seq = frame::INITIAL_FRAME_SEQ;

            let mut first_segment = storage.open_segment(0).await.expect("open segment 0");
            append_command(
                &mut first_segment,
                &mut next_seq,
                WalCommand::TxnBegin { provisional_id: 42 },
            )
            .await;
            append_command(
                &mut first_segment,
                &mut next_seq,
                WalCommand::TxnAbort { provisional_id: 42 },
            )
            .await;
            first_segment
                .file_mut()
                .flush()
                .await
                .expect("flush first segment");
            drop(first_segment);

            let mut second_segment = storage.open_segment(1).await.expect("open segment 1");
            append_command(
                &mut second_segment,
                &mut next_seq,
                WalCommand::TxnBegin { provisional_id: 84 },
            )
            .await;
            append_command(
                &mut second_segment,
                &mut next_seq,
                WalCommand::TxnAbort { provisional_id: 84 },
            )
            .await;
            second_segment
                .file_mut()
                .flush()
                .await
                .expect("flush second segment");
            drop(second_segment);

            let cfg = WalConfig {
                dir,
                filesystem: Arc::clone(&fs),
                ..WalConfig::default()
            };

            let refs = collect_wal_segment_refs(&cfg)
                .await
                .expect("collect wal refs");
            assert_eq!(refs.len(), 2);

            assert_eq!(refs[0].seq(), 0);
            assert_eq!(refs[0].first_frame(), frame::INITIAL_FRAME_SEQ);
            assert_eq!(refs[0].last_frame(), frame::INITIAL_FRAME_SEQ + 1);

            assert_eq!(refs[1].seq(), 1);
            assert_eq!(refs[1].first_frame(), frame::INITIAL_FRAME_SEQ + 2);
            assert_eq!(refs[1].last_frame(), frame::INITIAL_FRAME_SEQ + 3);
        });
    }

    async fn append_command(
        segment: &mut crate::wal::storage::WalSegment,
        next_seq: &mut u64,
        command: WalCommand,
    ) {
        for frame in encode_command(command).expect("encode command") {
            let bytes = frame.into_bytes(*next_seq);
            *next_seq = next_seq.saturating_add(1);
            let (write_res, _buf) = segment.file_mut().write_all(bytes).await;
            write_res.expect("write frame");
        }
    }
}
