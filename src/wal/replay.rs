//! Recovery helpers for scanning WAL segments.

use std::sync::Arc;

use fusio::Read;
#[cfg(test)]
use fusio::{DynFs, fs::FsCas, path::Path as FusioPath};

use crate::{
    manifest::WalSegmentRef,
    wal::{
        WalConfig, WalError, WalResult,
        frame::{FRAME_HEADER_SIZE, FrameHeader, WalEvent, decode_frame},
        storage::WalStorage,
    },
};

/// Scans WAL segments on disk and yields decoded events.
pub struct Replayer {
    /// Configuration snapshot guiding where segments reside.
    cfg: WalConfig,
    /// Storage facade shared with the WAL writer for segment access.
    storage: WalStorage,
}

impl Replayer {
    /// Create a new replayer using the provided configuration.
    pub fn new(cfg: WalConfig) -> Self {
        let storage = WalStorage::new(Arc::clone(&cfg.segment_backend), cfg.dir.clone());
        Self { cfg, storage }
    }

    /// Iterate through WAL segments and produce events.
    pub async fn scan(&self) -> WalResult<Vec<WalEvent>> {
        self.scan_with_floor(None).await
    }

    /// Iterate through WAL segments while honoring the provided manifest floor.
    pub(crate) async fn scan_with_floor(
        &self,
        floor: Option<&WalSegmentRef>,
    ) -> WalResult<Vec<WalEvent>> {
        use crate::wal::WalRecoveryMode;

        match self.cfg.recovery {
            WalRecoveryMode::PointInTime | WalRecoveryMode::TolerateCorruptedTail => {
                // Both variants currently share the same implementation: stop at the first
                // truncated or unreadable frame. `TolerateCorruptedTail` exists so
                // we can later introduce tail-specific heuristics without breaking
                // configuration semantics.
            }
            WalRecoveryMode::AbsoluteConsistency => {
                return Err(WalError::Unimplemented(
                    "wal recovery mode AbsoluteConsistency is not implemented",
                ));
            }
            WalRecoveryMode::SkipCorrupted => {
                return Err(WalError::Unimplemented(
                    "wal recovery mode SkipCorrupted is not implemented",
                ));
            }
        }

        let state_hint = self
            .storage
            .load_state_handle(self.cfg.state_store.as_ref())
            .await?
            .and_then(|handle| handle.state().last_segment_seq);

        let segments = self.storage.list_segments_with_hint(state_hint).await?;
        if segments.is_empty() {
            return Ok(Vec::new());
        }

        let floor_seq = floor.map(|f| f.seq());
        let floor_first_frame = floor.map(|f| f.first_frame());

        let mut events = Vec::new();
        for segment in segments {
            if let Some(seq) = floor_seq
                && segment.seq < seq
            {
                continue;
            }

            let segment_floor_first_frame = match floor_seq {
                Some(seq) if segment.seq == seq => floor_first_frame,
                _ => None,
            };

            let path = segment.path;
            let path_display = path.to_string();
            let mut file = self
                .storage
                .fs()
                .open_options(&path, WalStorage::read_options())
                .await
                .map_err(|err| {
                    WalError::Storage(format!(
                        "failed to open wal segment {}: {}",
                        path_display, err
                    ))
                })?;

            let (read_res, data) = file.read_to_end_at(Vec::new(), 0).await;
            read_res.map_err(|err| {
                WalError::Storage(format!(
                    "failed to read wal segment {}: {}",
                    path_display, err
                ))
            })?;

            let mut offset: usize = 0;
            while offset < data.len() {
                let slice = &data[offset..];
                let header = match FrameHeader::decode_from(slice) {
                    Ok((header, _)) => header,
                    Err(WalError::Corrupt(reason))
                        if reason == "frame header truncated"
                            || reason == "frame payload truncated" =>
                    {
                        // Treat a truncated tail (common for crash-at-end scenarios) as EOF so
                        // recovery returns the events observed before the partial frame. Any
                        // other corruption surfaces as an error to avoid silently skipping valid
                        // transactions further in the log.
                        return Ok(events);
                    }
                    Err(err) => return Err(err),
                };
                let payload_start = offset + FRAME_HEADER_SIZE;
                let payload_end = payload_start + header.len as usize;
                if payload_end > data.len() {
                    return Ok(events);
                }
                let payload = &data[payload_start..payload_end];
                if let Some(floor_frame) = segment_floor_first_frame
                    && header.seq < floor_frame
                {
                    offset = payload_end;
                    continue;
                }

                match decode_frame(header.frame_type, payload) {
                    Ok(event) => events.push(event),
                    Err(err) => return Err(err),
                }
                offset = payload_end;
            }
        }

        Ok(events)
    }

    /// Access the configuration.
    pub fn config(&self) -> &WalConfig {
        &self.cfg
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use fusio::{
        Write,
        impls::{disk::TokioFs, mem::fs::InMemoryFs},
    };
    use tempfile::tempdir;

    use super::*;
    use crate::{
        manifest::WalSegmentRef,
        mvcc::Timestamp,
        schema::SchemaBuilder,
        wal::{
            WalRecoveryMode,
            frame::{INITIAL_FRAME_SEQ, encode_autocommit_frames},
            state::FsWalStateStore,
            storage::WalStorage,
            wal_segment_file_id,
        },
    };

    #[test]
    fn replayer_returns_logged_events() {
        let backend = Arc::new(InMemoryFs::new());
        let fs_dyn: Arc<dyn DynFs> = backend.clone();
        let fs_cas: Arc<dyn FsCas> = backend.clone();
        let wal_root = FusioPath::parse("wal-test").expect("wal path");
        let storage = WalStorage::new(Arc::clone(&fs_dyn), wal_root.clone());

        let schema = default_test_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int32Array::from(vec![1])) as _,
            ],
        )
        .expect("batch");

        let frames = encode_autocommit_frames(batch.clone(), vec![false], 7, Timestamp::new(42))
            .expect("encode");
        let mut seq = INITIAL_FRAME_SEQ;
        let mut bytes = Vec::new();
        for frame in frames {
            bytes.extend_from_slice(&frame.into_bytes(seq));
            seq += 1;
        }
        let storage_clone = storage.clone();
        let wal_root_for_write = wal_root.clone();
        let bytes_for_write = bytes;
        futures::executor::block_on(async move {
            storage_clone
                .ensure_dir(&wal_root_for_write)
                .await
                .expect("ensure dir");
            let mut segment = storage_clone.open_segment(1).await.expect("open segment");
            let (write_res, _buf) = segment.file_mut().write_all(bytes_for_write).await;
            write_res.expect("write wal");
            segment.file_mut().flush().await.expect("flush");
        });

        let mut cfg = WalConfig::default();
        cfg.dir = wal_root;
        cfg.segment_backend = fs_dyn;
        cfg.state_store = Some(Arc::new(FsWalStateStore::new(fs_cas)));
        let replayer = Replayer::new(cfg);
        let events = futures::executor::block_on(replayer.scan()).expect("scan");
        assert_eq!(events.len(), 2);

        match &events[0] {
            WalEvent::DynAppend {
                provisional_id,
                payload,
            } => {
                assert_eq!(*provisional_id, 7);
                assert_eq!(payload.commit_ts_hint, Some(Timestamp::new(42)));
                let commit_array = payload
                    .commit_ts_column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("u64 column");
                assert_eq!(commit_array.len(), 1);
                assert_eq!(commit_array.value(0), 42);
                let tombstone_array = payload
                    .tombstones
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("boolean column");
                assert_eq!(tombstone_array.len(), 1);
                assert!(!tombstone_array.value(0));
                assert_eq!(payload.batch.num_rows(), 1);
            }
            other => panic!("unexpected event: {other:?}"),
        }

        match events[1] {
            WalEvent::TxnCommit {
                provisional_id,
                commit_ts,
            } => {
                assert_eq!(provisional_id, 7);
                assert_eq!(commit_ts, Timestamp::new(42));
            }
            ref other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn replayer_skips_events_below_floor() {
        futures::executor::block_on(async {
            let backend = Arc::new(InMemoryFs::new());
            let fs_dyn: Arc<dyn DynFs> = backend.clone();
            let wal_root = FusioPath::parse("wal-floor").expect("wal path");
            let storage = WalStorage::new(Arc::clone(&fs_dyn), wal_root.clone());

            let mut next_seq = INITIAL_FRAME_SEQ;
            let batch_a = sample_batch_with_label("a", 1);
            write_autocommit_segment(&storage, 0, batch_a, 10, Timestamp::new(100), &mut next_seq)
                .await;

            let batch_b = sample_batch_with_label("b", 2);
            let (floor_first_seq, floor_last_seq) = write_autocommit_segment(
                &storage,
                1,
                batch_b,
                11,
                Timestamp::new(200),
                &mut next_seq,
            )
            .await;

            let mut cfg = WalConfig::default();
            cfg.dir = wal_root;
            cfg.segment_backend = fs_dyn;
            cfg.state_store = None;

            let replayer = Replayer::new(cfg);
            let floor =
                WalSegmentRef::new(1, wal_segment_file_id(1), floor_first_seq, floor_last_seq);

            let events = replayer.scan_with_floor(Some(&floor)).await.expect("scan");
            assert_eq!(events.len(), 2, "only second segment survives the floor");

            match events[0] {
                WalEvent::DynAppend { provisional_id, .. } => {
                    assert_eq!(provisional_id, 11);
                }
                ref other => panic!("unexpected first event: {other:?}"),
            }

            match events[1] {
                WalEvent::TxnCommit { provisional_id, .. } => {
                    assert_eq!(provisional_id, 11);
                }
                ref other => panic!("unexpected second event: {other:?}"),
            }
        });
    }

    #[test]
    fn replayer_stops_after_truncated_tail() {
        let backend = Arc::new(InMemoryFs::new());
        let fs_dyn: Arc<dyn DynFs> = backend.clone();
        let fs_cas: Arc<dyn FsCas> = backend.clone();
        let wal_root = FusioPath::parse("wal-truncated").expect("wal path");
        let storage = WalStorage::new(Arc::clone(&fs_dyn), wal_root.clone());
        let storage_clone = storage.clone();
        let wal_root_for_dir = wal_root.clone();
        futures::executor::block_on(async move {
            storage_clone
                .ensure_dir(&wal_root_for_dir)
                .await
                .expect("ensure dir");
        });

        let schema = default_test_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int32Array::from(vec![1])) as _,
            ],
        )
        .expect("batch");

        let frames = encode_autocommit_frames(batch.clone(), vec![false], 9, Timestamp::new(42))
            .expect("encode");

        let mut seq = INITIAL_FRAME_SEQ;
        let append_bytes = frames[0].clone().into_bytes(seq);
        seq += 1;
        let mut commit_bytes = frames[1].clone().into_bytes(seq);
        commit_bytes.truncate(commit_bytes.len() - 3);

        let storage_clone = storage.clone();
        futures::executor::block_on(async move {
            let mut segment = storage_clone.open_segment(5).await.expect("open segment");
            let (res, _buf) = segment.file_mut().write_all(append_bytes).await;
            res.expect("write append");
            let (res_commit, _buf) = segment.file_mut().write_all(commit_bytes).await;
            res_commit.expect("write truncated commit");
            segment.file_mut().flush().await.expect("flush");
        });

        let mut cfg = WalConfig::default();
        cfg.dir = wal_root;
        cfg.segment_backend = fs_dyn;
        cfg.state_store = Some(Arc::new(FsWalStateStore::new(fs_cas)));
        let replayer = Replayer::new(cfg);
        let events = futures::executor::block_on(replayer.scan()).expect("scan succeeds");

        assert_eq!(events.len(), 1, "commit frame should be ignored");
        match &events[0] {
            WalEvent::DynAppend {
                provisional_id,
                payload,
            } => {
                assert_eq!(*provisional_id, 9);
                assert_eq!(payload.commit_ts_hint, Some(Timestamp::new(42)));
                let commit_array = payload
                    .commit_ts_column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("u64 column");
                assert_eq!(commit_array.len(), 1);
                assert_eq!(commit_array.value(0), 42);
                let tombstone_array = payload
                    .tombstones
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("boolean column");
                assert_eq!(tombstone_array.len(), 1);
                assert!(!tombstone_array.value(0));
                assert_eq!(payload.batch.num_rows(), 1);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replayer_reads_tokiofs_segments() {
        let dir = tempdir().expect("tempdir");
        let wal_dir = dir.path().join("wal");

        let backend = Arc::new(TokioFs);
        let fs_dyn: Arc<dyn DynFs> = backend.clone();
        let wal_root = FusioPath::from_filesystem_path(&wal_dir).expect("wal path");
        let storage = WalStorage::new(Arc::clone(&fs_dyn), wal_root.clone());

        storage
            .ensure_dir(storage.root())
            .await
            .expect("ensure dir");

        let schema = default_test_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["tokio"])) as _,
                Arc::new(Int32Array::from(vec![9])) as _,
            ],
        )
        .expect("batch");

        let frames = encode_autocommit_frames(batch.clone(), vec![false], 11, Timestamp::new(7))
            .expect("encode");

        let mut seq = INITIAL_FRAME_SEQ;
        let mut bytes = Vec::new();
        for frame in frames {
            bytes.extend_from_slice(&frame.into_bytes(seq));
            seq += 1;
        }

        let mut segment = storage.open_segment(42).await.expect("segment");
        let (write_res, _buf) = segment.file_mut().write_all(bytes).await;
        write_res.expect("write");
        segment.file_mut().flush().await.expect("flush");

        let mut cfg = WalConfig::default();
        cfg.dir = wal_root;
        cfg.segment_backend = fs_dyn;
        cfg.state_store = None;

        let replayer = Replayer::new(cfg);
        let events = replayer.scan().await.expect("scan");

        assert_eq!(events.len(), 2);
        match events[0] {
            WalEvent::DynAppend { ref payload, .. } => {
                assert_eq!(payload.batch.num_rows(), 1);
            }
            ref other => panic!("unexpected event: {other:?}"),
        }
        match events[1] {
            WalEvent::TxnCommit { .. } => {}
            ref other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replayer_skips_events_below_floor_on_disk() {
        let dir = tempdir().expect("tempdir");
        let wal_dir = dir.path().join("wal-floor-disk");

        let backend = Arc::new(TokioFs);
        let fs_dyn: Arc<dyn DynFs> = backend.clone();
        let wal_root = FusioPath::from_filesystem_path(&wal_dir).expect("wal path");
        let storage = WalStorage::new(Arc::clone(&fs_dyn), wal_root.clone());

        storage
            .ensure_dir(storage.root())
            .await
            .expect("ensure dir");

        let mut next_seq = INITIAL_FRAME_SEQ;
        let batch_a = sample_batch_with_label("disk-a", 7);
        write_autocommit_segment(
            &storage,
            10,
            batch_a,
            21,
            Timestamp::new(300),
            &mut next_seq,
        )
        .await;

        let batch_b = sample_batch_with_label("disk-b", 8);
        let (floor_first_seq, floor_last_seq) = write_autocommit_segment(
            &storage,
            11,
            batch_b,
            22,
            Timestamp::new(400),
            &mut next_seq,
        )
        .await;

        let mut cfg = WalConfig::default();
        cfg.dir = wal_root;
        cfg.segment_backend = fs_dyn;
        cfg.state_store = None;

        let replayer = Replayer::new(cfg);
        let floor =
            WalSegmentRef::new(11, wal_segment_file_id(11), floor_first_seq, floor_last_seq);

        let events = replayer
            .scan_with_floor(Some(&floor))
            .await
            .expect("scan succeeds");

        assert_eq!(events.len(), 2);
        match events[0] {
            WalEvent::DynAppend { provisional_id, .. } => assert_eq!(provisional_id, 22),
            ref other => panic!("unexpected first event: {other:?}"),
        }
        match events[1] {
            WalEvent::TxnCommit { provisional_id, .. } => assert_eq!(provisional_id, 22),
            ref other => panic!("unexpected second event: {other:?}"),
        }
    }

    #[test]
    fn replayer_rejects_unimplemented_recovery_mode() {
        let backend = Arc::new(InMemoryFs::new());
        let fs_dyn: Arc<dyn DynFs> = backend.clone();
        let fs_cas: Arc<dyn FsCas> = backend.clone();
        let wal_root = FusioPath::parse("wal-unimplemented").expect("wal path");

        let mut cfg = WalConfig::default();
        cfg.dir = wal_root;
        cfg.segment_backend = fs_dyn;
        cfg.state_store = Some(Arc::new(FsWalStateStore::new(fs_cas)));
        cfg.recovery = WalRecoveryMode::AbsoluteConsistency;

        let replayer = Replayer::new(cfg);
        let err = futures::executor::block_on(replayer.scan()).expect_err("mode unimplemented");
        assert!(matches!(
            err,
            WalError::Unimplemented("wal recovery mode AbsoluteConsistency is not implemented")
        ));
    }

    fn sample_batch_with_label(label: &str, value: i32) -> RecordBatch {
        let schema = default_test_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![label])) as _,
                Arc::new(Int32Array::from(vec![value])) as _,
            ],
        )
        .expect("batch")
    }

    async fn write_autocommit_segment(
        storage: &WalStorage,
        segment_seq: u64,
        batch: RecordBatch,
        provisional_id: u64,
        commit_ts: Timestamp,
        next_seq: &mut u64,
    ) -> (u64, u64) {
        let tombstones = vec![false; batch.num_rows()];
        let frames =
            encode_autocommit_frames(batch, tombstones, provisional_id, commit_ts).expect("encode");
        let first_seq = *next_seq;
        let mut segment = storage
            .open_segment(segment_seq)
            .await
            .expect("open segment");
        for frame in frames {
            let bytes = frame.into_bytes(*next_seq);
            *next_seq = next_seq.saturating_add(1);
            let (res, _buf) = segment.file_mut().write_all(bytes).await;
            res.expect("write frame");
        }
        segment.file_mut().flush().await.expect("flush segment");
        let last_seq = next_seq.saturating_sub(1);
        (first_seq, last_seq)
    }

    fn default_test_schema() -> SchemaRef {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        SchemaBuilder::from_schema(Arc::clone(&schema))
            .primary_key("id")
            .build()
            .expect("schema builder should succeed")
            .schema
    }
}
