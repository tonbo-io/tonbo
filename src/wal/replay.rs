//! Recovery helpers for scanning WAL segments.

use std::sync::Arc;

use fusio::Read;
#[cfg(test)]
use fusio::{DynFs, path::Path as FusioPath};

use crate::wal::{
    WalConfig, WalError, WalResult,
    frame::{FRAME_HEADER_SIZE, FrameHeader, WalEvent, decode_frame},
    storage::WalStorage,
};

/// Scans WAL segments on disk and yields decoded events.
pub struct Replayer {
    /// Configuration snapshot guiding where segments reside.
    cfg: WalConfig,
    /// Storage facade shared with the WAL writer for filesystem access.
    storage: WalStorage,
}

impl Replayer {
    /// Create a new replayer using the provided configuration.
    pub fn new(cfg: WalConfig) -> Self {
        let storage = WalStorage::new(Arc::clone(&cfg.filesystem), cfg.dir.clone());
        Self { cfg, storage }
    }

    /// Iterate through WAL segments and produce events.
    pub async fn scan(&self) -> WalResult<Vec<WalEvent>> {
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

        let segments = self.storage.list_segments().await?;
        if segments.is_empty() {
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        for segment in segments {
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

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use fusio::{
        Write,
        impls::{disk::TokioFs, mem::fs::InMemoryFs},
    };
    use tempfile::tempdir;

    use super::*;
    use crate::{
        mvcc::Timestamp,
        wal::{
            WalPayload, WalRecoveryMode,
            frame::{INITIAL_FRAME_SEQ, encode_payload},
            storage::WalStorage,
        },
    };

    fn wal_payload(batch: &RecordBatch, tombstones: Vec<bool>, commit_ts: Timestamp) -> WalPayload {
        WalPayload::new(batch.clone(), tombstones, commit_ts).expect("payload")
    }

    #[test]
    fn replayer_returns_logged_events() {
        let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
        let wal_root = FusioPath::parse("wal-test").expect("wal path");
        let storage = WalStorage::new(Arc::clone(&fs), wal_root.clone());

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int32Array::from(vec![1])) as _,
            ],
        )
        .expect("batch");

        let payload = wal_payload(&batch, vec![false], Timestamp::new(42));
        let frames = encode_payload(payload, 7).expect("encode");
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
        cfg.filesystem = fs;
        let replayer = Replayer::new(cfg);
        let events = futures::executor::block_on(replayer.scan()).expect("scan");
        assert_eq!(events.len(), 2);

        match &events[0] {
            WalEvent::DynAppend {
                provisional_id,
                batch: decoded,
                commit_ts_hint,
                tombstones,
            } => {
                assert_eq!(*provisional_id, 7);
                assert_eq!(*commit_ts_hint, Some(Timestamp::new(42)));
                assert_eq!(tombstones, &vec![false]);
                assert_eq!(decoded.num_rows(), 1);
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
    fn replayer_stops_after_truncated_tail() {
        let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
        let wal_root = FusioPath::parse("wal-truncated").expect("wal path");
        let storage = WalStorage::new(Arc::clone(&fs), wal_root.clone());
        let storage_clone = storage.clone();
        let wal_root_for_dir = wal_root.clone();
        futures::executor::block_on(async move {
            storage_clone
                .ensure_dir(&wal_root_for_dir)
                .await
                .expect("ensure dir");
        });

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])) as _,
                Arc::new(Int32Array::from(vec![1])) as _,
            ],
        )
        .expect("batch");

        let payload = wal_payload(&batch, vec![false], Timestamp::new(42));
        let frames = encode_payload(payload, 9).expect("encode");

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
        cfg.filesystem = fs;
        let replayer = Replayer::new(cfg);
        let events = futures::executor::block_on(replayer.scan()).expect("scan succeeds");

        assert_eq!(events.len(), 1, "commit frame should be ignored");
        match &events[0] {
            WalEvent::DynAppend {
                provisional_id,
                batch: decoded,
                commit_ts_hint,
                tombstones,
            } => {
                assert_eq!(*provisional_id, 9);
                assert_eq!(*commit_ts_hint, Some(Timestamp::new(42)));
                assert_eq!(tombstones, &vec![false]);
                assert_eq!(decoded.num_rows(), 1);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replayer_reads_tokiofs_segments() {
        let dir = tempdir().expect("tempdir");
        let wal_dir = dir.path().join("wal");

        let fs: Arc<dyn DynFs> = Arc::new(TokioFs);
        let wal_root = FusioPath::from_filesystem_path(&wal_dir).expect("wal path");
        let storage = WalStorage::new(Arc::clone(&fs), wal_root.clone());

        storage
            .ensure_dir(storage.root())
            .await
            .expect("ensure dir");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["tokio"])) as _,
                Arc::new(Int32Array::from(vec![9])) as _,
            ],
        )
        .expect("batch");

        let payload = wal_payload(&batch, vec![false], Timestamp::new(7));
        let frames = encode_payload(payload, 11).expect("encode");

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
        cfg.filesystem = fs;

        let replayer = Replayer::new(cfg);
        let events = replayer.scan().await.expect("scan");

        assert_eq!(events.len(), 2);
        match events[0] {
            WalEvent::DynAppend { ref batch, .. } => {
                assert_eq!(batch.num_rows(), 1);
            }
            ref other => panic!("unexpected event: {other:?}"),
        }
        match events[1] {
            WalEvent::TxnCommit { .. } => {}
            ref other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn replayer_rejects_unimplemented_recovery_mode() {
        let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
        let wal_root = FusioPath::parse("wal-unimplemented").expect("wal path");

        let mut cfg = WalConfig::default();
        cfg.dir = wal_root;
        cfg.filesystem = fs;
        cfg.recovery = WalRecoveryMode::AbsoluteConsistency;

        let replayer = Replayer::new(cfg);
        let err = futures::executor::block_on(replayer.scan()).expect_err("mode unimplemented");
        assert!(matches!(
            err,
            WalError::Unimplemented("wal recovery mode AbsoluteConsistency is not implemented")
        ));
    }
}
