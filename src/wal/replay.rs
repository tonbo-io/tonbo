//! Recovery helpers for scanning WAL segments.

use std::{io, sync::Arc};

use fusio::{DynFs, Read, error::Error as FusioError, path::Path as FusioPath};
use futures::{StreamExt, executor::block_on};

use crate::wal::{
    WalConfig, WalError, WalResult,
    frame::{FRAME_HEADER_SIZE, FrameHeader, WalEvent, decode_frame},
    storage::WalStorage,
};

/// Scans WAL segments on disk and yields decoded events.
pub struct Replayer {
    /// Configuration snapshot guiding where segments reside.
    cfg: WalConfig,
    /// Filesystem interface backing the configured WAL directory.
    fs: Arc<dyn DynFs>,
}

impl Replayer {
    /// Create a new replayer using the provided configuration.
    pub fn new(cfg: WalConfig) -> Self {
        let fs = Arc::clone(&cfg.filesystem);
        Self { cfg, fs }
    }

    /// Iterate through WAL segments and produce events.
    ///
    /// The public API stays synchronous for callers such as `DB::recover_with_wal`,
    /// but we delegate to the async helper internally because `DynFs::list`,
    /// `open_options`, and `read_to_end_at` are async-only fusio primitives.
    pub fn scan(&self) -> WalResult<Vec<WalEvent>> {
        block_on(self.scan_async())
    }

    /// Access the configuration.
    pub fn config(&self) -> &WalConfig {
        &self.cfg
    }

    /// Async implementation detail used by [`scan`].
    ///
    /// Fusio exposes filesystem accessors (`list`, `open_options`, `read_to_end_at`)
    /// exclusively as async operations, so we stage their usage inside this future
    /// and have the public entry point (`scan`) synchronously `block_on` it.
    async fn scan_async(&self) -> WalResult<Vec<WalEvent>> {
        let mut entries = Vec::<(u64, FusioPath)>::new();
        let mut stream = match self.fs.list(&self.cfg.dir).await {
            Ok(stream) => stream,
            Err(FusioError::Io(err)) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(err) => {
                return Err(WalError::Storage(format!(
                    "failed to list wal dir {}: {}",
                    self.cfg.dir.as_ref(),
                    err
                )));
            }
        };

        while let Some(meta_result) = stream.next().await {
            let meta = meta_result.map_err(|err| {
                WalError::Storage(format!(
                    "failed to read wal metadata under {}: {}",
                    self.cfg.dir.as_ref(),
                    err
                ))
            })?;
            if let Some(seq) = segment_sequence(&meta.path) {
                entries.push((seq, meta.path));
            }
        }

        entries.sort_by_key(|(seq, _)| *seq);

        let mut events = Vec::new();
        for (_, path) in entries {
            let path_display = path.to_string();
            let mut file = self
                .fs
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
                        // recovery returns the events observed before the
                        // partial frame. Any other corruption surfaces as
                        // an error to avoid silently skipping valid transactions further in
                        // the log.
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
}

fn segment_sequence(path: &FusioPath) -> Option<u64> {
    let filename = path.filename()?;
    let trimmed = filename.strip_prefix("wal-")?.strip_suffix(".tonwal")?;
    if trimmed.len() != 20 {
        return None;
    }
    trimmed.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use fusio::{Write, impls::mem::fs::InMemoryFs};

    use super::*;
    use crate::{
        mvcc::Timestamp,
        wal::{
            WalPayload,
            frame::{INITIAL_FRAME_SEQ, encode_payload},
            storage::WalStorage,
        },
    };

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

        let wal_batch =
            crate::wal::append_tombstone_column(&batch, Some(&[false])).expect("wal batch");
        let payload = WalPayload::DynBatch {
            batch: wal_batch.clone(),
            commit_ts: Timestamp::new(42),
        };
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
        let events = replayer.scan().expect("scan");
        assert_eq!(events.len(), 2);

        match &events[0] {
            WalEvent::DynAppend {
                provisional_id,
                batch: decoded,
                tombstones,
            } => {
                assert_eq!(*provisional_id, 7);
                assert_eq!(*tombstones, vec![false]);
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

        let wal_batch =
            crate::wal::append_tombstone_column(&batch, Some(&[false])).expect("wal batch");
        let payload = WalPayload::DynBatch {
            batch: wal_batch.clone(),
            commit_ts: Timestamp::new(42),
        };
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
        let events = replayer.scan().expect("scan succeeds");

        assert_eq!(events.len(), 1, "commit frame should be ignored");
        match &events[0] {
            WalEvent::DynAppend {
                provisional_id,
                batch: decoded,
                tombstones,
            } => {
                assert_eq!(*provisional_id, 9);
                assert_eq!(*tombstones, vec![false]);
                assert_eq!(decoded.num_rows(), 1);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
