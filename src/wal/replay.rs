//! Recovery helpers for scanning WAL segments.

use std::{io, sync::Arc};

use fusio::{DynFs, Read, error::Error as FusioError, fs::OpenOptions, path::Path as FusioPath};
use futures::{StreamExt, executor::block_on};

use crate::wal::{
    WalConfig, WalError, WalResult,
    frame::{FRAME_HEADER_SIZE, FrameHeader, WalEvent, decode_frame},
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
    pub fn scan(&self) -> WalResult<Vec<WalEvent>> {
        block_on(self.scan_async())
    }

    /// Access the configuration.
    pub fn config(&self) -> &WalConfig {
        &self.cfg
    }

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
                .open_options(&path, OpenOptions::default())
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
                    Err(WalError::Corrupt(_)) => return Ok(events),
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
                    Err(WalError::Corrupt(_)) | Err(WalError::Codec(_)) => return Ok(events),
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
    use std::{fs, sync::Arc, time::Duration};

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use tokio::runtime::Runtime;

    use super::*;
    use crate::{
        mvcc::Timestamp,
        wal::{
            WalPayload,
            frame::{INITIAL_FRAME_SEQ, encode_payload},
        },
    };

    fn with_tokio_runtime<F, R>(f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let runtime = Runtime::new().expect("tokio runtime");
        let guard = runtime.enter();
        let result = f();
        drop(guard);
        runtime.shutdown_timeout(Duration::from_secs(0));
        result
    }

    #[test]
    fn replayer_returns_logged_events() {
        let temp_dir = std::env::temp_dir().join(format!(
            "tonbo-wal-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&temp_dir).expect("create wal dir");

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
        fs::write(temp_dir.join("wal-00000000000000000001.tonwal"), bytes)
            .expect("write wal segment");

        let mut cfg = WalConfig::default();
        cfg.dir = FusioPath::from_filesystem_path(&temp_dir).expect("wal dir path");
        let replayer = Replayer::new(cfg);
        let events = with_tokio_runtime(|| replayer.scan()).expect("scan");
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

        fs::remove_dir_all(&temp_dir).expect("cleanup");
    }

    #[test]
    fn replayer_stops_after_truncated_tail() {
        let temp_dir = std::env::temp_dir().join(format!(
            "tonbo-wal-truncated-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&temp_dir).expect("create wal dir");

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
        let mut bytes = Vec::new();
        let append_bytes = frames[0].clone().into_bytes(seq);
        bytes.extend_from_slice(&append_bytes);
        seq += 1;
        let mut commit_bytes = frames[1].clone().into_bytes(seq);
        commit_bytes.truncate(commit_bytes.len() - 3);
        bytes.extend_from_slice(&commit_bytes);

        fs::write(temp_dir.join("wal-00000000000000000005.tonwal"), bytes)
            .expect("write wal segment");

        let mut cfg = WalConfig::default();
        cfg.dir = FusioPath::from_filesystem_path(&temp_dir).expect("wal dir path");
        let replayer = Replayer::new(cfg);
        let events = with_tokio_runtime(|| replayer.scan()).expect("scan succeeds");

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

        fs::remove_dir_all(&temp_dir).expect("cleanup");
    }
}
