//! Recovery helpers for scanning WAL segments.

use std::{fs, io, path::PathBuf};

use crate::wal::{
    WalConfig, WalError, WalResult,
    frame::{FRAME_HEADER_SIZE, FrameHeader, WalEvent, decode_frame},
};

/// Scans WAL segments on disk and yields decoded events.
pub struct Replayer {
    /// Configuration snapshot guiding where segments reside.
    cfg: WalConfig,
}

impl Replayer {
    /// Create a new replayer using the provided configuration.
    pub fn new(cfg: WalConfig) -> Self {
        Self { cfg }
    }

    /// Iterate through WAL segments and produce events.
    pub fn scan(&self) -> WalResult<Vec<WalEvent>> {
        let dir = self.cfg.dir.clone();
        let mut entries: Vec<PathBuf> = match fs::read_dir(&dir) {
            Ok(read_dir) => read_dir
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.metadata().map(|m| m.is_file()).unwrap_or(false))
                .map(|entry| entry.path())
                .collect(),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(err) => {
                return Err(WalError::Storage(format!(
                    "failed to read wal dir {}: {}",
                    dir.display(),
                    err
                )));
            }
        };

        entries.sort();

        let mut events = Vec::new();
        for entry in entries {
            let data = fs::read(&entry).map_err(|err| {
                WalError::Storage(format!(
                    "failed to read wal segment {}: {}",
                    entry.display(),
                    err
                ))
            })?;

            let mut offset: usize = 0;
            while offset < data.len() {
                let slice = &data[offset..];
                let (header, remaining) = FrameHeader::decode_from(slice)?;
                let payload_start = offset + FRAME_HEADER_SIZE;
                let payload_end = payload_start + header.len as usize;
                if payload_end > data.len() {
                    return Err(WalError::Corrupt("frame payload extends past segment"));
                }
                let payload = &data[payload_start..payload_end];
                let event = decode_frame(header.frame_type, payload)?;
                events.push(event);

                offset = data.len() - remaining.len();
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
    use std::{fs, sync::Arc};

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::{
        mvcc::Timestamp,
        wal::{
            WalPayload,
            frame::{INITIAL_FRAME_SEQ, encode_payload},
        },
    };

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
        fs::write(temp_dir.join("000001.wal"), bytes).expect("write wal segment");

        let mut cfg = WalConfig::default();
        cfg.dir = temp_dir.clone();
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

        fs::remove_dir_all(&temp_dir).expect("cleanup");
    }
}
