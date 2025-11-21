//! Frame encoding and decoding primitives for the WAL.

use std::{convert::TryFrom, io::Cursor, mem::size_of, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use crc32c::crc32c;

#[cfg(test)]
use crate::wal::DynBatchPayload;
use crate::{
    inmem::immutable::memtable::MVCC_COMMIT_COL,
    mvcc::Timestamp,
    wal::{WalCommand, WalError, WalResult, append_commit_column, split_commit_column},
};

/// Maximum supported frame version.
pub const FRAME_VERSION: u16 = 1;

/// Magic constant identifying Tonbo WAL frames (`"TONQ"`).
/// ASCII tag stays human-readable while landing on a 32-bit prime for cheap corruption checks.
pub const FRAME_MAGIC: u32 = 0x544F_4E51;

/// First sequence number written to disk.
///
/// Leaving `0` unused keeps a sentinel reserved for unset/invalid sequences in
/// intermediate buffers and metrics, which makes tail recovery checks easier to
/// reason about and mirrors other log formats that treat zero as "no value".
pub const INITIAL_FRAME_SEQ: u64 = 1;

/// Total number of header bytes emitted for each frame.
pub const FRAME_HEADER_SIZE: usize = frame_header_size();

const fn frame_header_size() -> usize {
    size_of::<u32>() // magic
        + size_of::<u16>() // version
        + size_of::<u16>() // frame type discriminant
        + size_of::<u64>() // sequence
        + size_of::<u32>() // payload length
        + size_of::<u32>() // payload crc32c
}

/// Payload prefix size for `TxnAppend` frames: provisional id + mode + row_mode + reserved bytes.
///
/// We reserve 6 bytes following the `row_mode` so future metadata (e.g. schema hashes,
/// compression hints) can slot in without reshaping the layout or bumping the frame version.
const TXN_APPEND_PREFIX_SIZE: usize = 8 + 1 + 1 + 6;
/// Number of reserved bytes after the `row_mode` byte in a `TxnAppend` payload.
const TXN_APPEND_RESERVED_BYTES: usize = 6;
/// Discriminant for dynamic-mode appends written in the `TxnAppend` payload.
const APPEND_MODE_DYN: u8 = 0;
/// Discriminant reserved for typed-mode appends.
const APPEND_MODE_TYPED: u8 = 1;
/// Payload size for `TxnCommit` frames (provisional id + commit timestamp).
const TXN_COMMIT_PAYLOAD_SIZE: usize = 8 + 8;

/// Logical payload type encoded inside a `TxnAppend` frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DynRowMode {
    /// Full row payload carrying `_commit_ts` metadata.
    RowPayload = 0,
    /// Key-only delete payload carrying key columns + `_commit_ts`.
    KeyDelete = 1,
}

impl DynRowMode {
    fn as_u8(self) -> u8 {
        match self {
            DynRowMode::RowPayload => 0,
            DynRowMode::KeyDelete => 1,
        }
    }

    fn from_u8(value: u8) -> Result<Self, WalError> {
        match value {
            0 => Ok(DynRowMode::RowPayload),
            1 => Ok(DynRowMode::KeyDelete),
            _ => Err(WalError::Corrupt("unknown txn append row mode")),
        }
    }
}

/// Encoded frame payload paired with its discriminant.
#[derive(Debug, Clone)]
pub struct Frame {
    frame_type: FrameType,
    payload: Vec<u8>,
}

impl Frame {
    /// Construct a new frame payload.
    pub fn new(frame_type: FrameType, payload: Vec<u8>) -> Self {
        Self {
            frame_type,
            payload,
        }
    }

    /// Access the frame type.
    pub fn frame_type(&self) -> FrameType {
        self.frame_type
    }

    /// Access the encoded payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consume the frame and return the payload bytes.
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }

    /// Build a [`FrameHeader`] for this payload using the supplied sequence.
    pub fn header(&self, seq: u64) -> FrameHeader {
        FrameHeader::new(seq, self.frame_type, &self.payload)
    }

    /// Serialize the frame into bytes by prepending the computed header.
    pub fn into_bytes(self, seq: u64) -> Vec<u8> {
        let header = FrameHeader::new(seq, self.frame_type, &self.payload);
        let mut buf = Vec::with_capacity(FRAME_HEADER_SIZE + self.payload.len());
        header.encode_into(&mut buf);
        buf.extend_from_slice(&self.payload);
        buf
    }
}

/// Discriminant describing the logical frame type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    /// Transaction begin marker.
    TxnBegin,
    /// Transaction append payload.
    TxnAppend,
    /// Transaction commit marker.
    TxnCommit,
    /// Transaction abort marker.
    TxnAbort,
    /// Advisory seal marker.
    SealMarker,
    /// Reserved for future typed payloads.
    #[allow(dead_code)]
    TypedAppend,
}

impl FrameType {
    /// Return the on-disk discriminant for the frame type.
    pub const fn as_u16(self) -> u16 {
        match self {
            FrameType::TxnBegin => 0,
            FrameType::TxnAppend => 1,
            FrameType::TxnCommit => 2,
            FrameType::TxnAbort => 3,
            FrameType::SealMarker => 4,
            FrameType::TypedAppend => 5,
        }
    }
}

impl From<FrameType> for u16 {
    fn from(value: FrameType) -> Self {
        value.as_u16()
    }
}

impl TryFrom<u16> for FrameType {
    type Error = ();

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FrameType::TxnBegin),
            1 => Ok(FrameType::TxnAppend),
            2 => Ok(FrameType::TxnCommit),
            3 => Ok(FrameType::TxnAbort),
            4 => Ok(FrameType::SealMarker),
            5 => Ok(FrameType::TypedAppend),
            _ => Err(()),
        }
    }
}

/// Header prepended to every frame on disk.
#[derive(Debug, Clone)]
pub struct FrameHeader {
    /// Magic constant.
    pub magic: u32,
    /// Version number of the frame format.
    pub version: u16,
    /// Frame discriminant.
    pub frame_type: FrameType,
    /// Monotonically increasing frame sequence.
    ///
    /// Sequences start at [`INITIAL_FRAME_SEQ`] so `0` remains an explicit
    /// "unset" sentinel when staging frames or emitting metrics.
    pub seq: u64,
    /// Payload length in bytes.
    pub len: u32,
    /// CRC32C checksum covering only the payload bytes.
    ///
    /// We checksum the body instead of the header so the encoder can finalize
    /// header fields (including the CRC itself) without rehashing, and because
    /// payload corruption is the durability risk we need to detect; header
    /// fields receive explicit validation during decode.
    pub crc32c: u32,
}

impl FrameHeader {
    /// Construct a header for the provided payload bytes and sequence.
    pub fn new(seq: u64, frame_type: FrameType, payload: &[u8]) -> Self {
        debug_assert!(seq >= INITIAL_FRAME_SEQ, "frame sequence must be non-zero");
        Self {
            magic: FRAME_MAGIC,
            version: FRAME_VERSION,
            frame_type,
            seq,
            len: payload.len() as u32,
            crc32c: crc32c(payload),
        }
    }

    /// Serialize the header into the provided buffer.
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.magic.to_le_bytes());
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&u16::from(self.frame_type).to_le_bytes());
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.extend_from_slice(&self.len.to_le_bytes());
        buf.extend_from_slice(&self.crc32c.to_le_bytes());
    }

    /// Parse a header from the provided bytes.
    pub fn decode_from(bytes: &[u8]) -> WalResult<(Self, &[u8])> {
        if bytes.len() < FRAME_HEADER_SIZE {
            return Err(WalError::Corrupt("frame header truncated"));
        }

        let (header_bytes, rest) = bytes.split_at(FRAME_HEADER_SIZE);
        let mut magic_bytes = [0u8; 4];
        magic_bytes.copy_from_slice(&header_bytes[0..4]);
        let magic = u32::from_le_bytes(magic_bytes);
        if magic != FRAME_MAGIC {
            return Err(WalError::Corrupt("frame magic mismatch"));
        }

        let mut version_bytes = [0u8; 2];
        version_bytes.copy_from_slice(&header_bytes[4..6]);
        let version = u16::from_le_bytes(version_bytes);
        if version != FRAME_VERSION {
            return Err(WalError::Corrupt("unsupported frame version"));
        }

        let mut frame_type_bytes = [0u8; 2];
        frame_type_bytes.copy_from_slice(&header_bytes[6..8]);
        let frame_type_u16 = u16::from_le_bytes(frame_type_bytes);
        let frame_type = FrameType::try_from(frame_type_u16)
            .map_err(|_| WalError::Corrupt("unknown frame type"))?;

        let mut seq_bytes = [0u8; 8];
        seq_bytes.copy_from_slice(&header_bytes[8..16]);
        let seq = u64::from_le_bytes(seq_bytes);
        if seq == 0 {
            return Err(WalError::Corrupt("frame sequence zero is reserved"));
        }

        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&header_bytes[16..20]);
        let len = u32::from_le_bytes(len_bytes);

        let mut crc_bytes = [0u8; 4];
        crc_bytes.copy_from_slice(&header_bytes[20..24]);
        let crc32c_expected = u32::from_le_bytes(crc_bytes);

        let payload_len = len as usize;
        if rest.len() < payload_len {
            return Err(WalError::Corrupt("frame payload truncated"));
        }

        let (payload, remaining) = rest.split_at(payload_len);
        let crc32c_actual = crc32c(payload);
        if crc32c_actual != crc32c_expected {
            return Err(WalError::Corrupt("frame payload crc32c mismatch"));
        }

        let header = FrameHeader {
            magic,
            version,
            frame_type,
            seq,
            len,
            crc32c: crc32c_expected,
        };

        Ok((header, remaining))
    }
}

/// Encode a WAL command into one or more frames.
pub fn encode_command(command: WalCommand) -> WalResult<Vec<Frame>> {
    match command {
        WalCommand::TxnBegin { provisional_id } => {
            let begin = encode_txn_begin(provisional_id);
            Ok(vec![Frame::new(FrameType::TxnBegin, begin)])
        }
        WalCommand::TxnAppend {
            provisional_id,
            payload,
        } => match payload {
            crate::wal::DynBatchPayload::Row {
                batch,
                commit_ts_column,
            } => encode_txn_append_row(provisional_id, &batch, &commit_ts_column),
            crate::wal::DynBatchPayload::Delete { batch } => {
                encode_txn_append_delete(provisional_id, &batch)
            }
        },
        WalCommand::TxnCommit {
            provisional_id,
            commit_ts,
        } => {
            let commit = encode_txn_commit(provisional_id, commit_ts);
            Ok(vec![Frame::new(FrameType::TxnCommit, commit)])
        }
        WalCommand::TxnAbort { provisional_id } => {
            let abort = encode_txn_abort(provisional_id);
            Ok(vec![Frame::new(FrameType::TxnAbort, abort)])
        }
    }
}
fn encode_txn_append_row(
    provisional_id: u64,
    batch: &RecordBatch,
    commit_ts_column: &ArrayRef,
) -> WalResult<Vec<Frame>> {
    let wal_batch = append_commit_column(batch, commit_ts_column)?;
    let append = encode_txn_append_batch(provisional_id, DynRowMode::RowPayload, wal_batch)?;
    Ok(vec![Frame::new(FrameType::TxnAppend, append)])
}

fn encode_txn_append_delete(provisional_id: u64, batch: &RecordBatch) -> WalResult<Vec<Frame>> {
    let append = encode_txn_append_batch(provisional_id, DynRowMode::KeyDelete, batch.clone())?;
    Ok(vec![Frame::new(FrameType::TxnAppend, append)])
}

/// Convenience helper used mainly by tests to encode a single append + commit from raw inputs.
pub fn encode_autocommit_frames(
    batch: RecordBatch,
    provisional_id: u64,
    commit_ts: Timestamp,
) -> WalResult<Vec<Frame>> {
    let commit_array: ArrayRef =
        Arc::new(UInt64Array::from(vec![commit_ts.get(); batch.num_rows()])) as ArrayRef;
    let append_frames = encode_txn_append_row(provisional_id, &batch, &commit_array)?;
    let mut frames = append_frames;
    let commit_frame = Frame::new(
        FrameType::TxnCommit,
        encode_txn_commit(provisional_id, commit_ts),
    );
    frames.push(commit_frame);
    Ok(frames)
}

fn encode_txn_append_batch(
    provisional_id: u64,
    row_mode: DynRowMode,
    batch: RecordBatch,
) -> WalResult<Vec<u8>> {
    let mut payload = Vec::with_capacity(TXN_APPEND_PREFIX_SIZE);
    payload.extend_from_slice(&provisional_id.to_le_bytes());
    payload.push(APPEND_MODE_DYN);
    payload.push(row_mode.as_u8());
    payload.extend_from_slice(&[0u8; TXN_APPEND_RESERVED_BYTES]);

    // Encode the RecordBatch using Arrow IPC streaming so the payload is
    // self-contained and can be decoded via StreamReader during recovery.
    let mut ipc_buf = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut ipc_buf, batch.schema().as_ref()).map_err(codec_err)?;
        writer.write(&batch).map_err(codec_err)?;
        writer.finish().map_err(codec_err)?;
    }

    payload.extend_from_slice(&ipc_buf);
    Ok(payload)
}

fn encode_txn_commit(provisional_id: u64, commit_ts: Timestamp) -> Vec<u8> {
    let mut payload = Vec::with_capacity(TXN_COMMIT_PAYLOAD_SIZE);
    payload.extend_from_slice(&provisional_id.to_le_bytes());
    payload.extend_from_slice(&commit_ts.get().to_le_bytes());
    payload
}

fn encode_txn_begin(provisional_id: u64) -> Vec<u8> {
    provisional_id.to_le_bytes().to_vec()
}

fn encode_txn_abort(provisional_id: u64) -> Vec<u8> {
    provisional_id.to_le_bytes().to_vec()
}

/// Decode a single frame payload into a [`WalEvent`].
pub fn decode_frame(frame_type: FrameType, payload: &[u8]) -> WalResult<WalEvent> {
    match frame_type {
        FrameType::TxnBegin => decode_txn_begin(payload),
        FrameType::TxnAppend => decode_txn_append(payload),
        FrameType::TxnCommit => decode_txn_commit(payload),
        FrameType::TxnAbort => decode_txn_abort(payload),
        FrameType::SealMarker => decode_seal_marker(payload),
        FrameType::TypedAppend => Err(WalError::Unimplemented("typed wal payload decoding")),
    }
}

fn decode_txn_begin(payload: &[u8]) -> WalResult<WalEvent> {
    if payload.len() != 8 {
        return Err(WalError::Corrupt("txn begin payload size mismatch"));
    }
    let mut id_bytes = [0u8; 8];
    id_bytes.copy_from_slice(payload);
    let provisional_id = u64::from_le_bytes(id_bytes);
    Ok(WalEvent::TxnBegin { provisional_id })
}

fn decode_txn_append(payload: &[u8]) -> WalResult<WalEvent> {
    if payload.len() < TXN_APPEND_PREFIX_SIZE {
        return Err(WalError::Corrupt("txn append payload truncated"));
    }

    let mut id_bytes = [0u8; 8];
    id_bytes.copy_from_slice(&payload[0..8]);
    let provisional_id = u64::from_le_bytes(id_bytes);
    let mode = payload[8];
    let row_mode = DynRowMode::from_u8(payload[9])?;
    let payload_bytes = &payload[TXN_APPEND_PREFIX_SIZE..];

    match mode {
        APPEND_MODE_DYN => decode_dyn_append(provisional_id, row_mode, payload_bytes),
        APPEND_MODE_TYPED => Err(WalError::Unimplemented("typed wal payload decoding")),
        _ => Err(WalError::Corrupt("unknown txn append mode")),
    }
}

fn decode_dyn_append(
    provisional_id: u64,
    row_mode: DynRowMode,
    bytes: &[u8],
) -> WalResult<WalEvent> {
    if bytes.is_empty() {
        return Err(WalError::Codec(
            "txn append payload missing record batch".to_string(),
        ));
    }

    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(codec_err)?;
    let batch = reader
        .next()
        .transpose()
        .map_err(codec_err)?
        .ok_or_else(|| WalError::Codec("txn append payload missing record batch".to_string()))?;
    if reader.next().transpose().map_err(codec_err)?.is_some() {
        return Err(WalError::Codec(
            "txn append payload contained multiple record batches".to_string(),
        ));
    }

    match row_mode {
        DynRowMode::RowPayload => {
            let (stripped, commit_ts_hint, commit_ts_column) = split_commit_column(batch)?;

            let payload = DynAppendEvent {
                batch: stripped,
                commit_ts_hint,
                commit_ts_column,
            };

            Ok(WalEvent::DynAppend {
                provisional_id,
                payload,
            })
        }
        DynRowMode::KeyDelete => {
            let commit_ts_hint = extract_delete_commit_hint(&batch)?;
            let payload = DynDeleteEvent {
                batch,
                commit_ts_hint,
            };
            Ok(WalEvent::DynDelete {
                provisional_id,
                payload,
            })
        }
    }
}

fn decode_txn_commit(payload: &[u8]) -> WalResult<WalEvent> {
    if payload.len() != TXN_COMMIT_PAYLOAD_SIZE {
        return Err(WalError::Corrupt("txn commit payload size mismatch"));
    }
    let mut id_bytes = [0u8; 8];
    id_bytes.copy_from_slice(&payload[0..8]);
    let mut ts_bytes = [0u8; 8];
    ts_bytes.copy_from_slice(&payload[8..16]);
    let provisional_id = u64::from_le_bytes(id_bytes);
    let commit_ts = u64::from_le_bytes(ts_bytes);
    Ok(WalEvent::TxnCommit {
        provisional_id,
        commit_ts: Timestamp::from(commit_ts),
    })
}

fn decode_txn_abort(payload: &[u8]) -> WalResult<WalEvent> {
    if payload.len() != 8 {
        return Err(WalError::Corrupt("txn abort payload size mismatch"));
    }
    let mut id_bytes = [0u8; 8];
    id_bytes.copy_from_slice(payload);
    let provisional_id = u64::from_le_bytes(id_bytes);
    Ok(WalEvent::TxnAbort { provisional_id })
}

fn decode_seal_marker(payload: &[u8]) -> WalResult<WalEvent> {
    if !payload.is_empty() {
        return Err(WalError::Corrupt("seal marker payload should be empty"));
    }
    Ok(WalEvent::SealMarker)
}

fn codec_err<E>(err: E) -> WalError
where
    E: std::fmt::Display,
{
    WalError::Codec(err.to_string())
}

fn extract_delete_commit_hint(batch: &RecordBatch) -> WalResult<Option<Timestamp>> {
    let schema = batch.schema();
    let commit_idx = schema
        .fields()
        .iter()
        .position(|field| field.name() == MVCC_COMMIT_COL)
        .ok_or_else(|| WalError::Codec("delete payload missing _commit_ts column".to_string()))?;
    let commit_array = batch
        .column(commit_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| WalError::Codec("delete payload _commit_ts column not u64".to_string()))?;
    if commit_array.null_count() > 0 {
        return Err(WalError::Codec(
            "delete payload contained null commit_ts".to_string(),
        ));
    }
    if commit_array.is_empty() {
        return Ok(None);
    }
    let first = commit_array.value(0);
    if commit_array
        .iter()
        .any(|value| value.map(|v| v != first).unwrap_or(true))
    {
        return Err(WalError::Codec(
            "delete payload commit_ts column contained varying values".to_string(),
        ));
    }
    Ok(Some(Timestamp::from(first)))
}

/// Dynamic append payload surfaced during WAL replay.
#[derive(Debug)]
pub struct DynAppendEvent {
    /// Record batch payload (without MVCC columns).
    pub batch: RecordBatch,
    /// Commit timestamp derived from the append payload (if available).
    pub commit_ts_hint: Option<Timestamp>,
    /// Commit timestamp column recovered from the payload.
    pub commit_ts_column: ArrayRef,
}

impl DynAppendEvent {
    /// Number of rows stored in the batch.
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

/// Key-only delete payload surfaced during WAL replay.
#[derive(Debug)]
pub struct DynDeleteEvent {
    /// Delete batch encoded with key columns + `_commit_ts`.
    pub batch: RecordBatch,
    /// Commit timestamp derived from the payload (if available).
    pub commit_ts_hint: Option<Timestamp>,
}

/// High-level events produced by the frame decoder during recovery.
#[derive(Debug)]
pub enum WalEvent {
    /// Begin transaction with provisional identifier.
    TxnBegin {
        /// Provisional identifier associated with the transaction.
        provisional_id: u64,
    },
    /// Append dynamic rows to the open transaction.
    DynAppend {
        /// Provisional identifier.
        provisional_id: u64,
        /// Decoded append payload.
        payload: DynAppendEvent,
    },
    /// Append key-only deletes to the open transaction.
    DynDelete {
        /// Provisional identifier.
        provisional_id: u64,
        /// Decoded delete payload.
        payload: DynDeleteEvent,
    },
    /// Commit the transaction at the supplied timestamp.
    TxnCommit {
        /// Provisional identifier.
        provisional_id: u64,
        /// Commit timestamp.
        commit_ts: Timestamp,
    },
    /// Abort the open transaction (optional frame).
    TxnAbort {
        /// Provisional identifier associated with the transaction.
        provisional_id: u64,
    },
    /// Advisory seal marker (not required for correctness).
    SealMarker,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, UInt64Array};
    use typed_arrow::arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn build_frame(frame_type: FrameType, seq: u64, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        let header = FrameHeader {
            magic: FRAME_MAGIC,
            version: FRAME_VERSION,
            frame_type,
            seq,
            len: payload.len() as u32,
            crc32c: crc32c(payload),
        };

        header.encode_into(&mut buf);
        buf.extend_from_slice(payload);
        buf
    }

    #[test]
    fn frame_type_round_trip() {
        for ty in [
            FrameType::TxnBegin,
            FrameType::TxnAppend,
            FrameType::TxnCommit,
            FrameType::TxnAbort,
            FrameType::SealMarker,
            FrameType::TypedAppend,
        ] {
            let disc = u16::from(ty);
            let round_tripped =
                FrameType::try_from(disc).expect(&format!("failed to decode discriminant {disc}"));
            assert_eq!(round_tripped, ty);
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let payload: Vec<u8> = (0..32).map(|i| i ^ 0xAA).collect();
        let frame = build_frame(FrameType::TxnAppend, INITIAL_FRAME_SEQ, &payload);

        let (header, remaining) = FrameHeader::decode_from(&frame).expect("decode succeeds");
        assert_eq!(remaining.len(), 0);
        assert_eq!(header.magic, FRAME_MAGIC);
        assert_eq!(header.version, FRAME_VERSION);
        assert_eq!(header.frame_type, FrameType::TxnAppend);
        assert_eq!(header.seq, INITIAL_FRAME_SEQ);
        assert_eq!(header.len, payload.len() as u32);
        assert_eq!(header.crc32c, crc32c(&payload));
    }

    #[test]
    fn decode_rejects_truncated_header() {
        let frame = build_frame(FrameType::TxnCommit, INITIAL_FRAME_SEQ + 1, &[]);
        let err = FrameHeader::decode_from(&frame[..FRAME_HEADER_SIZE - 1])
            .expect_err("header truncation should fail");
        assert!(matches!(err, WalError::Corrupt("frame header truncated")));
    }

    #[test]
    fn decode_rejects_truncated_payload() {
        let payload = [1_u8, 2, 3, 4];
        let frame = build_frame(FrameType::TxnCommit, INITIAL_FRAME_SEQ + 2, &payload);
        let truncated = frame[..FRAME_HEADER_SIZE + payload.len() - 2].to_vec();
        let err = FrameHeader::decode_from(&truncated).expect_err("payload truncation should fail");
        assert!(matches!(err, WalError::Corrupt("frame payload truncated")));
    }

    #[test]
    fn decode_rejects_crc_mismatch() {
        let payload = [9_u8, 8, 7, 6];
        let mut frame = build_frame(FrameType::TxnAbort, INITIAL_FRAME_SEQ + 3, &payload);
        // Flip a payload byte without updating the checksum.
        let payload_offset = FRAME_HEADER_SIZE;
        frame[payload_offset] ^= 0xFF;
        let err = FrameHeader::decode_from(&frame).expect_err("crc mismatch should fail");
        assert!(matches!(
            err,
            WalError::Corrupt("frame payload crc32c mismatch")
        ));
    }

    #[test]
    fn decode_rejects_zero_sequence() {
        let payload = [0_u8; 4];
        let mut frame = build_frame(FrameType::TxnAppend, INITIAL_FRAME_SEQ, &payload);
        // Overwrite the sequence field with zero.
        frame[8..16].fill(0);
        let err = FrameHeader::decode_from(&frame).expect_err("zero sequence should fail");
        assert!(matches!(
            err,
            WalError::Corrupt("frame sequence zero is reserved")
        ));
    }
    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids = Arc::new(Int32Array::from(vec![1, 2, 3])) as _;
        let names = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as _;

        RecordBatch::try_new(schema, vec![ids, names]).expect("valid batch")
    }

    #[test]
    fn encode_command_dyn_batch_round_trip() {
        let base = sample_batch();
        let user_expected = base.clone();
        let commit_ts = Timestamp::new(42);
        let provisional_id = 7;

        let frames = encode_autocommit_frames(base.clone(), provisional_id, commit_ts)
            .expect("encode succeeds");

        assert_eq!(frames.len(), 2);

        match decode_frame(frames[0].frame_type(), frames[0].payload())
            .expect("append decode succeeds")
        {
            WalEvent::DynAppend {
                provisional_id: decoded_id,
                payload,
            } => {
                assert_eq!(decoded_id, provisional_id);
                assert_eq!(payload.commit_ts_hint, Some(commit_ts));
                let commit_array = payload
                    .commit_ts_column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("u64 column");
                assert_eq!(commit_array.len(), base.num_rows());
                assert!(
                    commit_array
                        .iter()
                        .all(|value| value.expect("non-null") == commit_ts.get())
                );
                assert_eq!(
                    payload.batch.schema().as_ref(),
                    user_expected.schema().as_ref()
                );
                assert_eq!(payload.batch.num_rows(), user_expected.num_rows());
            }
            other => panic!("unexpected event: {other:?}"),
        }

        match decode_frame(frames[1].frame_type(), frames[1].payload())
            .expect("commit decode succeeds")
        {
            WalEvent::TxnCommit {
                provisional_id: decoded_id,
                commit_ts: decoded_ts,
            } => {
                assert_eq!(decoded_id, provisional_id);
                assert_eq!(decoded_ts, commit_ts);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn encode_payload_handles_batches_larger_than_byte() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let ids = Arc::new(Int32Array::from((0..12).collect::<Vec<_>>())) as _;
        let large_batch = RecordBatch::try_new(schema, vec![ids]).expect("large batch");
        let commit_ts = Timestamp::new(77);

        let frames = encode_autocommit_frames(large_batch.clone(), 11, commit_ts).expect("encode");

        match decode_frame(frames[0].frame_type(), frames[0].payload())
            .expect("decode append frame")
        {
            WalEvent::DynAppend {
                provisional_id,
                payload,
            } => {
                assert_eq!(provisional_id, 11);
                assert_eq!(payload.commit_ts_hint, Some(commit_ts));
                let commit_array = payload
                    .commit_ts_column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("u64 column");
                assert_eq!(commit_array.len(), large_batch.num_rows());
                assert!(
                    commit_array
                        .iter()
                        .all(|value| value.expect("non-null") == commit_ts.get())
                );
                assert_eq!(payload.batch.num_rows(), 12);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn encode_command_does_not_clone_mvcc_columns() {
        let base = sample_batch();
        let commit_array = Arc::new(UInt64Array::from(vec![42_u64; base.num_rows()]));
        let commit_ref: ArrayRef = Arc::clone(&commit_array) as ArrayRef;
        let payload = DynBatchPayload::Row {
            batch: base,
            commit_ts_column: commit_ref,
        };

        let command = WalCommand::TxnAppend {
            provisional_id: 9,
            payload,
        };

        let frames = encode_command(command).expect("encode succeeds");
        assert_eq!(frames.len(), 1);
        assert_eq!(Arc::strong_count(&commit_array), 1);
    }

    #[test]
    fn encode_and_decode_delete_payload() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
        ]));
        let keys = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let commits = Arc::new(UInt64Array::from(vec![55_u64, 55_u64])) as ArrayRef;
        let delete_batch = RecordBatch::try_new(schema, vec![keys, commits]).expect("delete batch");
        let payload = DynBatchPayload::Delete {
            batch: delete_batch.clone(),
        };
        let frames = encode_command(WalCommand::TxnAppend {
            provisional_id: 11,
            payload,
        })
        .expect("encode succeeds");
        assert_eq!(frames.len(), 1);
        match decode_frame(frames[0].frame_type(), frames[0].payload()).expect("decode") {
            WalEvent::DynDelete {
                provisional_id,
                payload,
            } => {
                assert_eq!(provisional_id, 11);
                assert_eq!(payload.commit_ts_hint, Some(Timestamp::new(55)));
                assert_eq!(payload.batch.num_rows(), delete_batch.num_rows());
                assert_eq!(
                    payload.batch.schema().as_ref(),
                    delete_batch.schema().as_ref()
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn decode_append_rejects_truncated_payload() {
        let batch = sample_batch();
        let frames =
            encode_autocommit_frames(batch.clone(), 9, Timestamp::new(1)).expect("encode succeeds");

        let mut truncated = frames[0].payload().to_vec();
        truncated.truncate(TXN_APPEND_PREFIX_SIZE - 1);
        let err = decode_frame(FrameType::TxnAppend, &truncated)
            .expect_err("truncated append should fail");
        assert!(matches!(
            err,
            WalError::Corrupt("txn append payload truncated")
        ));
    }

    #[test]
    fn decode_commit_rejects_wrong_length() {
        let payload = vec![0_u8; TXN_COMMIT_PAYLOAD_SIZE - 2];
        let err = decode_frame(FrameType::TxnCommit, &payload)
            .expect_err("commit payload length mismatch should fail");
        assert!(matches!(
            err,
            WalError::Corrupt("txn commit payload size mismatch")
        ));
    }

    #[test]
    fn decode_append_rejects_unknown_mode() {
        let mut payload = Vec::with_capacity(TXN_APPEND_PREFIX_SIZE);
        payload.extend_from_slice(&123_u64.to_le_bytes());
        payload.push(0xFF);
        payload.extend_from_slice(&[0u8; TXN_APPEND_RESERVED_BYTES]);
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());

        let err =
            decode_frame(FrameType::TxnAppend, &payload).expect_err("unknown mode should fail");
        assert!(matches!(err, WalError::Corrupt("unknown txn append mode")));
    }
}
