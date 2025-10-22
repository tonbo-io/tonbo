//! Frame encoding and decoding primitives for the WAL.

use typed_arrow::arrow_array::RecordBatch;

use crate::wal::{WalError, WalResult};

/// Maximum supported frame version.
pub const FRAME_VERSION: u16 = 1;

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
    pub seq: u64,
    /// Payload length in bytes.
    pub len: u32,
    /// CRC32C checksum covering the payload.
    pub crc32c: u32,
}

impl FrameHeader {
    /// Serialize the header into the provided buffer.
    pub fn encode_into(&self, _buf: &mut Vec<u8>) {
        let _ = _buf;
        // Implementation placeholder.
        todo!("FrameHeader::encode_into");
    }

    /// Parse a header from the provided bytes.
    pub fn decode_from(_bytes: &[u8]) -> WalResult<(Self, &[u8])> {
        Err(WalError::Unimplemented("FrameHeader::decode_from"))
    }
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
        /// Record batch payload.
        batch: RecordBatch,
    },
    /// Commit the transaction at the supplied timestamp.
    TxnCommit {
        /// Provisional identifier.
        provisional_id: u64,
        /// Commit timestamp.
        commit_ts: u64,
    },
    /// Abort the open transaction (optional frame).
    TxnAbort {
        /// Provisional identifier associated with the transaction.
        provisional_id: u64,
    },
    /// Advisory seal marker (not required for correctness).
    SealMarker,
}
