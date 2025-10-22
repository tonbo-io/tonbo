//! Frame encoding and decoding primitives for the WAL.

use std::convert::TryFrom;

use crc32c::crc32c;
use typed_arrow::arrow_array::RecordBatch;

use crate::wal::{WalError, WalResult};

/// Maximum supported frame version.
pub const FRAME_VERSION: u16 = 1;

/// Magic constant identifying Tonbo WAL frames (`"TONW"`).
pub const FRAME_MAGIC: u32 = 0x544F_4E57;

/// First sequence number written to disk.
///
/// Leaving `0` unused keeps a sentinel reserved for unset/invalid sequences in
/// intermediate buffers and metrics, which makes tail recovery checks easier to
/// reason about and mirrors other log formats that treat zero as "no value".
pub const INITIAL_FRAME_SEQ: u64 = 1;

/// Total number of header bytes emitted for each frame.
pub const FRAME_HEADER_SIZE: usize = 4 + 2 + 2 + 8 + 4 + 4;

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

#[cfg(test)]
mod tests {
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
            assert_eq!(FrameType::try_from(disc).unwrap(), ty);
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
        assert!(matches!(err, WalError::Corrupt("frame payload crc32c mismatch")));
    }

    #[test]
    fn decode_rejects_zero_sequence() {
        let payload = [0_u8; 4];
        let mut frame = build_frame(FrameType::TxnAppend, INITIAL_FRAME_SEQ, &payload);
        // Overwrite the sequence field with zero.
        frame[8..16].fill(0);
        let err = FrameHeader::decode_from(&frame).expect_err("zero sequence should fail");
        assert!(matches!(err, WalError::Corrupt("frame sequence zero is reserved")));
    }
}
