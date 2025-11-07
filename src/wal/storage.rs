//! Storage glue that relies directly on fusio traits.

use std::{io, sync::Arc};

use fusio::{
    DynFs, Read, Write,
    dynamic::fs::DynFile,
    error::Error as FusioError,
    fs::{FileSystemTag, OpenOptions},
    path::{Path, PathPart},
};
use futures::StreamExt;

use crate::wal::{
    WalError, WalResult,
    frame::{FRAME_HEADER_SIZE, FrameHeader},
};

/// Shared storage facade for WAL segments backed by fusio.
#[derive(Clone)]
pub struct WalStorage {
    /// Filesystem implementation used for segment operations.
    fs: Arc<dyn DynFs>,
    /// Root directory under which WAL segments are stored.
    root: Path,
}

impl WalStorage {
    /// Create a new storage facade over the provided filesystem.
    pub fn new(fs: Arc<dyn DynFs>, root: Path) -> Self {
        Self { fs, root }
    }

    /// Access the underlying filesystem.
    pub fn fs(&self) -> &Arc<dyn DynFs> {
        &self.fs
    }

    /// Root directory for WAL segments.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Open (or create) a WAL segment starting at the provided sequence.
    pub async fn open_segment(&self, seq: u64) -> WalResult<WalSegment> {
        self.ensure_dir(self.root()).await?;

        let segment_path = self.segment_path(seq)?;
        let options = Self::write_options();

        let file = self
            .fs
            .open_options(&segment_path, options)
            .await
            .map_err(|err| {
                WalError::Storage(format!(
                    "failed to open wal segment {}: {}",
                    segment_path, err
                ))
            })?;

        Ok(WalSegment::new(segment_path, file))
    }

    /// Remove an existing WAL segment by path.
    pub async fn remove_segment(&self, path: &Path) -> WalResult<()> {
        self.fs.remove(path).await.map_err(|err| {
            WalError::Storage(format!("failed to remove wal segment {}: {}", path, err))
        })
    }

    /// Expose convenience to create the WAL directory structure.
    pub async fn ensure_dir(&self, path: &Path) -> WalResult<()> {
        self.fs.create_dir_all(path).await.map_err(|err| {
            WalError::Storage(format!("failed to ensure wal directory {}: {}", path, err))
        })
    }

    /// Provide default open options for writable segments.
    pub fn write_options() -> OpenOptions {
        // `OpenOptions::truncate(false)` (the default) instructs the concrete backend to open
        // the handle in append mode (see fusio's disk adapters), so subsequent writes extend
        // the segment instead of clobbering existing frames.
        OpenOptions::default()
            .read(false)
            .write(true)
            .create(true)
            .truncate(false)
    }

    /// Provide default options for read-only access.
    pub fn read_options() -> OpenOptions {
        OpenOptions::default().read(true).write(false)
    }

    fn segment_path(&self, seq: u64) -> WalResult<Path> {
        let filename = format!("wal-{seq:020}.tonwal");
        let part = PathPart::parse(&filename).map_err(|err| {
            WalError::Storage(format!("invalid wal segment name {filename}: {err}"))
        })?;
        Ok(self.root.child(part))
    }

    /// Enumerate existing WAL segments along with their sizes.
    pub async fn list_segments(&self) -> WalResult<Vec<SegmentDescriptor>> {
        let mut entries = Vec::new();
        let mut stream = match self.fs.list(&self.root).await {
            Ok(stream) => stream,
            Err(FusioError::Io(err)) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(entries);
            }
            Err(err) => {
                return Err(WalError::Storage(format!(
                    "failed to list wal dir {}: {}",
                    self.root.as_ref(),
                    err
                )));
            }
        };

        while let Some(meta_result) = stream.next().await {
            let meta = meta_result.map_err(|err| {
                WalError::Storage(format!(
                    "failed to read wal metadata under {}: {}",
                    self.root.as_ref(),
                    err
                ))
            })?;
            if let Some(descriptor) = self.describe_segment(meta.path).await? {
                entries.push(descriptor);
            }
        }

        if entries.is_empty() && matches!(self.fs.file_system(), FileSystemTag::S3) {
            let root_prefix = Path::default();
            let mut fallback_stream = self.fs.list(&root_prefix).await.map_err(|err| {
                WalError::Storage(format!(
                    "failed fallback listing for wal dir {}: {}",
                    self.root.as_ref(),
                    err
                ))
            })?;

            while let Some(meta_result) = fallback_stream.next().await {
                let meta = meta_result.map_err(|err| {
                    WalError::Storage(format!(
                        "failed to read wal metadata under {} during fallback: {}",
                        self.root.as_ref(),
                        err
                    ))
                })?;

                if !meta.path.prefix_matches(&self.root) {
                    continue;
                }

                if let Some(descriptor) = self.describe_segment(meta.path).await? {
                    entries.push(descriptor);
                }
            }
        }

        entries.sort_by_key(|entry| entry.seq);
        Ok(entries)
    }

    /// Decode the frame bounds for the specified WAL segment.
    ///
    /// Returns `Ok(None)` if the segment contains no frames. Callers should treat a `None` result
    /// as an empty segment and typically skip emitting a manifest reference.
    pub async fn segment_frame_bounds(&self, path: &Path) -> WalResult<Option<SegmentFrameBounds>> {
        let mut file = self
            .fs
            .open_options(path, Self::read_options())
            .await
            .map_err(|err| {
                WalError::Storage(format!(
                    "failed to open wal segment {} for frame bounds: {}",
                    path, err
                ))
            })?;

        let (read_res, data) = file.read_to_end_at(Vec::new(), 0).await;
        let data = read_res.map(|_| data).map_err(|err| {
            WalError::Storage(format!(
                "failed to read wal segment {} for frame bounds: {}",
                path, err
            ))
        })?;

        decode_frame_bounds(&data)
    }

    /// Inspect the on-disk WAL tail, returning metadata about the active segment and frame
    /// sequence.
    pub async fn tail_metadata(&self) -> WalResult<Option<TailMetadata>> {
        let mut segments = self.list_segments().await?;
        if segments.is_empty() {
            return Ok(None);
        }

        let mut active = segments.pop().expect("segments.pop matches prior is_empty");
        let completed = segments;
        let TailScan {
            last,
            file_len,
            truncated,
            buffer,
        } = self.scan_tail(&active.path).await?;

        let last_frame_seq = last.as_ref().map(|meta| meta.seq);
        let last_provisional_id = last.as_ref().and_then(|meta| meta.provisional_id);
        let last_valid_offset = last.as_ref().map(|meta| meta.end_offset);

        let mut truncated_tail = truncated;
        if !truncated_tail {
            if let Some(offset) = last_valid_offset {
                if offset < file_len {
                    truncated_tail = true;
                }
            } else if file_len > 0 {
                truncated_tail = true;
            }
        }

        if truncated_tail {
            let safe_len = last_valid_offset.unwrap_or(0);
            let mut preserved = buffer.unwrap_or_else(Vec::new);
            if preserved.len() > safe_len {
                preserved.truncate(safe_len);
            } else if preserved.len() < safe_len {
                preserved.resize(safe_len, 0);
            }
            self.overwrite_segment(&active.path, preserved).await?;
            active.bytes = safe_len;
        } else {
            active.bytes = file_len;
        }

        Ok(Some(TailMetadata {
            active,
            completed,
            last_frame_seq,
            last_provisional_id,
            last_valid_offset,
            truncated_tail,
        }))
    }

    async fn scan_tail(&self, path: &Path) -> WalResult<TailScan> {
        let mut file = self
            .fs
            .open_options(path, Self::read_options())
            .await
            .map_err(|err| {
                WalError::Storage(format!(
                    "failed to open wal segment {} for tail read: {}",
                    path, err
                ))
            })?;
        let (read_res, data) = file.read_to_end_at(Vec::new(), 0).await;
        let data = read_res.map(|_| data).map_err(|err| {
            WalError::Storage(format!(
                "failed to read wal segment {} for tail: {}",
                path, err
            ))
        })?;

        let file_len = data.len();
        let mut offset = 0usize;
        let mut last = None;
        let mut truncated = false;
        while offset < data.len() {
            let slice = &data[offset..];
            let header = match FrameHeader::decode_from(slice) {
                Ok((header, _)) => header,
                Err(WalError::Corrupt(reason))
                    if reason == "frame header truncated"
                        || reason == "frame payload truncated" =>
                {
                    truncated = true;
                    break;
                }
                Err(err) => return Err(err),
            };

            let payload_end = offset + FRAME_HEADER_SIZE + header.len as usize;
            if payload_end > data.len() {
                truncated = true;
                break;
            }

            let payload = &data[(offset + FRAME_HEADER_SIZE)..payload_end];
            let provisional_id = match header.frame_type {
                crate::wal::frame::FrameType::TxnAppend
                | crate::wal::frame::FrameType::TxnCommit => {
                    if payload.len() < 8 {
                        truncated = true;
                        break;
                    }
                    let mut id_bytes = [0u8; 8];
                    id_bytes.copy_from_slice(&payload[..8]);
                    Some(u64::from_le_bytes(id_bytes))
                }
                _ => None,
            };

            last = Some(FrameTailMeta {
                seq: header.seq,
                provisional_id,
                end_offset: payload_end,
            });
            offset = payload_end;
        }

        if let Some(ref meta) = last {
            if meta.end_offset < data.len() {
                truncated = true;
            }
        } else if !data.is_empty() {
            truncated = true;
        }

        let buffer = if truncated { Some(data) } else { None };

        Ok(TailScan {
            last,
            file_len,
            truncated,
            buffer,
        })
    }

    async fn overwrite_segment(&self, path: &Path, data: Vec<u8>) -> WalResult<()> {
        let path_display = path.to_string();
        let mut file = self
            .fs
            .open_options(path, OpenOptions::default().truncate(true))
            .await
            .map_err(|err| {
                WalError::Storage(format!(
                    "failed to truncate wal segment {}: {}",
                    path_display, err
                ))
            })?;

        if !data.is_empty() {
            let (write_res, _buf) = file.write_all(data).await;
            write_res.map_err(|err| {
                WalError::Storage(format!(
                    "failed to rewrite wal segment {}: {}",
                    path_display, err
                ))
            })?;
        }

        file.flush().await.map_err(|err| {
            WalError::Storage(format!(
                "failed to flush wal segment {}: {}",
                path_display, err
            ))
        })?;
        Ok(())
    }
}

impl WalStorage {
    async fn describe_segment(&self, path: Path) -> WalResult<Option<SegmentDescriptor>> {
        let Some(seq) = segment_sequence(path.filename()) else {
            return Ok(None);
        };
        let file = self
            .fs
            .open_options(&path, Self::read_options())
            .await
            .map_err(|err| {
                WalError::Storage(format!(
                    "failed to open wal segment {} for size: {}",
                    path, err
                ))
            })?;
        let size = file
            .size()
            .await
            .map_err(|err| backend_err("determine wal segment size", err))?
            as usize;
        Ok(Some(SegmentDescriptor {
            seq,
            path,
            bytes: size,
        }))
    }
}

struct FrameTailMeta {
    seq: u64,
    provisional_id: Option<u64>,
    end_offset: usize,
}

struct TailScan {
    last: Option<FrameTailMeta>,
    file_len: usize,
    truncated: bool,
    buffer: Option<Vec<u8>>,
}

/// Handle representing an opened WAL segment file.
pub struct WalSegment {
    path: Path,
    file: Box<dyn DynFile>,
}

impl WalSegment {
    fn new(path: Path, file: Box<dyn DynFile>) -> Self {
        Self { path, file }
    }

    /// Return the path to the underlying segment.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Access the writable segment handle.
    pub fn file_mut(&mut self) -> &mut Box<dyn DynFile> {
        &mut self.file
    }
}

/// Descriptor describing an on-disk WAL segment.
#[derive(Clone, Debug)]
pub struct SegmentDescriptor {
    /// Sequence embedded in the file name.
    pub seq: u64,
    /// Path to the segment file.
    pub path: Path,
    /// Reported size of the segment in bytes.
    pub bytes: usize,
}

/// Inclusive frame sequence bounds for a WAL segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentFrameBounds {
    /// First frame sequence stored in the segment.
    pub first_seq: u64,
    /// Last frame sequence stored in the segment.
    pub last_seq: u64,
}

/// Snapshot describing the WAL tail state.
///
/// The writer relies on this bundle to resume cleanly after restart: `active`
/// is the segment it should keep appending to, `completed` lists older
/// segments so retention can trim them, and `last_frame_seq` records the last
/// fully decoded frame so new writes continue with the next sequence number.
pub struct TailMetadata {
    /// Active segment that new frames will append to.
    pub active: SegmentDescriptor,
    /// Completed segments ordered from oldest to newest.
    pub completed: Vec<SegmentDescriptor>,
    /// Sequence of the last fully decoded frame (if any).
    pub last_frame_seq: Option<u64>,
    /// Provisional ID carried by the last complete frame (if any).
    pub last_provisional_id: Option<u64>,
    /// Byte offset immediately after the last fully decoded frame within the active segment.
    pub last_valid_offset: Option<usize>,
    /// Indicates whether truncated bytes were observed (and repaired) at the tail.
    pub truncated_tail: bool,
}

fn decode_frame_bounds(data: &[u8]) -> WalResult<Option<SegmentFrameBounds>> {
    if data.is_empty() {
        return Ok(None);
    }

    let mut remaining = data;
    let mut first = None;
    let mut last = None;
    while !remaining.is_empty() {
        let (header, rest) = FrameHeader::decode_from(remaining)?;
        if first.is_none() {
            first = Some(header.seq);
        }
        last = Some(header.seq);
        remaining = rest;
    }

    match (first, last) {
        (Some(first_seq), Some(last_seq)) => Ok(Some(SegmentFrameBounds {
            first_seq,
            last_seq,
        })),
        _ => Ok(None),
    }
}

fn segment_sequence(filename: Option<&str>) -> Option<u64> {
    let raw = filename?;
    let trimmed = raw.strip_prefix("wal-")?.strip_suffix(".tonwal")?;
    if trimmed.len() != 20 {
        return None;
    }
    trimmed.parse().ok()
}

fn backend_err(action: &str, err: FusioError) -> WalError {
    WalError::Storage(format!("failed to {action}: {err}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fusio::{Read, Write, impls::mem::fs::InMemoryFs, path::Path};
    use futures::executor::block_on;

    use super::*;

    #[test]
    fn ensure_dir_is_idempotent() {
        block_on(async {
            let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
            let root = Path::parse("wal").expect("valid wal root");
            let storage = WalStorage::new(fs, root.clone());

            storage
                .ensure_dir(&root)
                .await
                .expect("first create succeeds");
            storage
                .ensure_dir(&root)
                .await
                .expect("second create succeeds");
        });
    }

    #[test]
    fn open_segment_persists_and_removes() {
        block_on(async {
            let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
            let root = Path::parse("wal").expect("valid wal root");
            let storage = WalStorage::new(Arc::clone(&fs), root.clone());

            let mut segment = storage.open_segment(1).await.expect("open segment");
            let data = vec![1_u8, 2, 3];
            let (write_res, _buf) = segment.file_mut().write_all(data.clone()).await;
            write_res.expect("write succeeds");
            segment.file_mut().flush().await.expect("flush succeeds");

            let segment_path = segment.path().clone();
            drop(segment);

            let mut reopened = storage
                .fs()
                .open_options(&segment_path, WalStorage::read_options())
                .await
                .expect("reopen for read");
            let (read_res, contents) = reopened.read_to_end_at(Vec::new(), 0).await;
            read_res.expect("read succeeds");
            assert_eq!(contents, data);

            storage
                .remove_segment(&segment_path)
                .await
                .expect("remove succeeds");
            let reopen_result = storage
                .fs()
                .open_options(&segment_path, WalStorage::read_options())
                .await;
            assert!(reopen_result.is_err(), "segment should be gone");
        });
    }

    #[test]
    fn open_segment_appends_existing_data() {
        block_on(async {
            let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
            let root = Path::parse("wal").expect("valid wal root");
            let storage = WalStorage::new(Arc::clone(&fs), root);

            let mut first = storage.open_segment(5).await.expect("open segment");
            let (write_res, _) = first.file_mut().write_all(b"abc".to_vec()).await;
            write_res.expect("initial write succeeds");
            first.file_mut().flush().await.expect("flush succeeds");
            drop(first);

            let mut second = storage.open_segment(5).await.expect("reopen segment");
            let (write_res, _) = second.file_mut().write_all(b"def".to_vec()).await;
            write_res.expect("append succeeds");
            second.file_mut().flush().await.expect("flush succeeds");
            let path = second.path().clone();
            drop(second);

            let mut reader = storage
                .fs()
                .open_options(&path, WalStorage::read_options())
                .await
                .expect("open for read");
            let (read_res, contents) = reader.read_to_end_at(Vec::new(), 0).await;
            read_res.expect("read succeeds");
            assert_eq!(contents, b"abcdef");
        });
    }

    #[test]
    fn list_segments_reports_sequence_and_size() {
        block_on(async {
            let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
            let root = Path::parse("wal").expect("valid wal root");
            let storage = WalStorage::new(Arc::clone(&fs), root.clone());

            let mut first = storage.open_segment(1).await.expect("open first");
            let (write_res, _) = first.file_mut().write_all(b"abc".to_vec()).await;
            write_res.expect("write first");
            first.file_mut().flush().await.expect("flush first");
            drop(first);

            let mut second = storage.open_segment(2).await.expect("open second");
            let (write_res, _) = second.file_mut().write_all(b"defghi".to_vec()).await;
            write_res.expect("write second");
            second.file_mut().flush().await.expect("flush second");
            drop(second);

            let segments = storage.list_segments().await.expect("list segments");
            assert_eq!(segments.len(), 2);
            assert_eq!(segments[0].seq, 1);
            assert_eq!(segments[0].bytes, 3);
            assert_eq!(segments[1].seq, 2);
            assert_eq!(segments[1].bytes, 6);
            assert!(
                segments[0]
                    .path
                    .as_ref()
                    .ends_with("wal-00000000000000000001.tonwal")
            );
            assert!(
                segments[1]
                    .path
                    .as_ref()
                    .ends_with("wal-00000000000000000002.tonwal")
            );
        });
    }

    #[test]
    fn tail_metadata_reports_last_frame_sequence() {
        use arrow_array::{Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        block_on(async {
            let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
            let root = Path::parse("wal-tail").expect("valid wal root");
            let storage = WalStorage::new(Arc::clone(&fs), root.clone());

            storage.ensure_dir(&root).await.expect("ensure dir");

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["a"])) as _,
                    Arc::new(Int32Array::from(vec![1])) as _,
                ],
            )
            .expect("batch");

            let frames = crate::wal::frame::encode_autocommit_frames(
                batch.clone(),
                vec![false],
                7,
                crate::mvcc::Timestamp::new(42),
            )
            .expect("encode");

            let mut seq = crate::wal::frame::INITIAL_FRAME_SEQ;
            let mut bytes = Vec::new();
            for frame in frames {
                bytes.extend_from_slice(&frame.into_bytes(seq));
                seq += 1;
            }

            let mut segment = storage.open_segment(5).await.expect("open segment");
            let (write_res, _) = segment.file_mut().write_all(bytes).await;
            write_res.expect("write wal");
            segment.file_mut().flush().await.expect("flush");
            drop(segment);

            let tail = storage.tail_metadata().await.expect("tail metadata");
            let tail = tail.expect("existing tail");
            assert_eq!(tail.active.seq, 5);
            assert!(tail.completed.is_empty());
            assert_eq!(
                tail.last_frame_seq,
                Some(crate::wal::frame::INITIAL_FRAME_SEQ + 1)
            );
            assert_eq!(tail.last_provisional_id, Some(7));
        });
    }
}
