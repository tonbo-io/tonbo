//! SSTable skeleton definitions.
//!
//! The goal of this module is to capture the public surface for Tonbo's
//! on-disk runs without committing to a specific encoding or IO strategy just
//! yet. Future patches will flesh out the concrete dynamic and typed builders,
//! readers, and range scans using this scaffolding.
//!
//! Design note: all SST readers/writers are expected to use fusio-backed async
//! Parquet I/O (no std fs) to stay object-store friendly and non-blocking per
//! `docs/overview.md`/RFC 0005/0006. Readers will stream via Parquet async
//! reader with projection + page/index pruning; no eager whole-file loads.

use std::{convert::TryFrom, fmt, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::take::take as arrow_take;
use fusio::{
    DynFs,
    error::Error as FsError,
    executor::NoopExecutor,
    fs::OpenOptions,
    path::{Path, PathPart},
};
use fusio_parquet::{reader::AsyncReader, writer::AsyncWriter};
use futures::stream::{self, BoxStream, StreamExt};
use parquet::{
    arrow::{
        ProjectionMask,
        async_reader::{ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder},
        async_writer::AsyncArrowWriter,
    },
    basic::{Compression, ZstdLevel},
    errors::ParquetError,
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};

pub use crate::ondisk::merge::{SsTableMergeSource, SsTableMerger, SsTableStreamBatch};
use crate::{
    extractor::{KeyExtractError, KeyProjection},
    id::FileId,
    inmem::immutable::{
        ImmutableSegment,
        memtable::{DeleteSidecar, MVCC_COMMIT_COL, MvccColumns},
    },
    key::{KeyOwned, KeyOwnedError},
    manifest::ManifestError,
    mvcc::Timestamp,
    ondisk::merge::{decode_delete_sidecar, extract_delete_key_at, extract_key_at},
    query::Predicate,
};

/// Identifier for an SSTable stored on disk.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SsTableId(u64);

impl SsTableId {
    /// Create a new identifier based on a monotonically increasing counter or external handle.
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Access the raw identifier value.
    pub fn raw(&self) -> u64 {
        self.0
    }
}

/// Default iteration budget used by merge operations to guard against runaway loops.
/// `usize::MAX` disables the guard; set explicitly via config when debugging.
pub const DEFAULT_MERGE_ITERATION_BUDGET: usize = usize::MAX;

/// Configuration bundle shared by SSTable builders and readers.
#[derive(Clone)]
pub struct SsTableConfig {
    schema: SchemaRef,
    target_level: usize,
    compression: SsTableCompression,
    fs: Arc<dyn DynFs>,
    root: Path,
    key_extractor: Option<Arc<dyn KeyProjection>>,
    merge_iteration_budget: usize,
}

impl SsTableConfig {
    /// Build a new configuration bundle for an SSTable.
    pub fn new(schema: SchemaRef, fs: Arc<dyn DynFs>, root: Path) -> Self {
        Self {
            schema,
            target_level: 0,
            compression: SsTableCompression::default(),
            fs,
            root,
            key_extractor: None,
            merge_iteration_budget: DEFAULT_MERGE_ITERATION_BUDGET,
        }
    }

    /// Set the target compaction level for the table being produced.
    pub fn with_target_level(mut self, level: usize) -> Self {
        self.target_level = level;
        self
    }

    /// Choose the compression strategy applied to persisted pages.
    pub fn with_compression(mut self, compression: SsTableCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Access the Arrow schema used by this table.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Access the target level hint used when enqueuing this table into the version set.
    pub fn target_level(&self) -> usize {
        self.target_level
    }

    /// Access the configured compression scheme.
    pub fn compression(&self) -> SsTableCompression {
        self.compression
    }

    /// Access the Parquet-backed store used to create or read tables.
    pub fn fs(&self) -> &Arc<dyn DynFs> {
        &self.fs
    }

    /// Root directory prefix for all SSTable files.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Build a level-scoped path under the SST root.
    pub(crate) fn level_dir(&self, level: usize) -> Result<Path, SsTableError> {
        let level_name = format!("L{level}");
        let part = PathPart::parse(&level_name)
            .map_err(|err| SsTableError::InvalidPath(err.to_string()))?;
        Ok(self.root.child(part))
    }

    /// Attach a key extractor used for ordered merges or reader planning.
    pub fn with_key_extractor(mut self, extractor: Arc<dyn KeyProjection>) -> Self {
        self.key_extractor = Some(extractor);
        self
    }

    /// Access the configured key extractor, if any.
    pub fn key_extractor(&self) -> Option<&Arc<dyn KeyProjection>> {
        self.key_extractor.as_ref()
    }

    /// Configure the merge iteration budget used by compaction merges.
    pub fn with_merge_iteration_budget(mut self, budget: usize) -> Self {
        self.merge_iteration_budget = budget.max(1);
        self
    }

    /// Access the merge iteration budget used for merges.
    pub fn merge_iteration_budget(&self) -> usize {
        self.merge_iteration_budget
    }
}

impl fmt::Debug for SsTableConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SsTableConfig")
            .field("schema_fields", &self.schema.fields().len())
            .field("target_level", &self.target_level)
            .field("compression", &self.compression)
            .field("root", &self.root)
            .field("has_key_extractor", &self.key_extractor.is_some())
            .field("merge_iteration_budget", &self.merge_iteration_budget)
            .finish()
    }
}

/// Compression choices supported by the SSTable writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SsTableCompression {
    /// Leave pages uncompressed (useful for tests and debugging).
    None,
    /// Apply Zstd compression with default tuning.
    #[default]
    Zstd,
}

/// Handle returned by the SSTable builder once data has been flushed.
#[derive(Debug)]
pub struct SsTable {
    descriptor: SsTableDescriptor,
}

impl SsTable {
    /// Create a handle for an SSTable that has been persisted.
    pub fn new(descriptor: SsTableDescriptor) -> Self {
        Self { descriptor }
    }

    /// Access the descriptor metadata for this table.
    pub fn descriptor(&self) -> &SsTableDescriptor {
        &self.descriptor
    }
}

/// Describes an SSTable's identity and layout hints.
#[derive(Clone, Debug)]
pub struct SsTableDescriptor {
    id: SsTableId,
    level: usize,
    stats: Option<SsTableStats>,
    wal_ids: Option<Vec<FileId>>,
    data_path: Option<Path>,
    mvcc_path: Option<Path>,
    delete_path: Option<Path>,
}

impl SsTableDescriptor {
    /// Build a descriptor for a freshly created table.
    pub fn new(id: SsTableId, level: usize) -> Self {
        Self {
            id,
            level,
            stats: None,
            wal_ids: None,
            data_path: None,
            mvcc_path: None,
            delete_path: None,
        }
    }

    /// Attach rough statistics computed during flush.
    pub fn with_stats(mut self, stats: SsTableStats) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Attach the WAL IDs associated with this table.
    pub fn with_wal_ids(mut self, wal_ids: Option<Vec<FileId>>) -> Self {
        self.wal_ids = wal_ids;
        self
    }

    /// Attach the storage paths for the data and MVCC sidecar files.
    pub fn with_storage_paths(mut self, data_path: Path, delete_path: Option<Path>) -> Self {
        self.data_path = Some(data_path);
        self.delete_path = delete_path;
        self
    }

    /// Identifier for the table.
    pub fn id(&self) -> &SsTableId {
        &self.id
    }

    /// Compaction level currently associated with the table.
    pub fn level(&self) -> usize {
        self.level
    }

    /// Optional statistics bundle.
    pub fn stats(&self) -> Option<&SsTableStats> {
        self.stats.as_ref()
    }

    /// WAL file identifiers that contributed to this table.
    pub fn wal_ids(&self) -> Option<&[FileId]> {
        self.wal_ids.as_deref()
    }

    /// Relative path to the SSTable payload file.
    pub fn data_path(&self) -> Option<&Path> {
        self.data_path.as_ref()
    }

    /// Relative path to the MVCC sidecar file.
    pub fn mvcc_path(&self) -> Option<&Path> {
        self.mvcc_path.as_ref()
    }

    /// Relative path to the delete sidecar file, when present.
    pub fn delete_path(&self) -> Option<&Path> {
        self.delete_path.as_ref()
    }
}

/// Lightweight table statistics captured at flush time.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SsTableStats {
    /// Estimated logical row count written to the table.
    pub(crate) rows: usize,
    /// Approximate on-disk byte size of the table payload.
    pub(crate) bytes: usize,
    /// Number of tombstoned rows recorded in the table.
    pub(crate) tombstones: usize,
    /// Minimum key observed across staged segments.
    pub(crate) min_key: Option<KeyOwned>,
    /// Maximum key observed across staged segments.
    pub(crate) max_key: Option<KeyOwned>,
    /// Oldest commit timestamp contained in the table.
    pub(crate) min_commit_ts: Option<Timestamp>,
    /// Newest commit timestamp contained in the table.
    pub(crate) max_commit_ts: Option<Timestamp>,
}

impl PartialEq for SsTableStats {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

/// Error type shared across SSTable planning and IO.
#[derive(Debug, thiserror::Error)]
pub enum SsTableError {
    /// Placeholder variant for features not yet implemented.
    #[error("sstable operation has not been implemented yet")]
    Unimplemented,
    /// No immutable segments were available when attempting to flush.
    #[error("no immutable segments available for SSTable flush")]
    NoImmutableSegments,
    /// Attempted to use the writer after it was already closed.
    #[error("parquet writer already closed")]
    WriterClosed,
    /// Merge attempted without any input batches.
    #[error("sstable merge received no input batches")]
    EmptyMergeInput,
    /// Merge attempted to split outputs without an id allocator.
    #[error("sstable merge missing output id allocator for split outputs")]
    MissingIdAllocator,
    /// Merge produced no output rows or deletes.
    #[error("sstable merge produced no output")]
    EmptyMergeOutput,
    /// Filesystem operation failed while writing an SSTable.
    #[error("filesystem error: {0}")]
    Fs(#[from] FsError),
    /// Parquet writer failed while persisting an SSTable.
    #[error("parquet write error: {0}")]
    Parquet(#[from] ParquetError),
    /// Invalid path component produced while building an SSTable destination.
    #[error("invalid sstable path component: {0}")]
    InvalidPath(String),
    /// Sidecar/data batch streams were misaligned.
    #[error("sstable sidecar stream length mismatch: {0}")]
    SidecarMismatch(&'static str),
    /// Manifest backend error
    #[error("failure when persist into manifest: {0}")]
    Manifest(#[from] ManifestError),
    /// Missing key extractor when ordered merge is required.
    #[error("sstable key extractor not configured")]
    MissingKeyExtractor,
    /// Key projection failed while merging.
    #[error("key extraction failed: {0}")]
    KeyExtract(#[from] KeyExtractError),
    /// Key materialization failed while merging.
    #[error("key materialization failed: {0}")]
    KeyOwned(#[from] KeyOwnedError),
    /// Merge exceeded the configured iteration budget.
    #[error("merge iteration budget {budget} exceeded")]
    MergeIterationBudgetExceeded {
        /// Configured iteration budget that was exceeded.
        budget: usize,
    },
}

/// Scratchpad used while minor compaction plans an SST. Keeps provisional stats
/// that may not map 1:1 onto the persisted descriptor yet.
pub(crate) struct StagedTableStats {
    pub segments: usize,
    pub rows: usize,
    pub tombstones: usize,
    pub min_key: Option<KeyOwned>,
    pub max_key: Option<KeyOwned>,
    pub min_commit_ts: Option<Timestamp>,
    pub max_commit_ts: Option<Timestamp>,
}

impl StagedTableStats {
    fn new() -> Self {
        Self {
            segments: 0,
            rows: 0,
            tombstones: 0,
            min_key: None,
            max_key: None,
            min_commit_ts: None,
            max_commit_ts: None,
        }
    }

    fn update_key_bounds(&mut self, seg_min: &KeyOwned, seg_max: &KeyOwned) {
        match self.min_key {
            Some(ref existing) if existing <= seg_min => {}
            _ => self.min_key = Some(seg_min.clone()),
        }
        match self.max_key {
            Some(ref existing) if existing >= seg_max => {}
            _ => self.max_key = Some(seg_max.clone()),
        }
    }
}

impl StagedTableStats {
    fn update_commit_bounds(&mut self, seg_min: Timestamp, seg_max: Timestamp) {
        match self.min_commit_ts {
            Some(curr) if curr <= seg_min => {}
            _ => self.min_commit_ts = Some(seg_min),
        }
        match self.max_commit_ts {
            Some(curr) if curr >= seg_max => {}
            _ => self.max_commit_ts = Some(seg_max),
        }
    }
}

impl fmt::Debug for StagedTableStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StagedTableStats")
            .field("segments", &self.segments)
            .field("rows", &self.rows)
            .field("tombstones", &self.tombstones)
            .field("has_min_key", &self.min_key.is_some())
            .field("has_max_key", &self.max_key.is_some())
            .field("min_commit_ts", &self.min_commit_ts)
            .field("max_commit_ts", &self.max_commit_ts)
            .finish()
    }
}

impl SsTableStats {
    fn from_staged(staged: StagedTableStats, bytes: usize) -> Self {
        Self {
            rows: staged.rows,
            bytes,
            tombstones: staged.tombstones,
            min_key: staged.min_key,
            max_key: staged.max_key,
            min_commit_ts: staged.min_commit_ts,
            max_commit_ts: staged.max_commit_ts,
        }
    }
}

/// Parquet writer facade that streams staged immutables into an SST file.
#[derive(Clone, Debug)]
struct StagedSegment {
    data_with_commit: RecordBatch,
    deletes: DeleteSidecar,
}

pub(crate) struct ParquetTableWriter {
    config: Arc<SsTableConfig>,
    descriptor: SsTableDescriptor,
    staged: StagedTableStats,
    segments: Vec<StagedSegment>,
    wal_ids: Option<Vec<FileId>>,
}

impl ParquetTableWriter {
    pub(crate) fn new(config: Arc<SsTableConfig>, descriptor: SsTableDescriptor) -> Self {
        Self {
            config,
            descriptor,
            staged: StagedTableStats::new(),
            segments: Vec::new(),
            wal_ids: None,
        }
    }

    pub(crate) fn set_wal_ids(&mut self, wal_ids: Option<Vec<FileId>>) {
        self.wal_ids = wal_ids;
    }

    pub(crate) fn stage_immutable(
        &mut self,
        segment: &ImmutableSegment,
    ) -> Result<(), SsTableError> {
        let data_rows = segment.storage().num_rows();
        let delete_rows = segment.delete_sidecar().len();
        if data_rows == 0 && delete_rows == 0 {
            return Ok(());
        }

        let mut segment_tombstones = 0usize;
        let mut seg_min_ts: Option<Timestamp> = None;
        let mut seg_max_ts: Option<Timestamp> = None;
        for entry in segment.row_iter() {
            if entry.tombstone {
                segment_tombstones += 1;
            }
            seg_min_ts = Some(match seg_min_ts {
                Some(current) if current <= entry.commit_ts => current,
                _ => entry.commit_ts,
            });
            seg_max_ts = Some(match seg_max_ts {
                Some(current) if current >= entry.commit_ts => current,
                _ => entry.commit_ts,
            });
        }

        debug_assert!(
            seg_min_ts.is_some() && seg_max_ts.is_some(),
            "immutable segment with rows must yield commit timestamp bounds"
        );

        if let (Some(min_key), Some(max_key)) = (segment.min_key(), segment.max_key()) {
            self.staged.update_key_bounds(&min_key, &max_key);
        }

        if let (Some(min_ts), Some(max_ts)) = (seg_min_ts, seg_max_ts) {
            self.staged.update_commit_bounds(min_ts, max_ts);
        }

        self.staged.segments += 1;
        self.staged.rows += data_rows;
        self.staged.tombstones += segment_tombstones;
        let mvcc = segment.mvcc_columns().clone();
        let data_with_commit = append_commit_column(segment.storage().clone(), &mvcc)?;
        let staged_segment = StagedSegment {
            data_with_commit,
            deletes: segment.delete_sidecar().clone(),
        };
        self.segments.push(staged_segment);
        Ok(())
    }

    pub(crate) fn stage_stream_batch(
        &mut self,
        batch: &SsTableStreamBatch,
        extractor: &dyn KeyProjection,
    ) -> Result<(), SsTableError> {
        let data_rows = batch.data.num_rows();
        let delete_rows = batch.delete.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        if data_rows == 0 && delete_rows == 0 {
            return Ok(());
        }

        let mut segment_tombstones = 0usize;
        let mut seg_min_ts = None;
        let mut seg_max_ts = None;

        if data_rows > 0 {
            let first = extract_key_at(&batch.data, extractor, 0)?;
            let last = extract_key_at(&batch.data, extractor, data_rows - 1)?;
            self.staged.update_key_bounds(&first, &last);
            let commits = commit_ts_column(&batch.data)?;
            for idx in 0..data_rows {
                let ts = Timestamp::new(commits.value(idx));
                seg_min_ts = Some(seg_min_ts.map(|t| std::cmp::min(t, ts)).unwrap_or(ts));
                seg_max_ts = Some(seg_max_ts.map(|t| std::cmp::max(t, ts)).unwrap_or(ts));
            }
        }

        if let Some(delete_batch) = &batch.delete
            && delete_batch.num_rows() > 0
        {
            segment_tombstones += delete_batch.num_rows();
            let first = extract_delete_key_at(delete_batch, extractor, 0)?;
            let last = extract_delete_key_at(delete_batch, extractor, delete_batch.num_rows() - 1)?;
            self.staged.update_key_bounds(&first, &last);

            let commits = delete_batch
                .column_by_name(MVCC_COMMIT_COL)
                .and_then(|arr| arr.as_any().downcast_ref::<UInt64Array>())
                .ok_or(SsTableError::SidecarMismatch(
                    "delete sidecar missing commit_ts column",
                ))?;
            for idx in 0..delete_batch.num_rows() {
                let ts = Timestamp::new(commits.value(idx));
                seg_min_ts = Some(seg_min_ts.map(|t| std::cmp::min(t, ts)).unwrap_or(ts));
                seg_max_ts = Some(seg_max_ts.map(|t| std::cmp::max(t, ts)).unwrap_or(ts));
            }
        }

        if let (Some(min_ts), Some(max_ts)) = (seg_min_ts, seg_max_ts) {
            self.staged.update_commit_bounds(min_ts, max_ts);
        }

        self.staged.segments += 1;
        self.staged.rows += data_rows;
        self.staged.tombstones += segment_tombstones;

        parse_mvcc_columns_from_data(&batch.data)?;
        let deletes = if let Some(delete_batch) = &batch.delete {
            decode_delete_sidecar(delete_batch, extractor)?
        } else {
            DeleteSidecar::empty(&extractor.key_schema())
        };

        // Data already carries the `_commit_ts` column when coming from an SST stream.
        let staged_segment = StagedSegment {
            data_with_commit: batch.data.clone(),
            deletes,
        };
        self.segments.push(staged_segment);
        Ok(())
    }

    pub(crate) async fn finish(self) -> Result<SsTable, SsTableError> {
        if self.staged.segments == 0 {
            return Err(SsTableError::NoImmutableSegments);
        }

        let mut ctx = WriteContext::new(Arc::clone(&self.config), &self.descriptor).await?;
        for segment in &self.segments {
            ctx.write_segment(segment).await?;
        }
        let (data_path, delete_path, data_bytes) = ctx.finish().await?;
        let stats = SsTableStats::from_staged(
            self.staged,
            usize::try_from(data_bytes).unwrap_or(usize::MAX),
        );
        Ok(SsTable::new(
            self.descriptor
                .with_stats(stats)
                .with_storage_paths(data_path, delete_path)
                .with_wal_ids(self.wal_ids),
        ))
    }

    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn plan(&self) -> &StagedTableStats {
        &self.staged
    }
}

fn writer_properties(compression: SsTableCompression) -> WriterProperties {
    let builder = match compression {
        SsTableCompression::None => {
            WriterProperties::builder().set_compression(Compression::UNCOMPRESSED)
        }
        SsTableCompression::Zstd => {
            WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::default()))
        }
    };
    builder.build()
}

/// Append the `_commit_ts` column to a record batch for persistence.
fn append_commit_column(
    batch: RecordBatch,
    mvcc: &MvccColumns,
) -> Result<RecordBatch, SsTableError> {
    if mvcc.commit_ts.len() != batch.num_rows() {
        return Err(SsTableError::SidecarMismatch(
            "commit_ts length mismatch data rows",
        ));
    }
    let commit_values = UInt64Array::from_iter_values(mvcc.commit_ts.iter().map(|ts| ts.get()));
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Field::new(MVCC_COMMIT_COL, DataType::UInt64, false).into());
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(commit_values) as ArrayRef);
    let new_schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|err| SsTableError::Parquet(ParquetError::ArrowError(err.to_string())))
}

fn commit_ts_column(batch: &RecordBatch) -> Result<Arc<UInt64Array>, SsTableError> {
    batch
        .column_by_name(MVCC_COMMIT_COL)
        .and_then(|arr| arr.as_any().downcast_ref::<UInt64Array>())
        .map(|col| Arc::new(col.clone()))
        .ok_or(SsTableError::SidecarMismatch(
            "commit_ts column missing or wrong type",
        ))
}

/// Reconstruct MVCC metadata from a data batch that already carries `_commit_ts`.
fn parse_mvcc_columns_from_data(batch: &RecordBatch) -> Result<MvccColumns, SsTableError> {
    let commit_col = commit_ts_column(batch)?;
    if commit_col.null_count() > 0 {
        return Err(SsTableError::SidecarMismatch("commit_ts contains nulls"));
    }
    let commits: Vec<Timestamp> = commit_col
        .values()
        .iter()
        .map(|v| Timestamp::new(*v))
        .collect();
    let tombstone = vec![false; commits.len()];
    Ok(MvccColumns::new(commits, tombstone))
}

struct WriteContext {
    fs: Arc<dyn DynFs>,
    data_path: Path,
    delete_path: Path,
    data_writer: Option<AsyncArrowWriter<AsyncWriter<NoopExecutor>>>,
    delete_writer: Option<AsyncArrowWriter<AsyncWriter<NoopExecutor>>>,
    delete_written: bool,
    compression: SsTableCompression,
    executor: NoopExecutor,
}
impl WriteContext {
    async fn new(
        config: Arc<SsTableConfig>,
        descriptor: &SsTableDescriptor,
    ) -> Result<Self, SsTableError> {
        let fs = Arc::clone(config.fs());
        let (dir_path, data_path, delete_path) =
            build_table_paths(&config, descriptor.level(), descriptor.id())?;
        fs.create_dir_all(&dir_path).await?;

        let data_options = OpenOptions::default()
            .create(true)
            .write(true)
            .truncate(true);
        let data_file = fs.open_options(&data_path, data_options).await?;

        // Extend user schema with commit_ts for on-disk persistence, preserving metadata.
        let mut fields = config.schema().fields().to_vec();
        fields.push(Field::new(MVCC_COMMIT_COL, DataType::UInt64, false).into());
        let data_schema = Arc::new(Schema::new_with_metadata(
            fields,
            config.schema().metadata().clone(),
        ));

        let executor = NoopExecutor;

        let data_writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(data_file, executor),
            data_schema,
            Some(writer_properties(config.compression())),
        )?;

        Ok(Self {
            fs,
            data_path,
            data_writer: Some(data_writer),
            delete_path,
            delete_writer: None,
            delete_written: false,
            compression: config.compression(),
            executor,
        })
    }

    async fn write_segment(&mut self, segment: &StagedSegment) -> Result<(), SsTableError> {
        if let Some(writer) = self.data_writer.as_mut() {
            writer.write(&segment.data_with_commit).await?;
        } else {
            return Err(SsTableError::WriterClosed);
        }
        if !segment.deletes.is_empty() {
            let delete_batch = segment
                .deletes
                .to_record_batch()
                .map_err(|err| SsTableError::Parquet(ParquetError::ArrowError(err.to_string())))?;
            self.write_delete_batch(delete_batch).await?;
        }
        Ok(())
    }

    async fn write_delete_batch(&mut self, batch: RecordBatch) -> Result<(), SsTableError> {
        if self.delete_writer.is_none() {
            let options = OpenOptions::default()
                .create(true)
                .write(true)
                .truncate(true);
            let file = self.fs.open_options(&self.delete_path, options).await?;
            let writer = AsyncArrowWriter::try_new(
                AsyncWriter::new(file, self.executor),
                batch.schema(),
                Some(writer_properties(self.compression)),
            )?;
            self.delete_writer = Some(writer);
        }
        if let Some(writer) = self.delete_writer.as_mut() {
            writer.write(&batch).await?;
            self.delete_written = true;
            Ok(())
        } else {
            Err(SsTableError::WriterClosed)
        }
    }

    async fn finish(mut self) -> Result<(Path, Option<Path>, u64), SsTableError> {
        if let Some(writer) = self.data_writer.take() {
            writer.close().await?;
        }

        let data_path = self.data_path.clone();
        if let Some(writer) = self.delete_writer.take() {
            writer.close().await?;
        }

        let data_file = self
            .fs
            .open_options(&data_path, OpenOptions::default().read(true))
            .await?;
        let data_bytes = data_file.size().await?;

        let delete_path = if self.delete_written {
            Some(self.delete_path.clone())
        } else {
            None
        };

        Ok((data_path, delete_path, data_bytes))
    }
}

impl Drop for WriteContext {
    fn drop(&mut self) {
        debug_assert!(
            self.data_writer.is_none() && self.delete_writer.is_none(),
            "WriteContext dropped without closing writers"
        );
    }
}

pub(crate) fn build_table_paths(
    config: &SsTableConfig,
    level: usize,
    id: &SsTableId,
) -> Result<(Path, Path, Path), SsTableError> {
    let dir_path = config.level_dir(level)?;

    let data_file_name = format!("{:020}.parquet", id.raw());
    let data_part = PathPart::parse(&data_file_name)
        .map_err(|err| SsTableError::InvalidPath(err.to_string()))?;
    let data_path = dir_path.child(data_part);

    let delete_file_name = format!("{:020}.delete.parquet", id.raw());
    let delete_part = PathPart::parse(&delete_file_name)
        .map_err(|err| SsTableError::InvalidPath(err.to_string()))?;
    let delete_path = dir_path.child(delete_part);

    Ok((dir_path, data_path, delete_path))
}

/// Planner responsible for turning immutable runs into SSTable files.
pub struct SsTableBuilder {
    writer: ParquetTableWriter,
}
impl SsTableBuilder {
    /// Create a new builder for the provided descriptor.
    pub fn new(config: Arc<SsTableConfig>, descriptor: SsTableDescriptor) -> Self {
        Self {
            writer: ParquetTableWriter::new(config, descriptor),
        }
    }
    /// Attach WAL identifiers derived from the staged immutables.
    pub fn set_wal_ids(&mut self, wal_ids: Option<Vec<FileId>>) {
        self.writer.set_wal_ids(wal_ids);
    }
    /// Stage an immutable run emitted by the mutable layer.
    pub(crate) fn add_immutable(&mut self, segment: &ImmutableSegment) -> Result<(), SsTableError> {
        self.writer.stage_immutable(segment)
    }
    /// Finalize the staged runs and flush them to durable storage.
    pub async fn finish(self) -> Result<SsTable, SsTableError> {
        self.writer.finish().await
    }
}

/// Handle used by the read path to stream rows from an SSTable.
#[derive(Debug)]
pub struct SsTableReader {
    descriptor: SsTableDescriptor,

    config: Arc<SsTableConfig>,
    // TODO: attach actual Parquet readers when scan/merge lands.
}

impl SsTableReader {
    /// Open an SSTable using the supplied descriptor and shared configuration.
    pub async fn open(
        config: Arc<SsTableConfig>,
        descriptor: SsTableDescriptor,
    ) -> Result<Self, SsTableError> {
        Ok(Self { descriptor, config })
    }

    /// Plan a range scan across this table.
    #[allow(dead_code)] // API surface for future use
    pub(crate) fn plan_scan(
        &self,
        _ts: Timestamp,
        _predicate: Option<&Predicate>,
    ) -> Result<(), SsTableError> {
        // TODO: implement real scan planning when reader lands.
        Ok(())
    }

    /// Stream all rows for the provided ranges/timestamp/predicate (stub).
    pub(crate) async fn into_stream(
        self,
        _ts: Timestamp,
        _predicate: Option<&Predicate>,
    ) -> Result<BoxStream<'static, Result<SsTableStreamBatch, SsTableError>>, SsTableError> {
        let fs = Arc::clone(self.config.fs());
        let data_path =
            self.descriptor.data_path().cloned().ok_or_else(|| {
                SsTableError::InvalidPath("missing data path on descriptor".into())
            })?;
        let data_stream = Box::pin(open_parquet_stream(fs.clone(), data_path, None).await?);
        let delete_stream = if let Some(path) = self.descriptor.delete_path() {
            Some(Box::pin(open_parquet_stream(fs, path.clone(), None).await?))
        } else {
            None
        };

        let schema = self.config.schema().clone();
        let stream = stream::try_unfold(
            (data_stream, delete_stream),
            move |(mut data_stream, mut delete_stream)| {
                let schema = schema.clone();
                async move {
                    let data_batch = data_stream
                        .as_mut()
                        .next()
                        .await
                        .transpose()
                        .map_err(SsTableError::Parquet)?;
                    let delete_batch = if let Some(ref mut del_stream) = delete_stream {
                        del_stream
                            .as_mut()
                            .next()
                            .await
                            .transpose()
                            .map_err(SsTableError::Parquet)?
                    } else {
                        None
                    };

                    match (data_batch, delete_batch) {
                        (None, None) => Ok(None),
                        (Some(data), delete) => {
                            let next_state = (data_stream, delete_stream);
                            Ok(Some((SsTableStreamBatch { data, delete }, next_state)))
                        }
                        (None, Some(delete)) => {
                            let empty_data = RecordBatch::new_empty(schema.clone());
                            let next_state = (data_stream, delete_stream);
                            Ok(Some((
                                SsTableStreamBatch {
                                    data: empty_data,
                                    delete: Some(delete),
                                },
                                next_state,
                            )))
                        }
                    }
                }
            },
        );

        Ok(Box::pin(stream))
    }
}

pub(crate) async fn open_parquet_stream(
    fs: Arc<dyn DynFs>,
    path: Path,
    projection: Option<ProjectionMask>,
) -> Result<ParquetRecordBatchStream<AsyncReader<NoopExecutor>>, SsTableError> {
    let (stream, _schema) = open_parquet_stream_with_schema(fs, path, projection).await?;
    Ok(stream)
}

/// Open a Parquet file as an async record batch stream, returning both the stream and
/// the Arrow schema derived from the Parquet metadata.
pub(crate) async fn open_parquet_stream_with_schema(
    fs: Arc<dyn DynFs>,
    path: Path,
    projection: Option<ProjectionMask>,
) -> Result<
    (
        ParquetRecordBatchStream<AsyncReader<NoopExecutor>>,
        SchemaRef,
    ),
    SsTableError,
> {
    let file = fs.open(&path).await?;
    let size = file.size().await.map_err(SsTableError::Fs)?;
    let reader = AsyncReader::new(file, size, NoopExecutor)
        .await
        .map_err(SsTableError::Fs)?;
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(SsTableError::Parquet)?;
    let schema = builder.schema().clone();
    let mask = projection.unwrap_or_else(ProjectionMask::all);
    builder = builder.with_projection(mask);
    let stream = builder.build().map_err(SsTableError::Parquet)?;
    Ok((stream, schema))
}

pub(crate) fn take_record_batch(
    batch: &RecordBatch,
    indices: &[u32],
) -> Result<RecordBatch, SsTableError> {
    let idx_array = UInt32Array::from(indices.to_vec());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col in batch.columns() {
        let taken = arrow_take(col.as_ref(), &idx_array, None)
            .map_err(|err| SsTableError::Parquet(ParquetError::ArrowError(err.to_string())))?;
        columns.push(taken);
    }
    RecordBatch::try_new(batch.schema().clone(), columns)
        .map_err(|err| SsTableError::Parquet(ParquetError::ArrowError(err.to_string())))
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, atomic::AtomicU64},
    };

    use arrow_schema::{DataType, Field, Schema};
    use fusio::{disk::LocalFs, dynamic::DynFs, path::Path};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        extractor::{KeyProjection, projection_for_field},
        id::FileIdGenerator,
        inmem::immutable::memtable::{ImmutableIndexEntry, ImmutableMemTable, bundle_mvcc_sidecar},
        key::KeyTsViewRaw,
        mvcc::Timestamp,
        test::build_batch,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn merge_source_yields_batches_in_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch1 = build_batch(
            Arc::clone(&schema),
            vec![DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .expect("batch1");
        let batch2 = build_batch(
            Arc::clone(&schema),
            vec![DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I32(2)),
            ])],
        )
        .expect("batch2");

        let mvcc1 = MvccColumns::new(vec![Timestamp::MIN], vec![false]);
        let mvcc2 = MvccColumns::new(vec![Timestamp::MIN], vec![false]);
        let batch1 = append_commit_column(batch1, &mvcc1).expect("commit");
        let batch2 = append_commit_column(batch2, &mvcc2).expect("commit");

        let mut source = SsTableMergeSource::with_batches(vec![
            SsTableStreamBatch {
                data: batch1.clone(),
                delete: None,
            },
            SsTableStreamBatch {
                data: batch2.clone(),
                delete: None,
            },
        ]);
        let out1 = source.next().await.expect("first").expect("batch");
        let out2 = source.next().await.expect("second").expect("batch");
        let out3 = source.next().await.expect("third");

        assert_eq!(out1.data.num_rows(), 1);
        assert_eq!(out2.data.num_rows(), 1);
        assert!(out3.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn merger_combines_stats_and_wal_ids() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let tempdir = tempfile::tempdir().expect("tempdir");
        let root = Path::from(tempdir.path().to_string_lossy().to_string());
        let extractor = projection_for_field(Arc::clone(&schema), 0).expect("extractor");
        let extractor: Arc<dyn KeyProjection> = extractor.into();
        let config = Arc::new(
            SsTableConfig::new(Arc::clone(&schema), fs, root).with_key_extractor(extractor),
        );

        let seg_a = sample_segment(
            vec![("a".to_string(), 1), ("b".to_string(), 2)],
            vec![1, 2],
            vec![false, false],
        );
        let seg_b = sample_segment(
            vec![("c".to_string(), 3), ("d".to_string(), 0)],
            vec![3, 4],
            vec![false, true],
        );

        let wal_a = vec![FileIdGenerator::default().generate()];
        let wal_b = vec![FileIdGenerator::default().generate(), wal_a[0]]; // include duplicate to ensure dedup

        let mut builder_a = SsTableBuilder::new(
            Arc::clone(&config),
            SsTableDescriptor::new(SsTableId::new(1), 0),
        );
        builder_a.set_wal_ids(Some(wal_a.clone()));
        builder_a.add_immutable(&seg_a).expect("stage a");
        let input_a = builder_a
            .finish()
            .await
            .expect("sst a")
            .descriptor()
            .clone();

        let mut builder_b = SsTableBuilder::new(
            Arc::clone(&config),
            SsTableDescriptor::new(SsTableId::new(2), 0),
        );
        builder_b.set_wal_ids(Some(wal_b.clone()));
        builder_b.add_immutable(&seg_b).expect("stage b");
        let input_b = builder_b
            .finish()
            .await
            .expect("sst b")
            .descriptor()
            .clone();

        let target = SsTableDescriptor::new(SsTableId::new(9), 1);
        let merger = SsTableMerger::new(config, vec![input_a, input_b], target)
            .with_output_id_allocator(Arc::new(AtomicU64::new(10)));
        let merged = merger.execute().await.expect("merge result");
        assert_eq!(merged.len(), 1);
        let descriptor = merged[0].descriptor();
        let merged_stats = descriptor.stats().expect("merged stats");

        assert_eq!(descriptor.id().raw(), 9);
        assert_eq!(descriptor.level(), 1);
        assert_eq!(merged_stats.rows, 3);
        assert_eq!(merged_stats.tombstones, 1);
        assert_eq!(
            merged_stats
                .min_key
                .as_ref()
                .map(|k| k.as_utf8().expect("min key utf8")),
            Some("a")
        );
        assert_eq!(
            merged_stats
                .max_key
                .as_ref()
                .map(|k| k.as_utf8().expect("max key utf8")),
            Some("d")
        );
        assert_eq!(merged_stats.min_commit_ts, Some(Timestamp::new(1)));
        assert_eq!(merged_stats.max_commit_ts, Some(Timestamp::new(4)));

        let wal_ids = descriptor.wal_ids().expect("wal ids");
        assert_eq!(wal_ids.len(), 2); // duplicate deduped
        assert!(wal_ids.contains(&wal_a[0]));
        assert!(wal_ids.contains(&wal_b[0]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn merger_applies_latest_wins_and_deletes() {
        use arrow_array::{Int32Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let tempdir = tempfile::tempdir().expect("tempdir");
        let root = Path::from(tempdir.path().to_string_lossy().to_string());
        let extractor = projection_for_field(Arc::clone(&schema), 0).expect("extractor");
        let extractor: Arc<dyn KeyProjection> = extractor.into();
        let config = Arc::new(
            SsTableConfig::new(Arc::clone(&schema), fs, root).with_key_extractor(extractor),
        );

        // Older data: k1=1@10, k2=2@20
        let seg_old = sample_segment(
            vec![("k1".to_string(), 1), ("k2".to_string(), 2)],
            vec![10, 20],
            vec![false, false],
        );
        // Newer segment: tombstone k1@30, updated k2=3@40
        let seg_new = sample_segment(
            vec![("k1".to_string(), 0), ("k2".to_string(), 3)],
            vec![30, 40],
            vec![true, false],
        );

        let mut builder_old = SsTableBuilder::new(
            Arc::clone(&config),
            SsTableDescriptor::new(SsTableId::new(1), 0),
        );
        builder_old.add_immutable(&seg_old).expect("stage old");
        let input_old = builder_old
            .finish()
            .await
            .expect("sst old")
            .descriptor()
            .clone();

        let mut builder_new = SsTableBuilder::new(
            Arc::clone(&config),
            SsTableDescriptor::new(SsTableId::new(2), 0),
        );
        builder_new.add_immutable(&seg_new).expect("stage new");
        let input_new = builder_new
            .finish()
            .await
            .expect("sst new")
            .descriptor()
            .clone();

        let target = SsTableDescriptor::new(SsTableId::new(9), 1);
        let merger = SsTableMerger::new(config.clone(), vec![input_old, input_new], target)
            .with_output_id_allocator(Arc::new(AtomicU64::new(10)));
        let merged = merger.execute().await.expect("merge result");
        assert_eq!(merged.len(), 1);
        let descriptor = merged[0].descriptor();
        let stats = descriptor.stats().expect("stats");
        assert_eq!(stats.rows, 1);
        assert_eq!(stats.tombstones, 1);
        assert_eq!(
            stats.min_key.as_ref().expect("min key").as_utf8(),
            Some("k1")
        );
        assert_eq!(
            stats.max_key.as_ref().expect("max key").as_utf8(),
            Some("k2")
        );

        // Read merged output and validate visible rows/tombstones.
        let reader = SsTableReader::open(config, descriptor.clone())
            .await
            .expect("reader");
        let mut stream = reader
            .into_stream(Timestamp::MAX, None)
            .await
            .expect("stream");
        let mut data_keys = Vec::new();
        let mut data_vals = Vec::new();
        let mut delete_keys = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch ok");
            if batch.data.num_rows() > 0 {
                let ids = batch
                    .data
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("string ids");
                let vals = batch
                    .data
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int vals");
                for i in 0..batch.data.num_rows() {
                    data_keys.push(ids.value(i).to_string());
                    data_vals.push(vals.value(i));
                }
            }
            if let Some(delete) = batch.delete.as_ref() {
                if delete.num_rows() > 0 {
                    let ids = delete
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("string delete ids");
                    for i in 0..delete.num_rows() {
                        delete_keys.push(ids.value(i).to_string());
                    }
                }
            }
        }
        assert_eq!(data_keys, vec!["k2"]);
        assert_eq!(data_vals, vec![3]);
        assert_eq!(delete_keys, vec!["k1"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn merger_splits_outputs_by_row_cap() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let tempdir = tempfile::tempdir().expect("tempdir");
        let root = Path::from(tempdir.path().to_string_lossy().to_string());
        let extractor = projection_for_field(Arc::clone(&schema), 0).expect("extractor");
        let extractor: Arc<dyn KeyProjection> = extractor.into();
        let config = Arc::new(
            SsTableConfig::new(Arc::clone(&schema), fs, root).with_key_extractor(extractor),
        );

        let seg_a = sample_segment(
            vec![("a".to_string(), 1), ("b".to_string(), 2)],
            vec![1, 2],
            vec![false, false],
        );
        let seg_b = sample_segment(vec![("c".to_string(), 3)], vec![3], vec![false]);

        let mut builder_a = SsTableBuilder::new(
            Arc::clone(&config),
            SsTableDescriptor::new(SsTableId::new(1), 0),
        );
        builder_a.add_immutable(&seg_a).expect("stage a");
        let input_a = builder_a
            .finish()
            .await
            .expect("sst a")
            .descriptor()
            .clone();

        let mut builder_b = SsTableBuilder::new(
            Arc::clone(&config),
            SsTableDescriptor::new(SsTableId::new(2), 0),
        );
        builder_b.add_immutable(&seg_b).expect("stage b");
        let input_b = builder_b
            .finish()
            .await
            .expect("sst b")
            .descriptor()
            .clone();

        let target = SsTableDescriptor::new(SsTableId::new(9), 1);
        let outputs = SsTableMerger::new(config, vec![input_a, input_b], target)
            .with_output_id_allocator(Arc::new(AtomicU64::new(10)))
            .with_output_caps(Some(1), None)
            .with_chunk_rows(1)
            .execute()
            .await
            .expect("merge result");

        assert_eq!(outputs.len(), 3);
        for table in outputs {
            let stats = table.descriptor().stats().expect("stats");
            assert!(stats.rows <= 1);
        }
    }

    fn test_config(schema: SchemaRef) -> Arc<SsTableConfig> {
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        Arc::new(SsTableConfig::new(
            schema,
            fs,
            Path::from("/tmp/tonbo-test"),
        ))
    }

    fn sample_segment(
        rows: Vec<(String, i32)>,
        commits: Vec<u64>,
        tombstones: Vec<bool>,
    ) -> ImmutableMemTable<arrow_array::RecordBatch> {
        assert_eq!(rows.len(), commits.len());
        assert_eq!(rows.len(), tombstones.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let mut data_rows = Vec::new();
        let mut data_commits = Vec::new();
        let mut delete_rows = Vec::new();
        let mut delete_commits = Vec::new();
        for ((key, value), (commit, tombstone)) in rows
            .into_iter()
            .zip(commits.into_iter().zip(tombstones.into_iter()))
        {
            let ts = Timestamp::new(commit);
            if tombstone {
                delete_rows.push(DynRow(vec![Some(DynCell::Str(key.into()))]));
                delete_commits.push(ts);
            } else {
                data_rows.push(DynRow(vec![
                    Some(DynCell::Str(key.into())),
                    Some(DynCell::I32(value)),
                ]));
                data_commits.push(ts);
            }
        }

        let batch = if data_rows.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            build_batch(schema.clone(), data_rows).expect("record batch")
        };
        let tombstone_flags = vec![false; batch.num_rows()];
        let (batch, mvcc) =
            bundle_mvcc_sidecar(batch, data_commits.clone(), tombstone_flags).expect("mvcc");

        let delete_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let delete_batch = if delete_rows.is_empty() {
            RecordBatch::new_empty(delete_schema.clone())
        } else {
            build_batch(delete_schema.clone(), delete_rows).expect("delete batch")
        };
        let delete_sidecar = DeleteSidecar::new(delete_batch, delete_commits);

        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let mut composite = BTreeMap::new();
        let row_indices: Vec<usize> = (0..batch.num_rows()).collect();
        let key_rows = extractor
            .project_view(&batch, &row_indices)
            .expect("project view");
        for (row, key_row) in key_rows.into_iter().enumerate() {
            composite.insert(
                KeyTsViewRaw::new(key_row, mvcc.commit_ts[row]),
                ImmutableIndexEntry::Row(row as u32),
            );
        }

        if !delete_sidecar.is_empty() {
            let delete_schema = delete_sidecar.key_batch().schema().clone();
            let indices: Vec<usize> = (0..delete_schema.fields().len()).collect();
            let projection =
                crate::extractor::projection_for_columns(delete_schema.clone(), indices)
                    .expect("identity projection");
            let delete_row_indices: Vec<usize> =
                (0..delete_sidecar.key_batch().num_rows()).collect();
            let delete_key_rows = projection
                .project_view(delete_sidecar.key_batch(), &delete_row_indices)
                .expect("delete keys");
            for (row, key_row) in delete_key_rows.into_iter().enumerate() {
                let ts = delete_sidecar.commit_ts(row);
                composite.insert(KeyTsViewRaw::new(key_row, ts), ImmutableIndexEntry::Delete);
            }
        }

        ImmutableMemTable::new(batch, composite, mvcc, delete_sidecar)
    }

    #[test]
    fn parquet_writer_accumulates_segment_stats() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let descriptor = SsTableDescriptor::new(SsTableId::new(7), 0);
        let mut writer: ParquetTableWriter =
            ParquetTableWriter::new(test_config(schema.clone()), descriptor);

        let segment1 = sample_segment(
            vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3)],
            vec![30, 20, 10],
            vec![false, true, false],
        );
        writer.stage_immutable(&segment1).expect("stage first");

        let segment2 = sample_segment(
            vec![("d".into(), 4), ("e".into(), 5)],
            vec![25, 35],
            vec![false, false],
        );
        writer.stage_immutable(&segment2).expect("stage second");

        let plan = writer.plan();
        assert_eq!(plan.segments, 2);
        assert_eq!(plan.rows, 4);
        assert_eq!(plan.tombstones, 1);
        assert_eq!(plan.min_commit_ts, Some(Timestamp::new(10)));
        assert_eq!(plan.max_commit_ts, Some(Timestamp::new(35)));

        let min_key = plan
            .min_key
            .as_ref()
            .and_then(|k| k.as_utf8())
            .expect("min key");
        let max_key = plan
            .max_key
            .as_ref()
            .and_then(|k| k.as_utf8())
            .expect("max key");
        assert_eq!(min_key, "a");
        assert_eq!(max_key, "e");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finish_without_segments_errors() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let descriptor = SsTableDescriptor::new(SsTableId::new(1), 0);
        let writer: ParquetTableWriter = ParquetTableWriter::new(test_config(schema), descriptor);
        let result = writer.finish().await;
        assert!(matches!(result, Err(SsTableError::NoImmutableSegments)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finish_threads_wal_ids_into_descriptor() {
        use std::str::FromStr;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let descriptor = SsTableDescriptor::new(SsTableId::new(11), 0);
        let mut writer: ParquetTableWriter =
            ParquetTableWriter::new(test_config(schema.clone()), descriptor);

        let wal_ids = vec![FileId::from_str("01HV6Z2Z8Q4W5X6Y7Z8A9BCDEF").expect("valid ulid")];
        writer.set_wal_ids(Some(wal_ids.clone()));

        let segment = sample_segment(vec![("k".into(), 1)], vec![42], vec![false]);
        writer.stage_immutable(&segment).expect("stage segment");

        let table = writer.finish().await.expect("finish");
        let recorded = table.descriptor().wal_ids().expect("descriptor wal ids");
        assert_eq!(recorded, wal_ids.as_slice());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finish_records_data_and_sidecar_paths() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let descriptor = SsTableDescriptor::new(SsTableId::new(21), 0);
        let mut writer: ParquetTableWriter =
            ParquetTableWriter::new(test_config(schema.clone()), descriptor);

        let segment = sample_segment(vec![("m".into(), 9)], vec![123], vec![false]);
        writer.stage_immutable(&segment).expect("stage segment");

        let table = writer.finish().await.expect("finish table");
        let descriptor = table.descriptor();
        let data_path = descriptor.data_path().expect("data path present");
        assert!(data_path.as_ref().ends_with(".parquet"));
        assert!(descriptor.delete_path().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finish_records_delete_sidecar_when_tombstones_exist() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let descriptor = SsTableDescriptor::new(SsTableId::new(22), 0);
        let mut writer: ParquetTableWriter =
            ParquetTableWriter::new(test_config(schema.clone()), descriptor);

        let segment = sample_segment(
            vec![("z".into(), 1), ("tomb".into(), 0)],
            vec![100, 200],
            vec![false, true],
        );
        writer.stage_immutable(&segment).expect("stage segment");

        let table = writer.finish().await.expect("finish table");
        let descriptor = table.descriptor();
        let delete_path = descriptor.delete_path().expect("delete sidecar present");
        assert!(delete_path.as_ref().ends_with(".delete.parquet"));
    }
}
