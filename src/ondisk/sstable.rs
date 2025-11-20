//! SSTable skeleton definitions.
//!
//! The goal of this module is to capture the public surface for Tonbo's
//! on-disk runs without committing to a specific encoding or IO strategy just
//! yet. Future patches will flesh out the concrete dynamic and typed builders,
//! readers, and range scans using this scaffolding.

use std::{collections::HashMap, convert::TryFrom, fmt, marker::PhantomData, sync::Arc};

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fusio::{
    DynFs,
    error::Error as FsError,
    fs::OpenOptions,
    path::{Path, PathPart},
};
use fusio_parquet::writer::AsyncWriter;
use once_cell::sync::Lazy;
use parquet::{
    arrow::async_writer::AsyncArrowWriter,
    basic::{Compression, ZstdLevel},
    errors::ParquetError,
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};

use crate::{
    id::FileId,
    inmem::immutable::{
        Immutable,
        memtable::{DeleteSidecar, MVCC_COMMIT_COL, MVCC_TOMBSTONE_COL, MvccColumns},
    },
    key::{KeyOwned, RangeSet},
    manifest::ManifestError,
    mode::Mode,
    mvcc::Timestamp,
    query::Predicate,
};

const MVCC_SCHEMA_VERSION_KEY: &str = "tonbo.mvcc.version";
const MVCC_SCHEMA_VERSION_V1: &str = "1";

static MVCC_SIDECAR_SCHEMA_V1: Lazy<SchemaRef> = Lazy::new(|| {
    let mut metadata = HashMap::new();
    metadata.insert(
        MVCC_SCHEMA_VERSION_KEY.to_string(),
        MVCC_SCHEMA_VERSION_V1.to_string(),
    );
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new(MVCC_COMMIT_COL, DataType::UInt64, false),
            Field::new(MVCC_TOMBSTONE_COL, DataType::Boolean, false),
        ],
        metadata,
    ))
});

fn encode_mvcc_sidecar(mvcc: &MvccColumns) -> Result<RecordBatch, SsTableError> {
    let commit_values = UInt64Array::from_iter_values(mvcc.commit_ts.iter().map(|ts| ts.get()));
    let tombstone_values = BooleanArray::from(mvcc.tombstone.clone());
    RecordBatch::try_new(
        Arc::clone(&MVCC_SIDECAR_SCHEMA_V1),
        vec![
            Arc::new(commit_values) as ArrayRef,
            Arc::new(tombstone_values) as ArrayRef,
        ],
    )
    .map_err(|err| SsTableError::Parquet(ParquetError::ArrowError(err.to_string())))
}

fn validate_mvcc_schema(schema: &Schema) -> Result<(), SsTableError> {
    match schema.metadata().get(MVCC_SCHEMA_VERSION_KEY) {
        Some(version) if version == MVCC_SCHEMA_VERSION_V1 => Ok(()),
        Some(other) => Err(SsTableError::Parquet(ParquetError::ArrowError(format!(
            "unsupported mvcc sidecar schema version {other}"
        )))),
        None => Err(SsTableError::Parquet(ParquetError::ArrowError(
            "mvcc sidecar schema missing version metadata".to_string(),
        ))),
    }
}

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

/// Configuration bundle shared by SSTable builders and readers.
#[derive(Clone)]
pub struct SsTableConfig {
    schema: SchemaRef,
    target_level: usize,
    compression: SsTableCompression,
    fs: Arc<dyn DynFs>,
    root: Path,
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
}

impl fmt::Debug for SsTableConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SsTableConfig")
            .field("schema_fields", &self.schema.fields().len())
            .field("target_level", &self.target_level)
            .field("compression", &self.compression)
            .field("root", &self.root)
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
pub struct SsTable<M: Mode> {
    descriptor: SsTableDescriptor,
    _mode: PhantomData<M>,
}

impl<M: Mode> SsTable<M> {
    /// Create a handle for an SSTable that has been persisted.
    pub fn new(descriptor: SsTableDescriptor) -> Self {
        Self {
            descriptor,
            _mode: PhantomData,
        }
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
    pub fn with_storage_paths(
        mut self,
        data_path: Path,
        mvcc_path: Path,
        delete_path: Option<Path>,
    ) -> Self {
        self.data_path = Some(data_path);
        self.mvcc_path = Some(mvcc_path);
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
    pub rows: usize,
    /// Approximate on-disk byte size of the table payload.
    pub bytes: usize,
    /// Number of tombstoned rows recorded in the table.
    pub tombstones: usize,
    /// Minimum key observed across staged segments.
    pub min_key: Option<KeyOwned>,
    /// Maximum key observed across staged segments.
    pub max_key: Option<KeyOwned>,
    /// Oldest commit timestamp contained in the table.
    pub min_commit_ts: Option<Timestamp>,
    /// Newest commit timestamp contained in the table.
    pub max_commit_ts: Option<Timestamp>,
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
    /// Filesystem operation failed while writing an SSTable.
    #[error("filesystem error: {0}")]
    Fs(#[from] FsError),
    /// Parquet writer failed while persisting an SSTable.
    #[error("parquet write error: {0}")]
    Parquet(#[from] ParquetError),
    /// Invalid path component produced while building an SSTable destination.
    #[error("invalid sstable path component: {0}")]
    InvalidPath(String),
    /// Manifest backend error
    #[error("failure when persist into manifest: {0}")]
    Manifest(#[from] ManifestError),
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
    data: RecordBatch,
    mvcc: MvccColumns,
    deletes: DeleteSidecar,
}

pub(crate) struct ParquetTableWriter<M: Mode> {
    config: Arc<SsTableConfig>,
    descriptor: SsTableDescriptor,
    staged: StagedTableStats,
    segments: Vec<StagedSegment>,
    wal_ids: Option<Vec<FileId>>,
    _mode: PhantomData<M>,
}

impl<M> ParquetTableWriter<M>
where
    M: Mode<ImmLayout = RecordBatch, Key = KeyOwned>,
{
    pub(crate) fn new(config: Arc<SsTableConfig>, descriptor: SsTableDescriptor) -> Self {
        Self {
            config,
            descriptor,
            staged: StagedTableStats::new(),
            segments: Vec::new(),
            wal_ids: None,
            _mode: PhantomData,
        }
    }
    pub(crate) fn set_wal_ids(&mut self, wal_ids: Option<Vec<FileId>>) {
        self.wal_ids = wal_ids;
    }

    pub(crate) fn stage_immutable(&mut self, segment: &Immutable<M>) -> Result<(), SsTableError> {
        let data_rows = segment.storage().num_rows();
        let delete_rows = segment.delete_sidecar().len();
        if data_rows == 0 && delete_rows == 0 {
            return Ok(());
        }

        let mut segment_tombstones = 0usize;
        let mut seg_min_ts = None;
        let mut seg_max_ts = None;
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
        let staged_segment = StagedSegment {
            data: segment.storage().clone(),
            mvcc: segment.mvcc_columns().clone(),
            deletes: segment.delete_sidecar().clone(),
        };
        self.segments.push(staged_segment);
        Ok(())
    }

    pub(crate) async fn finish(self) -> Result<SsTable<M>, SsTableError> {
        if self.staged.segments == 0 {
            return Err(SsTableError::NoImmutableSegments);
        }

        let mut ctx = WriteContext::new(Arc::clone(&self.config), &self.descriptor).await?;
        for segment in &self.segments {
            ctx.write_segment(segment).await?;
        }
        let (data_path, mvcc_path, delete_path, data_bytes) = ctx.finish().await?;
        let stats = SsTableStats::from_staged(
            self.staged,
            usize::try_from(data_bytes).unwrap_or(usize::MAX),
        );
        Ok(SsTable::new(
            self.descriptor
                .with_stats(stats)
                .with_storage_paths(data_path, mvcc_path, delete_path)
                .with_wal_ids(self.wal_ids),
        ))
    }

    #[cfg(test)]
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
struct WriteContext {
    fs: Arc<dyn DynFs>,
    data_path: Path,
    mvcc_path: Path,
    delete_path: Path,
    data_writer: Option<AsyncArrowWriter<AsyncWriter>>,
    mvcc_writer: Option<AsyncArrowWriter<AsyncWriter>>,
    delete_writer: Option<AsyncArrowWriter<AsyncWriter>>,
    delete_written: bool,
    compression: SsTableCompression,
}
impl WriteContext {
    async fn new(
        config: Arc<SsTableConfig>,
        descriptor: &SsTableDescriptor,
    ) -> Result<Self, SsTableError> {
        let fs = Arc::clone(config.fs());
        let level_name = format!("L{}", descriptor.level());
        let level_part = PathPart::parse(&level_name)
            .map_err(|err| SsTableError::InvalidPath(err.to_string()))?;
        let dir_path = config.root().child(level_part);
        fs.create_dir_all(&dir_path).await?;

        let data_file_name = format!("{:020}.parquet", descriptor.id().raw());
        let data_part = PathPart::parse(&data_file_name)
            .map_err(|err| SsTableError::InvalidPath(err.to_string()))?;
        let data_path = dir_path.child(data_part);

        let mvcc_file_name = format!("{:020}.mvcc.parquet", descriptor.id().raw());
        let mvcc_part = PathPart::parse(&mvcc_file_name)
            .map_err(|err| SsTableError::InvalidPath(err.to_string()))?;
        let mvcc_path = dir_path.child(mvcc_part);

        let delete_file_name = format!("{:020}.delete.parquet", descriptor.id().raw());
        let delete_part = PathPart::parse(&delete_file_name)
            .map_err(|err| SsTableError::InvalidPath(err.to_string()))?;
        let delete_path = dir_path.child(delete_part);

        let data_options = OpenOptions::default()
            .create(true)
            .write(true)
            .truncate(true);
        let mvcc_options = OpenOptions::default()
            .create(true)
            .write(true)
            .truncate(true);
        let data_file = fs.open_options(&data_path, data_options).await?;
        let mvcc_file = fs.open_options(&mvcc_path, mvcc_options).await?;

        let data_writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(data_file),
            config.schema().clone(),
            Some(writer_properties(config.compression())),
        )?;

        let mvcc_writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(mvcc_file),
            Arc::clone(&MVCC_SIDECAR_SCHEMA_V1),
            Some(writer_properties(config.compression())),
        )?;

        Ok(Self {
            fs,
            data_path,
            mvcc_path,
            data_writer: Some(data_writer),
            mvcc_writer: Some(mvcc_writer),
            delete_path,
            delete_writer: None,
            delete_written: false,
            compression: config.compression(),
        })
    }

    async fn write_segment(&mut self, segment: &StagedSegment) -> Result<(), SsTableError> {
        if let Some(writer) = self.data_writer.as_mut() {
            writer.write(&segment.data).await?;
        } else {
            return Err(SsTableError::WriterClosed);
        }

        let sidecar_batch = encode_mvcc_sidecar(&segment.mvcc)?;
        validate_mvcc_schema(sidecar_batch.schema().as_ref())?;

        if let Some(writer) = self.mvcc_writer.as_mut() {
            writer.write(&sidecar_batch).await?;
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
                AsyncWriter::new(file),
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

    async fn finish(mut self) -> Result<(Path, Path, Option<Path>, u64), SsTableError> {
        if let Some(writer) = self.data_writer.take() {
            writer.close().await?;
        }
        if let Some(writer) = self.mvcc_writer.take() {
            writer.close().await?;
        }

        let data_path = self.data_path.clone();
        let mvcc_path = self.mvcc_path.clone();
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

        Ok((data_path, mvcc_path, delete_path, data_bytes))
    }
}

impl Drop for WriteContext {
    fn drop(&mut self) {
        debug_assert!(
            self.data_writer.is_none()
                && self.mvcc_writer.is_none()
                && self.delete_writer.is_none(),
            "WriteContext dropped without closing writers"
        );
    }
}

/// Planner responsible for turning immutable runs into SSTable files.
pub struct SsTableBuilder<M: Mode> {
    writer: ParquetTableWriter<M>,
}
impl<M> SsTableBuilder<M>
where
    M: Mode<ImmLayout = RecordBatch, Key = KeyOwned>,
{
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
    pub(crate) fn add_immutable(&mut self, segment: &Immutable<M>) -> Result<(), SsTableError> {
        self.writer.stage_immutable(segment)
    }
    /// Finalize the staged runs and flush them to durable storage.
    pub async fn finish(self) -> Result<SsTable<M>, SsTableError> {
        self.writer.finish().await
    }
}

/// Handle used by the read path to stream rows from an SSTable.
#[derive(Debug)]
pub struct SsTableReader<M: Mode> {
    _descriptor: SsTableDescriptor,
    _mode: PhantomData<M>,
}

impl<M: Mode> SsTableReader<M> {
    /// Open an SSTable using the supplied descriptor and shared configuration.
    pub async fn open(
        config: Arc<SsTableConfig>,
        descriptor: SsTableDescriptor,
    ) -> Result<Self, SsTableError> {
        let _fs = config.fs();
        let _root = config.root();
        let _schema = config.schema();
        let _descriptor = &descriptor;
        let _ = config;
        let _ = descriptor;
        // IO and cache wiring will live here.
        Err(SsTableError::Unimplemented)
    }

    /// Plan a range scan across this table.
    pub fn plan_scan(
        &self,
        _ranges: &RangeSet<M::Key>,
        _ts: Timestamp,
        _predicate: Option<&Predicate>,
    ) -> Result<SsTableScanPlan<M>, SsTableError> {
        let _ = self;
        Err(SsTableError::Unimplemented)
    }
}

/// Execution plan for reading rows out of an SSTable.
#[derive(Debug)]
pub struct SsTableScanPlan<M: Mode> {
    _descriptor: SsTableDescriptor,
    _mode: PhantomData<M>,
}

impl<M: Mode> SsTableScanPlan<M> {
    /// Materialize the plan into a streaming cursor that yields Arrow batches.
    pub async fn execute(self) -> Result<SsTableStream<M>, SsTableError> {
        let _ = self;
        Err(SsTableError::Unimplemented)
    }
}

/// Placeholder stream returned by executed scan plans.
#[derive(Debug)]
pub struct SsTableStream<M: Mode> {
    _mode: PhantomData<M>,
}

impl<M: Mode> SsTableStream<M> {
    /// Consume the stream into owned batches once the reader is implemented.
    pub async fn collect(self) -> Result<Vec<M::ImmLayout>, SsTableError> {
        let _ = self;
        Err(SsTableError::Unimplemented)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow_schema::{DataType, Field, Schema};
    use fusio::{disk::LocalFs, dynamic::DynFs, path::Path};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        inmem::immutable::memtable::{ImmutableIndexEntry, ImmutableMemTable, bundle_mvcc_sidecar},
        key::KeyTsViewRaw,
        mvcc::Timestamp,
        test_util::build_batch,
    };

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
        let mut writer: ParquetTableWriter<crate::mode::DynMode> =
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
        let writer: ParquetTableWriter<crate::mode::DynMode> =
            ParquetTableWriter::new(test_config(schema), descriptor);
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
        let mut writer: ParquetTableWriter<crate::mode::DynMode> =
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
        let mut writer: ParquetTableWriter<crate::mode::DynMode> =
            ParquetTableWriter::new(test_config(schema.clone()), descriptor);

        let segment = sample_segment(vec![("m".into(), 9)], vec![123], vec![false]);
        writer.stage_immutable(&segment).expect("stage segment");

        let table = writer.finish().await.expect("finish table");
        let descriptor = table.descriptor();
        let data_path = descriptor.data_path().expect("data path present");
        let mvcc_path = descriptor.mvcc_path().expect("mvcc path present");
        assert!(data_path.as_ref().ends_with(".parquet"));
        assert!(mvcc_path.as_ref().ends_with(".mvcc.parquet"));
        assert_ne!(data_path.as_ref(), mvcc_path.as_ref());
        assert!(descriptor.delete_path().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finish_records_delete_sidecar_when_tombstones_exist() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let descriptor = SsTableDescriptor::new(SsTableId::new(22), 0);
        let mut writer: ParquetTableWriter<crate::mode::DynMode> =
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
