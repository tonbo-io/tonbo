//! SSTable skeleton definitions.
//!
//! The goal of this module is to capture the public surface for Tonbo's
//! on-disk runs without committing to a specific encoding or IO strategy just
//! yet. Future patches will flesh out the concrete dynamic and typed builders,
//! readers, and range scans using this scaffolding.

use std::{convert::TryFrom, fmt, marker::PhantomData, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use fusio::{
    DynFs,
    error::Error as FsError,
    fs::OpenOptions,
    path::{Path, PathPart},
};
use fusio_parquet::writer::AsyncWriter;
use parquet::{
    arrow::async_writer::AsyncArrowWriter,
    basic::{Compression, ZstdLevel},
    errors::ParquetError,
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};

use crate::{
    fs::FileId, inmem::immutable::Immutable, mode::Mode, mvcc::Timestamp, query::Predicate,
    record::extract::KeyDyn, scan::RangeSet,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SsTableDescriptor {
    id: SsTableId,
    level: usize,
    approximate_stats: Option<SsTableStats>,
    wal_ids: Option<Vec<FileId>>,
}

impl SsTableDescriptor {
    /// Build a descriptor for a freshly created table.
    pub fn new(id: SsTableId, level: usize) -> Self {
        Self {
            id,
            level,
            approximate_stats: None,
            wal_ids: None,
        }
    }

    /// Attach rough statistics computed during flush.
    pub fn with_stats(mut self, stats: SsTableStats) -> Self {
        self.approximate_stats = Some(stats);
        self
    }

    /// Attach the WAL IDs associated with this table.
    pub fn with_wal_ids(mut self, wal_ids: Option<Vec<FileId>>) -> Self {
        self.wal_ids = wal_ids;
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
        self.approximate_stats.as_ref()
    }

    /// WAL file identifiers that contributed to this table.
    pub fn wal_ids(&self) -> Option<&[FileId]> {
        self.wal_ids.as_deref()
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
    pub min_key: Option<KeyDyn>,
    /// Maximum key observed across staged segments.
    pub max_key: Option<KeyDyn>,
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
}

/// Scratchpad used while minor compaction plans an SST. Keeps provisional stats
/// that may not map 1:1 onto the persisted descriptor yet.
pub(crate) struct StagedTableStats {
    pub segments: usize,
    pub rows: usize,
    pub tombstones: usize,
    pub min_key: Option<KeyDyn>,
    pub max_key: Option<KeyDyn>,
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

    fn update_key_bounds(&mut self, seg_min: &KeyDyn, seg_max: &KeyDyn) {
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
pub(crate) struct ParquetTableWriter<M: Mode> {
    config: Arc<SsTableConfig>,
    descriptor: SsTableDescriptor,
    staged: StagedTableStats,
    segments: Vec<RecordBatch>,
    wal_ids: Option<Vec<FileId>>,
    _mode: PhantomData<M>,
}

impl<M> ParquetTableWriter<M>
where
    M: Mode<ImmLayout = RecordBatch, Key = KeyDyn>,
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
        let rows = segment.len();
        if rows == 0 {
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
            self.staged.update_key_bounds(min_key, max_key);
        }

        if let (Some(min_ts), Some(max_ts)) = (seg_min_ts, seg_max_ts) {
            self.staged.update_commit_bounds(min_ts, max_ts);
        }

        self.staged.segments += 1;
        self.staged.rows += rows;
        self.staged.tombstones += segment_tombstones;
        self.segments.push(segment.storage().clone());
        Ok(())
    }

    pub(crate) async fn finish(self) -> Result<SsTable<M>, SsTableError> {
        if self.staged.segments == 0 {
            return Err(SsTableError::NoImmutableSegments);
        }

        let mut ctx = WriteContext::new(Arc::clone(&self.config), &self.descriptor).await?;
        for batch in &self.segments {
            ctx.write_batch(batch).await?;
        }
        let bytes_written = ctx.finish().await?;
        let stats = SsTableStats::from_staged(
            self.staged,
            usize::try_from(bytes_written).unwrap_or(usize::MAX),
        );
        Ok(SsTable::new(
            self.descriptor.with_stats(stats).with_wal_ids(self.wal_ids),
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
    path: Path,
    writer: Option<AsyncArrowWriter<AsyncWriter>>,
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
        let file_name = format!("{:020}.parquet", descriptor.id().raw());
        let file_part = PathPart::parse(&file_name)
            .map_err(|err| SsTableError::InvalidPath(err.to_string()))?;
        let file_path = dir_path.child(file_part);
        let options = OpenOptions::default()
            .create(true)
            .write(true)
            .truncate(true);
        let file = fs.open_options(&file_path, options).await?;
        let writer = AsyncWriter::new(file);
        let props = writer_properties(config.compression());
        let arrow_writer = AsyncArrowWriter::try_new(writer, config.schema().clone(), Some(props))?;
        Ok(Self {
            fs,
            path: file_path,
            writer: Some(arrow_writer),
        })
    }
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SsTableError> {
        match self.writer.as_mut() {
            Some(writer) => {
                writer.write(batch).await?;
                Ok(())
            }
            None => Err(SsTableError::WriterClosed),
        }
    }
    async fn finish(mut self) -> Result<u64, SsTableError> {
        if let Some(writer) = self.writer.take() {
            writer.close().await?;
        }
        let file = self
            .fs
            .open_options(&self.path, OpenOptions::default().read(true))
            .await?;
        let size = file.size().await?;
        Ok(size)
    }
}
impl Drop for WriteContext {
    fn drop(&mut self) {
        debug_assert!(
            self.writer.is_none(),
            "WriteContext dropped without closing writer"
        );
    }
}

/// Planner responsible for turning immutable runs into SSTable files.
pub struct SsTableBuilder<M: Mode> {
    writer: ParquetTableWriter<M>,
}
impl<M> SsTableBuilder<M>
where
    M: Mode<ImmLayout = RecordBatch, Key = KeyDyn>,
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
        _predicate: Option<&Predicate<M::Key>>,
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
        inmem::immutable::memtable::{ImmutableMemTable, VersionSlice, attach_mvcc_columns},
        mvcc::Timestamp,
        record::extract::KeyDyn,
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
    ) -> ImmutableMemTable<KeyDyn, arrow_array::RecordBatch> {
        assert_eq!(rows.len(), commits.len());
        assert_eq!(rows.len(), tombstones.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let mut keys: Vec<String> = Vec::with_capacity(rows.len());
        let dyn_rows: Vec<DynRow> = rows
            .into_iter()
            .map(|(k, v)| {
                keys.push(k.clone());
                DynRow(vec![Some(DynCell::Str(k.into())), Some(DynCell::I32(v))])
            })
            .collect();
        let batch = build_batch(schema.clone(), dyn_rows).expect("record batch");
        let mut index = BTreeMap::new();
        for (offset, key) in keys.into_iter().enumerate() {
            index.insert(KeyDyn::from(key), VersionSlice::new(offset as u32, 1));
        }
        let (batch, mvcc) = attach_mvcc_columns(
            batch,
            commits.into_iter().map(Timestamp::new).collect(),
            tombstones,
        )
        .expect("mvcc columns");
        ImmutableMemTable::new(batch, index, mvcc)
    }

    fn tokio_block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(future)
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
        assert_eq!(plan.rows, 5);
        assert_eq!(plan.tombstones, 1);
        assert_eq!(plan.min_commit_ts, Some(Timestamp::new(10)));
        assert_eq!(plan.max_commit_ts, Some(Timestamp::new(35)));

        let min_key = plan
            .min_key
            .as_ref()
            .and_then(|k| match k {
                KeyDyn::Str(s) => Some(s.as_str()),
                _ => None,
            })
            .expect("min key");
        let max_key = plan
            .max_key
            .as_ref()
            .and_then(|k| match k {
                KeyDyn::Str(s) => Some(s.as_str()),
                _ => None,
            })
            .expect("max key");
        assert_eq!(min_key, "a");
        assert_eq!(max_key, "e");
    }

    #[test]
    fn finish_without_segments_errors() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let descriptor = SsTableDescriptor::new(SsTableId::new(1), 0);
        let writer: ParquetTableWriter<crate::mode::DynMode> =
            ParquetTableWriter::new(test_config(schema), descriptor);
        let result = futures::executor::block_on(writer.finish());
        assert!(matches!(result, Err(SsTableError::NoImmutableSegments)));
    }

    #[test]
    fn finish_threads_wal_ids_into_descriptor() {
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

        let table = tokio_block_on(writer.finish()).expect("finish");
        let recorded = table.descriptor().wal_ids().expect("descriptor wal ids");
        assert_eq!(recorded, wal_ids.as_slice());
    }
}
