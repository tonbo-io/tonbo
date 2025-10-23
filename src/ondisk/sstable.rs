//! SSTable skeleton definitions.
//!
//! The goal of this module is to capture the public surface for Tonbo's
//! on-disk runs without committing to a specific encoding or IO strategy just
//! yet. Future patches will flesh out the concrete dynamic and typed builders,
//! readers, and range scans using this scaffolding.

use std::{fmt, marker::PhantomData, sync::Arc};

use arrow_schema::SchemaRef;
use fusio::{DynFs, path::Path};

use crate::{
    inmem::immutable::Immutable, mode::Mode, mvcc::Timestamp, query::Predicate, scan::RangeSet,
};

/// Identifier for an SSTable stored on disk.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
    approximate_stats: Option<SsTableStats>,
}

impl SsTableDescriptor {
    /// Build a descriptor for a freshly created table.
    pub fn new(id: SsTableId, level: usize) -> Self {
        Self {
            id,
            level,
            approximate_stats: None,
        }
    }

    /// Attach rough statistics computed during flush.
    pub fn with_stats(mut self, stats: SsTableStats) -> Self {
        self.approximate_stats = Some(stats);
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
}

/// Lightweight table statistics captured at flush time.
#[derive(Clone, Debug, Default)]
pub struct SsTableStats {
    /// Estimated logical row count written to the table.
    pub rows: usize,
    /// Approximate on-disk byte size of the table payload.
    pub bytes: usize,
    // TODO: decide which staged stats (key bounds, tombstones, ts range) graduate here.
}

/// Error type shared across SSTable planning and IO.
#[derive(Debug, thiserror::Error)]
pub enum SsTableError {
    /// Placeholder variant until real IO wiring lands.
    #[error("sstable operation has not been implemented yet")]
    Unimplemented,
    /// No immutable segments were available when attempting to flush.
    #[error("no immutable segments available for SSTable flush")]
    NoImmutableSegments,
}

/// Scratchpad used while minor compaction plans an SST. Keeps provisional stats
/// that may not map 1:1 onto the persisted descriptor yet.
pub(crate) struct StagedTableStats<K> {
    pub segments: usize,
    pub rows: usize,
    pub tombstones: usize,
    pub min_key: Option<K>,
    pub max_key: Option<K>,
    pub min_commit_ts: Option<Timestamp>,
    pub max_commit_ts: Option<Timestamp>,
}

impl<K: Ord + Clone> StagedTableStats<K> {
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

    fn update_key_bounds(&mut self, seg_min: &K, seg_max: &K) {
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

impl<K> StagedTableStats<K> {
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

impl<K> fmt::Debug for StagedTableStats<K> {
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

/// PlaceHolder Parquet writer facade used while IO is stubbed out.
pub(crate) struct ParquetTableWriter<M: Mode> {
    _config: Arc<SsTableConfig>,
    descriptor: SsTableDescriptor,
    staged: StagedTableStats<M::Key>,
    _mode: PhantomData<M>,
}

impl<M> ParquetTableWriter<M>
where
    M: Mode,
    M::Key: Clone,
{
    pub(crate) fn new(config: Arc<SsTableConfig>, descriptor: SsTableDescriptor) -> Self {
        Self {
            _config: config,
            descriptor,
            staged: StagedTableStats::new(),
            _mode: PhantomData,
        }
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
        Ok(())
    }

    pub(crate) async fn finish(self) -> Result<SsTable<M>, SsTableError> {
        if self.staged.segments == 0 {
            return Err(SsTableError::NoImmutableSegments);
        }

        // TODO: Implement actual Parquet writing logic here.
        let stats = SsTableStats {
            rows: self.staged.rows,
            bytes: 0,
        };

        let descriptor = if stats.rows > 0 {
            self.descriptor.with_stats(stats)
        } else {
            self.descriptor
        };

        Ok(SsTable::new(descriptor))
    }

    #[cfg(test)]
    pub(crate) fn plan(&self) -> &StagedTableStats<M::Key> {
        &self.staged
    }
}

/// Planner responsible for turning immutable runs into SSTable files.
pub struct SsTableBuilder<M: Mode> {
    writer: ParquetTableWriter<M>,
}

impl<M: Mode> SsTableBuilder<M> {
    /// Create a new builder for the provided descriptor.
    pub fn new(config: Arc<SsTableConfig>, descriptor: SsTableDescriptor) -> Self
    where
        M::Key: Clone,
    {
        Self {
            writer: ParquetTableWriter::new(config, descriptor),
        }
    }

    /// Stage an immutable run emitted by the mutable layer.
    pub(crate) fn add_immutable(&mut self, segment: &Immutable<M>) -> Result<(), SsTableError>
    where
        M::Key: Clone,
    {
        self.writer.stage_immutable(segment)
    }

    /// Finalize the staged runs and flush them to durable storage.
    pub async fn finish(self) -> Result<SsTable<M>, SsTableError>
    where
        M::Key: Clone,
    {
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
}
