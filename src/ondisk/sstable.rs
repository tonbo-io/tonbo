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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SsTableCompression {
    /// Leave pages uncompressed (useful for tests and debugging).
    None,
    /// Apply Zstd compression with default tuning.
    Zstd,
}

impl Default for SsTableCompression {
    fn default() -> Self {
        SsTableCompression::Zstd
    }
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

/// Planner responsible for turning immutable runs into SSTable files.
#[derive(Debug)]
pub struct SsTableBuilder<M: Mode> {
    config: Arc<SsTableConfig>,
    descriptor: SsTableDescriptor,
    _mode: PhantomData<M>,
}

impl<M: Mode> SsTableBuilder<M> {
    /// Create a new builder for the provided descriptor.
    pub fn new(config: Arc<SsTableConfig>, descriptor: SsTableDescriptor) -> Self {
        Self {
            config,
            descriptor,
            _mode: PhantomData,
        }
    }

    /// Stage an immutable run emitted by the mutable layer.
    pub(crate) fn add_immutable(&mut self, segment: &Immutable<M>) -> Result<(), SsTableError> {
        let _fs = self.config.fs();
        let _root = self.config.root();
        let _schema = self.config.schema();
        let _level = self.descriptor.level;
        let _segment = segment;
        // The actual serialization path will plug in here.
        Err(SsTableError::Unimplemented)
    }

    /// Finalize the staged runs and flush them to durable storage.
    pub async fn finish(self) -> Result<SsTable<M>, SsTableError> {
        let _fs = self.config.fs();
        let _root = self.config.root();
        let _descriptor = &self.descriptor;
        // Future patches will perform IO and return a populated handle.
        Err(SsTableError::Unimplemented)
    }
}

/// Handle used by the read path to stream rows from an SSTable.
#[derive(Debug)]
pub struct SsTableReader<M: Mode> {
    descriptor: SsTableDescriptor,
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
    descriptor: SsTableDescriptor,
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
