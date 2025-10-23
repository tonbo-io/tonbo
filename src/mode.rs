//! Mode definitions for Tonbo.
//!
//! The `Mode` trait describes the storage primitives plugged into the core DB,
//! while concrete mode implementations (today only `DynMode`) live alongside it.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use fusio::executor::{Executor, Timer};

use crate::{
    db::DB,
    inmem::mutable::{DynMem, MutableLayout},
    mvcc::Timestamp,
    record::extract::{DynKeyExtractor, KeyDyn, KeyExtractError},
    wal::{WalPayload, append_tombstone_column, frame::WalEvent},
};

mod dyn_config;

/// Trait describing how a Tonbo `DB` instance stores and accesses data.
pub trait Mode {
    /// Logical key type stored in the memtable.
    type Key: Ord;

    /// Storage type inside the immutable segments for this mode.
    type ImmLayout;

    /// Mutable memtable implementation used by this mode.
    type Mutable: MutableLayout<Self::Key>;

    /// Configuration required to instantiate this mode and its mutable layout.
    type Config;

    /// Input accepted by `DB::ingest` for this mode.
    type InsertInput;

    /// Build the mode and its mutable layout from `config`.
    fn build(config: Self::Config) -> Result<(Self, Self::Mutable), KeyExtractError>
    where
        Self: Sized;

    /// Insert `value` into the given database instance.
    fn insert<'a, E>(
        db: &'a mut DB<Self, E>,
        value: Self::InsertInput,
    ) -> impl std::future::Future<Output = Result<(), KeyExtractError>> + 'a
    where
        Self: Sized,
        E: Executor + Timer;

    /// Replay WAL events into the database during recovery, returning the last commit timestamp if
    /// any.
    fn replay_wal<E>(
        db: &mut DB<Self, E>,
        events: Vec<WalEvent>,
    ) -> Result<Option<Timestamp>, KeyExtractError>
    where
        Self: Sized,
        E: Executor + Timer;
}

/// Dynamic runtime-schema mode backed by Arrow `RecordBatch` values.
pub struct DynMode {
    pub(crate) schema: SchemaRef,
    pub(crate) extractor: Box<dyn DynKeyExtractor>,
}

/// Configuration bundle for constructing a `DynMode`.
pub struct DynModeConfig {
    /// Arrow schema describing the dynamic table.
    pub schema: SchemaRef,
    /// Extractor used to derive logical keys from dynamic batches.
    pub extractor: Box<dyn DynKeyExtractor>,
}

impl DynModeConfig {
    /// Validate the extractor against `schema` and construct the config bundle.
    pub fn new(
        schema: SchemaRef,
        extractor: Box<dyn DynKeyExtractor>,
    ) -> Result<Self, KeyExtractError> {
        extractor.validate_schema(&schema)?;
        Ok(Self { schema, extractor })
    }
}

impl Mode for DynMode {
    type Key = KeyDyn;
    type ImmLayout = RecordBatch;
    type Mutable = DynMem;
    type Config = DynModeConfig;
    type InsertInput = RecordBatch;

    fn build(config: Self::Config) -> Result<(Self, Self::Mutable), KeyExtractError> {
        let DynModeConfig { schema, extractor } = config;
        // schema already validated in DynModeConfig::new, but double-check for defensive callers.
        extractor.validate_schema(&schema)?;
        Ok((Self { schema, extractor }, DynMem::new()))
    }

    fn insert<'a, E>(
        db: &'a mut DB<Self, E>,
        batch: RecordBatch,
    ) -> impl std::future::Future<Output = Result<(), KeyExtractError>> + 'a
    where
        E: Executor + Timer,
    {
        async fn insert_impl<E>(
            db: &mut DB<DynMode, E>,
            batch: RecordBatch,
        ) -> Result<(), KeyExtractError>
        where
            E: Executor + Timer,
        {
            if db.schema().as_ref() != batch.schema().as_ref() {
                return Err(KeyExtractError::SchemaMismatch {
                    expected: db.schema().clone(),
                    actual: batch.schema(),
                });
            }
            let commit_ts = db.next_commit_ts();
            if let Some(handle) = db.wal_handle().cloned() {
                let wal_batch =
                    append_tombstone_column(&batch, None).map_err(KeyExtractError::from)?;
                let payload = WalPayload::DynBatch {
                    batch: wal_batch,
                    commit_ts,
                };
                let ticket = handle
                    .submit(payload)
                    .await
                    .map_err(KeyExtractError::from)?;
                ticket.durable().await.map_err(KeyExtractError::from)?;
            }
            db.insert_into_mutable(batch, commit_ts)?;
            db.maybe_seal_after_insert()?;
            Ok(())
        }

        insert_impl(db, batch)
    }

    fn replay_wal<E>(
        db: &mut DB<Self, E>,
        events: Vec<WalEvent>,
    ) -> Result<Option<Timestamp>, KeyExtractError>
    where
        E: Executor + Timer,
    {
        db.replay_wal_events(events)
    }
}
