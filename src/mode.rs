//! Mode definitions for Tonbo.
//!
//! The `Mode` trait describes the storage primitives plugged into the core DB,
//! while concrete mode implementations (today only `DynMode`) live alongside it.

use std::{hash::Hash, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fusio::executor::{Executor, Timer};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    db::DB,
    extractor::{KeyExtractError, KeyProjection, projection_for_columns},
    inmem::{
        immutable::memtable::MVCC_COMMIT_COL,
        mutable::{DynMem, MutableLayout},
    },
    key::KeyOwned,
    manifest::TableDefinition,
    mvcc::Timestamp,
    transaction::CommitAckMode,
    wal::frame::WalEvent,
};

mod dyn_config;

/// Trait describing how a Tonbo `DB` instance stores and accesses data.
pub trait Mode {
    /// Logical key type stored in the memtable.
    type Key: Ord + Hash + Clone;

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

/// Modes capable of describing their catalog metadata for bootstrap.
pub trait CatalogDescribe: Mode {
    /// Derive the table definition for the provided config and logical table name.
    fn table_definition(config: &Self::Config, table_name: &str) -> TableDefinition;
}

/// Dynamic runtime-schema mode backed by Arrow `RecordBatch` values.
pub struct DynMode {
    pub(crate) schema: SchemaRef,
    pub(crate) delete_schema: SchemaRef,
    pub(crate) extractor: Arc<dyn KeyProjection>,
    pub(crate) delete_projection: Arc<dyn KeyProjection>,
    pub(crate) commit_ack_mode: CommitAckMode,
}

/// Configuration bundle for constructing a `DynMode`.
pub struct DynModeConfig {
    /// Arrow schema describing the dynamic table.
    pub schema: SchemaRef,
    /// Extractor used to derive logical keys from dynamic batches.
    pub extractor: Arc<dyn KeyProjection>,
    /// WAL acknowledgement mode for transactional commits.
    pub commit_ack_mode: CommitAckMode,
}

impl DynModeConfig {
    /// Validate the extractor against `schema` and construct the config bundle.
    pub fn new(
        schema: SchemaRef,
        extractor: Box<dyn KeyProjection>,
    ) -> Result<Self, KeyExtractError> {
        extractor.validate_schema(&schema)?;
        let extractor: Arc<dyn KeyProjection> = extractor.into();
        Ok(Self {
            schema,
            extractor,
            commit_ack_mode: CommitAckMode::default(),
        })
    }

    /// Override the commit acknowledgement mode for transactional writes.
    pub fn with_commit_ack_mode(mut self, mode: CommitAckMode) -> Self {
        self.commit_ack_mode = mode;
        self
    }
}

impl Mode for DynMode {
    type Key = KeyOwned;
    type ImmLayout = RecordBatch;
    type Mutable = DynMem;
    type Config = DynModeConfig;
    type InsertInput = RecordBatch;

    fn build(config: Self::Config) -> Result<(Self, Self::Mutable), KeyExtractError> {
        let DynModeConfig {
            schema,
            extractor,
            commit_ack_mode,
        } = config;
        // schema already validated in DynModeConfig::new, but double-check for defensive callers.
        extractor.validate_schema(&schema)?;
        let key_schema = extractor.key_schema();
        let delete_schema = build_delete_schema(&key_schema);
        let key_columns = key_schema.fields().len();
        let delete_projection =
            projection_for_columns(delete_schema.clone(), (0..key_columns).collect())?;
        let delete_projection: Arc<dyn KeyProjection> = delete_projection.into();

        let mutable = DynMem::new(schema.clone());
        Ok((
            Self {
                schema,
                delete_schema,
                extractor,
                delete_projection,
                commit_ack_mode,
            },
            mutable,
        ))
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
            let mut wal_spans: Vec<(u64, u64)> = Vec::new();
            if let Some(handle) = db.wal_handle().cloned() {
                let provisional_id = handle.next_provisional_id();
                let append_ticket = handle
                    .txn_append(provisional_id, &batch, commit_ts)
                    .await
                    .map_err(KeyExtractError::from)?;
                let commit_ticket = handle
                    .txn_commit(provisional_id, commit_ts)
                    .await
                    .map_err(KeyExtractError::from)?;
                for ticket in [append_ticket, commit_ticket] {
                    let ack = ticket.durable().await.map_err(KeyExtractError::from)?;
                    wal_spans.push((ack.first_seq, ack.last_seq));
                }
            }
            db.insert_into_mutable(batch, commit_ts)?;
            for (first, last) in wal_spans {
                db.observe_mutable_wal_span(first, last);
            }
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

fn build_delete_schema(key_schema: &SchemaRef) -> SchemaRef {
    let mut fields = key_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<Field>>();
    fields.push(Field::new(MVCC_COMMIT_COL, DataType::UInt64, false));
    std::sync::Arc::new(Schema::new(fields))
}

impl CatalogDescribe for DynMode {
    fn table_definition(config: &Self::Config, table_name: &str) -> TableDefinition {
        let key_columns = config
            .extractor
            .key_indices()
            .iter()
            .map(|idx| config.schema.field(*idx).name().clone())
            .collect();
        TableDefinition {
            name: table_name.to_string(),
            schema_fingerprint: fingerprint_schema(&config.schema),
            primary_key_columns: key_columns,
            retention: None,
            schema_version: 0,
        }
    }
}

fn fingerprint_schema(schema: &SchemaRef) -> String {
    let mut hasher = Sha256::new();
    let value =
        serde_json::to_value(schema.as_ref()).expect("arrow schema serialization should not fail");
    let canonical = canonicalize_json(value);
    let bytes =
        serde_json::to_vec(&canonical).expect("canonical schema serialization should not fail");
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let sorted = entries
                .into_iter()
                .map(|(key, value)| (key, canonicalize_json(value)))
                .collect();
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonicalize_json).collect()),
        other => other,
    }
}
