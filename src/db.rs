//! Generic DB parametrized by a `Mode` implementation.
//!
//! At the moment Tonbo only ships with the dynamic runtime-schema mode. The
//! trait-driven structure remains so that compile-time typed dispatch can be
//! reintroduced without reshaping the API.

use std::{collections::HashMap, future::Future, sync::Arc, time::Instant};

use fusio::executor::{Executor, Timer};
use typed_arrow::{arrow_array::RecordBatch, arrow_schema::SchemaRef};

use crate::{
    inmem::{
        immutable::memtable::{ImmutableMemTable, segment_from_batch_with_extractor},
        mutable::{
            MutableLayout,
            memtable::{DynLayout, DynRowScan},
        },
        policy::{SealDecision, SealPolicy, StatsProvider},
    },
    record::extract::{DynKeyExtractor, KeyDyn, KeyExtractError},
    scan::RangeSet,
    wal::{WalConfig, WalHandle, WalPayload, frame::WalEvent, replay::Replayer},
};

/// A DB parametrized by a mode `M` that defines key, payload and insert interface.
pub struct DB<M: Mode, E: Executor + Timer + Send + Sync> {
    mode: M,
    mem: M::Mutable,
    // Immutable in-memory runs (frozen memtables) in recency order (oldest..newest)
    immutables: Vec<Immutable<M>>,
    // Sealing policy and last seal timestamp
    policy: Box<dyn SealPolicy + Send + Sync>,
    last_seal_at: Option<Instant>,
    // Executor powering async subsystems such as the WAL.
    executor: Arc<E>,
    // Optional WAL handle when durability is enabled.
    wal: Option<WalHandle<E>>,
    /// Pending WAL configuration captured before the writer is installed.
    wal_config: Option<WalConfig>,
    /// Monotonic commit timestamp assigned to ingests (autocommit path for now).
    commit_clock: u64,
}

/// Mode trait describing how to insert and what is stored.
pub trait Mode {
    /// Logical key type stored in the memtable.
    type Key: Ord;

    /// Storage type inside the unified immutable segment for this mode.
    type ImmLayout;

    /// Mutable store type for this mode (columnar, last-writer index).
    type Mutable: MutableLayout<Self::Key>;
}

/// Convenience alias for the immutable segment type of a `Mode`.
pub(crate) type Immutable<M> = ImmutableMemTable<<M as Mode>::Key, <M as Mode>::ImmLayout>;

/// Dynamic mode: runtime schema + trait-object extractor produce keys and store dynamic rows.
///
/// Notes:
/// - Enforces schema equality per DB instance: inserting a `RecordBatch` with a different schema
///   returns an error. Create a new DB for a new schema.
/// - Payloads are stored as `typed_arrow_dyn::DynRow` by value; string/binary cells are copied when
///   materializing rows via `row_from_batch`.
pub struct DynMode {
    schema: SchemaRef,
    extractor: Box<dyn DynKeyExtractor>,
}

impl Mode for DynMode {
    type Key = KeyDyn;
    type ImmLayout = RecordBatch;
    type Mutable = DynMem;
}

impl<E> DB<DynMode, E>
where
    E: Executor + Timer,
{
    /// Create a DB in dynamic mode from `schema` and a trait-object extractor.
    pub fn new_dyn(
        schema: SchemaRef,
        extractor: Box<dyn DynKeyExtractor>,
        executor: Arc<E>,
    ) -> Result<Self, KeyExtractError> {
        extractor.validate_schema(&schema)?;
        Ok(Self {
            mem: DynMem::new(),
            mode: DynMode { schema, extractor },
            immutables: Vec::new(),
            policy: crate::inmem::policy::default_policy(),
            last_seal_at: None,
            executor,
            wal: None,
            wal_config: None,
            commit_clock: 0,
        })
    }

    /// Recover a dynamic DB by replaying WAL segments before enabling ingest.
    pub fn recover_dyn_with_wal(
        schema: SchemaRef,
        extractor: Box<dyn DynKeyExtractor>,
        executor: Arc<E>,
        wal_cfg: WalConfig,
    ) -> impl Future<Output = Result<Self, KeyExtractError>> {
        async move {
            let mut db = Self::new_dyn(schema, extractor, executor)?;
            db.set_wal_config(Some(wal_cfg.clone()));

            let replayer = Replayer::new(wal_cfg);
            let events = replayer.scan().map_err(KeyExtractError::from)?;

            let mut last_commit_ts: Option<u64> = None;
            for event in events {
                match event {
                    WalEvent::DynAppend { batch, .. } => {
                        // TODO: buffer by provisional_id once transactional replay is implemented.
                        db.ingest(batch).await?;
                    }
                    WalEvent::TxnCommit { commit_ts, .. } => {
                        last_commit_ts = Some(match last_commit_ts {
                            Some(prev) => prev.max(commit_ts),
                            None => commit_ts,
                        });
                    }
                    WalEvent::TxnBegin { .. }
                    | WalEvent::TxnAbort { .. }
                    | WalEvent::SealMarker => {
                        // TODO: implement once transactional semantics are wired up.
                    }
                }
            }

            if let Some(ts) = last_commit_ts {
                db.commit_clock = ts.saturating_add(1);
            }

            Ok(db)
        }
    }

    /// Create a dynamic DB by specifying the key column index.
    ///
    /// Validates that the column exists and its Arrow data type is supported for keys,
    /// then constructs the appropriate extractor internally.
    pub fn new_dyn_with_key_col(
        schema: SchemaRef,
        key_col: usize,
        executor: Arc<E>,
    ) -> Result<Self, KeyExtractError> {
        let fields = schema.fields();
        if key_col >= fields.len() {
            return Err(KeyExtractError::ColumnOutOfBounds(key_col, fields.len()));
        }
        let dt = fields[key_col].data_type();
        let extractor = crate::record::extract::dyn_extractor_for_field(key_col, dt)?;
        Self::new_dyn(schema, extractor, executor)
    }

    /// Create a dynamic DB by specifying the key field name.
    ///
    /// Looks up the column index by name and delegates to `new_dyn_with_key_col`.
    pub fn new_dyn_with_key_name(
        schema: SchemaRef,
        key_field: &str,
        executor: Arc<E>,
    ) -> Result<Self, KeyExtractError> {
        let fields = schema.fields();
        let Some((idx, _)) = fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == key_field)
        else {
            return Err(KeyExtractError::NoSuchField {
                name: key_field.to_string(),
            });
        };
        Self::new_dyn_with_key_col(schema, idx, executor)
    }

    /// Create a dynamic DB by inspecting Arrow metadata to find the key field(s).
    ///
    /// Priority:
    /// - Field-level: fields with metadata `tonbo.key = "true"` (single) or numeric ordinals like
    ///   `"1"`, `"2"` for composite keys (lexicographic order by ordinal).
    /// - Schema-level fallback: schema metadata `tonbo.keys` as a single name (e.g., `"id"`) or a
    ///   JSON-like array of names (e.g., `"[\"user_id\",\"ts\"]"`).
    ///
    /// Returns an error if no key is defined, a referenced field is missing, or multiple
    /// field-level markers are present without numeric ordinals.
    pub fn new_dyn_from_metadata(
        schema: SchemaRef,
        executor: Arc<E>,
    ) -> Result<Self, KeyExtractError> {
        // Helpers
        fn is_truthy(s: &str) -> bool {
            matches!(s, "true" | "TRUE" | "True" | "yes" | "YES" | "Yes")
        }
        fn parse_names_list(s: &str) -> Vec<String> {
            let t = s.trim();
            if t.starts_with('[') && t.ends_with(']') {
                let inner = &t[1..t.len() - 1];
                inner
                    .split(',')
                    .map(|p| p.trim().trim_matches('"').to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            } else {
                vec![t.trim_matches('"').to_string()]
            }
        }

        let fields = schema.fields();

        // 1) Field-level markers: collect (ord, idx) for any field with tonbo.key
        let mut marks: Vec<(Option<u32>, usize)> = Vec::new();
        for (i, f) in fields.iter().enumerate() {
            let md: &HashMap<String, String> = f.metadata();
            if let Some(v) = md.get("tonbo.key") {
                let v = v.trim();
                if let Ok(ord) = v.parse::<u32>() {
                    marks.push((Some(ord), i));
                } else if is_truthy(v) {
                    marks.push((None, i));
                }
            }
        }
        if !marks.is_empty() {
            if marks.len() == 1 {
                let idx = marks[0].1;
                return Self::new_dyn_with_key_col(schema, idx, executor);
            }
            // Composite: require numeric ordinals for disambiguation
            if marks.iter().any(|(o, _)| o.is_none()) {
                return Err(KeyExtractError::NoSuchField {
                    name: "multiple tonbo.key markers require numeric ordinals".to_string(),
                });
            }
            marks.sort_by_key(|(o, _)| o.unwrap());
            let mut parts: Vec<Box<dyn DynKeyExtractor>> = Vec::with_capacity(marks.len());
            for (_, idx) in marks.into_iter() {
                let dt = fields[idx].data_type();
                let ex = crate::record::extract::dyn_extractor_for_field(idx, dt)?;
                parts.push(ex);
            }
            let extractor = Box::new(crate::record::extract::CompositeDynExtractor::new(parts))
                as Box<dyn DynKeyExtractor>;
            return Self::new_dyn(schema, extractor, executor);
        }

        // 2) Schema-level fallback: tonbo.keys = "name" | "[\"a\",\"b\"]"
        let smd: &HashMap<String, String> = schema.metadata();
        if let Some(namev) = smd.get("tonbo.keys") {
            let names = parse_names_list(namev);
            if names.is_empty() {
                return Err(KeyExtractError::NoSuchField {
                    name: "tonbo.keys[]".to_string(),
                });
            }
            if names.len() == 1 {
                let key_name = &names[0];
                let Some((idx, _)) = fields
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name() == key_name)
                else {
                    return Err(KeyExtractError::NoSuchField {
                        name: key_name.to_string(),
                    });
                };
                return Self::new_dyn_with_key_col(schema, idx, executor);
            } else {
                // Composite schema-level
                let mut parts: Vec<Box<dyn DynKeyExtractor>> = Vec::with_capacity(names.len());
                for n in names.iter() {
                    let Some((idx, f)) = fields.iter().enumerate().find(|(_, f)| f.name() == n)
                    else {
                        return Err(KeyExtractError::NoSuchField { name: n.clone() });
                    };
                    let dt = f.data_type();
                    let ex = crate::record::extract::dyn_extractor_for_field(idx, dt)?;
                    parts.push(ex);
                }
                let extractor = Box::new(crate::record::extract::CompositeDynExtractor::new(parts))
                    as Box<dyn DynKeyExtractor>;
                return Self::new_dyn(schema, extractor, executor);
            }
        }

        // Nothing found
        Err(KeyExtractError::NoSuchField {
            name: "<tonbo.key|tonbo.keys>".to_string(),
        })
    }

    /// Insert a dynamic `RecordBatch`; last writer wins per key.
    pub fn insert_batch(&mut self, batch: RecordBatch) -> Result<(), KeyExtractError> {
        if self.mode.schema.as_ref() != batch.schema().as_ref() {
            return Err(KeyExtractError::SchemaMismatch {
                expected: self.mode.schema.clone(),
                actual: batch.schema(),
            });
        }
        self.mem.insert_batch(self.mode.extractor.as_ref(), batch)?;
        self.maybe_seal_after_insert()?;
        Ok(())
    }

    fn maybe_seal_after_insert(&mut self) -> Result<(), KeyExtractError> {
        let since = self.last_seal_at.map(|t| t.elapsed());
        let stats = self.mem.build_stats(since);
        if let SealDecision::Seal(_reason) = self.policy.evaluate(&stats) {
            let batches = self.mem.take_attached_batches();
            for b in batches {
                let seg = segment_from_batch_with_extractor(b, self.mode.extractor.as_ref())?;
                self.immutables.push(seg);
            }
            self.last_seal_at = Some(Instant::now());
        }
        Ok(())
    }
}

impl<E> DB<DynMode, E>
where
    E: Executor + Timer,
{
    /// Scan the dynamic mutable memtable over key ranges, yielding owned dynamic rows.
    pub fn scan_mutable_rows<'a>(
        &'a self,
        ranges: &'a RangeSet<KeyDyn>,
    ) -> impl Iterator<Item = typed_arrow_dyn::DynRow> + 'a {
        self.mem.scan_rows(ranges)
    }
}

// Methods common to all modes
impl<M: Mode, E: Executor + Timer> DB<M, E> {
    /// Unified ingestion entry point using `Insertable<M>` implementors.
    pub fn ingest<'a, I>(
        &'a mut self,
        input: I,
    ) -> impl Future<Output = Result<(), KeyExtractError>> + 'a
    where
        I: Insertable<M> + 'a,
    {
        input.insert_into(self)
    }

    /// Ingest many items implementing `Insertable<M>`.
    pub fn ingest_many<'a, I>(
        &'a mut self,
        inputs: I,
    ) -> impl Future<Output = Result<(), KeyExtractError>> + 'a
    where
        I: IntoIterator + 'a,
        I::Item: Insertable<M>,
    {
        async move {
            for item in inputs {
                item.insert_into(self).await?;
            }
            Ok(())
        }
    }

    /// Approximate bytes used by keys in the mutable memtable.
    pub fn approx_mutable_bytes(&self) -> usize {
        <M::Mutable as MutableLayout<M::Key>>::approx_bytes(&self.mem)
    }

    /// Attach WAL configuration prior to enabling durability.
    pub fn with_wal_config(mut self, cfg: WalConfig) -> Self {
        self.wal_config = Some(cfg);
        self
    }

    /// Access the configured WAL settings, if any.
    pub fn wal_config(&self) -> Option<&WalConfig> {
        self.wal_config.as_ref()
    }

    /// Access the executor powering async subsystems.
    pub(crate) fn executor(&self) -> &Arc<E> {
        &self.executor
    }

    /// Allocate the next commit timestamp for WAL/autocommit flows.
    pub(crate) fn next_commit_ts(&mut self) -> u64 {
        let ts = self.commit_clock;
        self.commit_clock = self.commit_clock.saturating_add(1);
        ts
    }

    /// Access the active WAL handle, if any.
    pub(crate) fn wal_handle(&self) -> Option<&WalHandle<E>> {
        self.wal.as_ref()
    }

    /// Replace the WAL handle (used by WAL extension methods).
    pub(crate) fn set_wal_handle(&mut self, handle: Option<WalHandle<E>>) {
        self.wal = handle;
    }

    /// Replace the stored WAL configuration.
    pub(crate) fn set_wal_config(&mut self, cfg: Option<WalConfig>) {
        self.wal_config = cfg;
    }

    /// Number of immutable segments attached to this DB (oldest..newest).
    pub fn num_immutable_segments(&self) -> usize {
        self.immutables.len()
    }

    // Key-only merged scans have been removed.
}

// Segment management (generic, zero-cost)
impl<M: Mode, E: Executor + Timer> DB<M, E> {
    #[allow(dead_code)]
    pub(crate) fn add_immutable(&mut self, seg: Immutable<M>) {
        self.immutables.push(seg);
    }

    /// Set or replace the sealing policy used by this DB.
    pub fn set_seal_policy(&mut self, policy: Box<dyn SealPolicy + Send + Sync>) {
        self.policy = policy;
    }
}

/// A unified ingestion interface implemented per mode.
///
/// Today only the dynamic mode implements this trait. Typed-mode support can
/// be added back by implementing `Insertable` for the future typed inputs.
pub trait Insertable<M: Mode>: Sized {
    /// Insert this value into the provided `DB` in mode `M`.
    ///
    /// Returns `Ok(())` on success, or a `KeyExtractError` for dynamic mode
    /// schema/key extraction issues.
    fn insert_into<'a, E>(
        self,
        db: &'a mut DB<M, E>,
    ) -> impl Future<Output = Result<(), KeyExtractError>> + 'a
    where
        E: Executor + Timer;
}

// Dynamic mode: single RecordBatch
impl Insertable<DynMode> for RecordBatch {
    fn insert_into<'a, E>(
        self,
        db: &'a mut DB<DynMode, E>,
    ) -> impl Future<Output = Result<(), KeyExtractError>> + 'a
    where
        E: Executor + Timer,
    {
        async move {
            if db.mode.schema.as_ref() != self.schema().as_ref() {
                return Err(KeyExtractError::SchemaMismatch {
                    expected: db.mode.schema.clone(),
                    actual: self.schema(),
                });
            }
            let has_wal = db.wal_handle().is_some();
            let commit_ts = has_wal.then(|| db.next_commit_ts());
            let wal_handle = if has_wal {
                db.wal_handle().cloned()
            } else {
                None
            };
            if let (Some(handle), Some(ts)) = (wal_handle, commit_ts) {
                let payload = WalPayload::DynBatch {
                    batch: self.clone(),
                    commit_ts: ts,
                };
                // TODO: await the WAL ticket once durability handling lands.
                let _ticket = handle
                    .submit(payload)
                    .await
                    .map_err(KeyExtractError::from)?;
            }
            db.mem.insert_batch(db.mode.extractor.as_ref(), self)?;
            db.maybe_seal_after_insert()?;
            Ok(())
        }
    }
}

// Dynamic mode: Vec of RecordBatch
impl Insertable<DynMode> for Vec<RecordBatch> {
    fn insert_into<'a, E>(
        self,
        db: &'a mut DB<DynMode, E>,
    ) -> impl Future<Output = Result<(), KeyExtractError>> + 'a
    where
        E: Executor + Timer,
    {
        async move {
            for batch in self.into_iter() {
                if db.mode.schema.as_ref() != batch.schema().as_ref() {
                    return Err(KeyExtractError::SchemaMismatch {
                        expected: db.mode.schema.clone(),
                        actual: batch.schema(),
                    });
                }
                let has_wal = db.wal_handle().is_some();
                let commit_ts = has_wal.then(|| db.next_commit_ts());
                let wal_handle = if has_wal {
                    db.wal_handle().cloned()
                } else {
                    None
                };
                if let (Some(handle), Some(ts)) = (wal_handle, commit_ts) {
                    let payload = WalPayload::DynBatch {
                        batch: batch.clone(),
                        commit_ts: ts,
                    };
                    let _ticket = handle
                        .submit(payload)
                        .await
                        .map_err(KeyExtractError::from)?;
                }
                db.mem.insert_batch(db.mode.extractor.as_ref(), batch)?;
                db.maybe_seal_after_insert()?;
            }
            Ok(())
        }
    }
}

/// Opaque dynamic mutable store for dynamic-mode DBs.
///
/// This wraps the internal `DynLayout` to avoid exposing private types via the
/// public `Mode` trait while preserving performance and behavior.
pub struct DynMem(pub(crate) DynLayout);

impl DynMem {
    pub(crate) fn new() -> Self {
        Self(DynLayout::new())
    }

    pub(crate) fn insert_batch(
        &mut self,
        extractor: &dyn DynKeyExtractor,
        batch: RecordBatch,
    ) -> Result<(), KeyExtractError> {
        self.0.insert_batch(extractor, batch)
    }

    pub(crate) fn take_attached_batches(&mut self) -> Vec<RecordBatch> {
        self.0.take_attached_batches()
    }

    pub(crate) fn scan_rows<'t, 's>(&'t self, ranges: &'s RangeSet<KeyDyn>) -> DynRowScan<'t, 's> {
        self.0.scan_rows(ranges)
    }
}

impl MutableLayout<KeyDyn> for DynMem {
    fn approx_bytes(&self) -> usize {
        self.0.approx_bytes()
    }
}

impl StatsProvider for DynMem {
    fn build_stats(
        &self,
        since_last_seal: Option<std::time::Duration>,
    ) -> crate::inmem::policy::MemStats {
        self.0.build_stats(since_last_seal)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fusio::executor::BlockingExecutor;
    use futures::executor::block_on;
    use typed_arrow::arrow_schema::{DataType, Field, Schema};
    use typed_arrow_dyn::{DynCell, DynRow};
    use typed_arrow_unified::SchemaLike;

    use super::*;
    use crate::inmem::policy::BatchesThreshold;

    #[test]
    fn dynamic_seal_on_batches_threshold() {
        // Build a simple schema: id: Utf8 (key), v: Int32
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        // Build one batch with two rows
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("valid dyn rows");

        let mut db =
            DB::new_dyn_with_key_name(schema.clone(), "id", Arc::new(BlockingExecutor::default()))
                .expect("schema ok");
        db.set_seal_policy(Box::new(BatchesThreshold { batches: 1 }));
        assert_eq!(db.num_immutable_segments(), 0);
        block_on(db.ingest(batch)).expect("insert batch");
        assert_eq!(db.num_immutable_segments(), 1);
    }

    #[test]
    fn dynamic_new_from_metadata_field_marker() {
        use std::collections::HashMap;
        // Schema: mark id with field-level metadata tonbo.key = true
        let mut fm = HashMap::new();
        fm.insert("tonbo.key".to_string(), "true".to_string());
        let f_id = Field::new("id", DataType::Utf8, false).with_metadata(fm);
        let f_v = Field::new("v", DataType::Int32, false);
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]));
        let mut db =
            DB::new_dyn_from_metadata(schema.clone(), Arc::new(BlockingExecutor::default()))
                .expect("metadata key");

        // Build one batch and insert to ensure extractor wired
        let rows = vec![
            DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("valid dyn rows");
        block_on(db.ingest(batch)).expect("insert via metadata");
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[test]
    fn dynamic_new_from_metadata_schema_level() {
        use std::collections::HashMap;
        let f_id = Field::new("id", DataType::Utf8, false);
        let f_v = Field::new("v", DataType::Int32, false);
        let mut sm = HashMap::new();
        sm.insert("tonbo.keys".to_string(), "id".to_string());
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_v]).with_metadata(sm));
        let mut db =
            DB::new_dyn_from_metadata(schema.clone(), Arc::new(BlockingExecutor::default()))
                .expect("schema metadata key");

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("x".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("y".into())), Some(DynCell::I32(2))]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("valid dyn rows");
        block_on(db.ingest(batch)).expect("insert via metadata");
        assert_eq!(db.num_immutable_segments(), 0);
    }

    #[test]
    fn dynamic_new_from_metadata_conflicts_and_missing() {
        use std::collections::HashMap;
        // Conflict: two fields marked as key
        let mut fm1 = HashMap::new();
        fm1.insert("tonbo.key".to_string(), "true".to_string());
        let mut fm2 = HashMap::new();
        fm2.insert("tonbo.key".to_string(), "1".to_string());
        let f1 = Field::new("id1", DataType::Utf8, false).with_metadata(fm1);
        let f2 = Field::new("id2", DataType::Utf8, false).with_metadata(fm2);
        let schema_conflict = std::sync::Arc::new(Schema::new(vec![f1, f2]));
        assert!(
            DB::new_dyn_from_metadata(schema_conflict, Arc::new(BlockingExecutor::default()))
                .is_err()
        );

        // Missing: no markers at field or schema level
        let schema_missing = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        assert!(
            DB::new_dyn_from_metadata(schema_missing, Arc::new(BlockingExecutor::default()))
                .is_err()
        );
    }

    #[test]
    fn dynamic_composite_from_field_ordinals_and_scan() {
        use std::collections::HashMap;
        // Fields: id (Utf8, ord 1), ts (Int64, ord 2), v (Int32)
        let mut m1 = HashMap::new();
        m1.insert("tonbo.key".to_string(), "1".to_string());
        let mut m2 = HashMap::new();
        m2.insert("tonbo.key".to_string(), "2".to_string());
        let f_id = Field::new("id", DataType::Utf8, false).with_metadata(m1);
        let f_ts = Field::new("ts", DataType::Int64, false).with_metadata(m2);
        let f_v = Field::new("v", DataType::Int32, false);
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]));
        let mut db =
            DB::new_dyn_from_metadata(schema.clone(), Arc::new(BlockingExecutor::default()))
                .expect("composite field metadata");

        let rows = vec![
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(10)),
                Some(DynCell::I32(1)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(5)),
                Some(DynCell::I32(2)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I64(1)),
                Some(DynCell::I32(3)),
            ]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("valid dyn rows");
        block_on(db.ingest(batch)).expect("insert batch");

        use std::ops::Bound as B;
        let lo = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(5i64)]);
        let hi = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(10i64)]);
        let rs = RangeSet::from_ranges(vec![crate::scan::KeyRange::new(
            B::Included(lo),
            B::Included(hi),
        )]);
        let got: Vec<(String, i64)> = db
            .scan_mutable_rows(&rs)
            .map(|row| match (&row.0[0], &row.0[1]) {
                (
                    Some(typed_arrow_dyn::DynCell::Str(s)),
                    Some(typed_arrow_dyn::DynCell::I64(ts)),
                ) => (s.clone(), *ts),
                _ => panic!("unexpected row content"),
            })
            .collect();
        assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
    }

    #[test]
    fn dynamic_composite_from_schema_list_and_scan() {
        use std::collections::HashMap;
        let f_id = Field::new("id", DataType::Utf8, false);
        let f_ts = Field::new("ts", DataType::Int64, false);
        let f_v = Field::new("v", DataType::Int32, false);
        let mut sm = HashMap::new();
        sm.insert("tonbo.keys".to_string(), "[\"id\", \"ts\"]".to_string());
        let schema = std::sync::Arc::new(Schema::new(vec![f_id, f_ts, f_v]).with_metadata(sm));
        let mut db =
            DB::new_dyn_from_metadata(schema.clone(), Arc::new(BlockingExecutor::default()))
                .expect("composite schema metadata");

        let rows = vec![
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(5)),
                Some(DynCell::I32(1)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I64(10)),
                Some(DynCell::I32(2)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I64(1)),
                Some(DynCell::I32(3)),
            ]),
        ];
        let batch: RecordBatch = schema.build_batch(rows).expect("valid dyn rows");
        block_on(db.ingest(batch)).expect("insert batch");

        use std::ops::Bound as B;
        let lo = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(1i64)]);
        let hi = KeyDyn::Tuple(vec![KeyDyn::from("a"), KeyDyn::from(10i64)]);
        let rs = RangeSet::from_ranges(vec![crate::scan::KeyRange::new(
            B::Included(lo),
            B::Included(hi),
        )]);
        let got: Vec<(String, i64)> = db
            .scan_mutable_rows(&rs)
            .map(|row| match (&row.0[0], &row.0[1]) {
                (
                    Some(typed_arrow_dyn::DynCell::Str(s)),
                    Some(typed_arrow_dyn::DynCell::I64(ts)),
                ) => (s.clone(), *ts),
                _ => panic!("unexpected row content"),
            })
            .collect();
        assert_eq!(got, vec![("a".to_string(), 5), ("a".to_string(), 10)]);
    }
}
// duplicates removed (moved above tests)
