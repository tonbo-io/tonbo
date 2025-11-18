//! Transactional write scaffolding (work in progress).
//!
//! The initial revision introduces the core data structures used to stage
//! dynamic mutations before they are committed through the WAL. Follow-up
//! patches will wire these pieces into `DB::begin_transaction`, WAL plumbing,
//! and recovery.

use std::{collections::BTreeMap, fmt, ops::Bound, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use fusio::executor::{Executor, Timer};
use fusio_manifest::snapshot::Snapshot as ManifestLease;
use lockable::AsyncLimit;
use thiserror::Error;
use typed_arrow_dyn::{DynBuilders, DynCell, DynError, DynRow};

use crate::{
    db::{DB, TxnWalPublishContext, WalFrameRange},
    extractor::{KeyExtractError, KeyProjection, row_from_batch},
    key::KeyOwned,
    manifest::{ManifestError, TableHead, TableSnapshot, VersionEdit, VersionState, WalSegmentRef},
    mode::DynMode,
    mutation::DynMutation,
    mvcc::{ReadView, Timestamp},
    scan::{KeyRange, RangeSet},
    wal::{WalError, manifest_ext},
};

/// Commit acknowledgement semantics for transactional writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitAckMode {
    /// Wait for every WAL frame to reach durability before acknowledging the commit.
    #[default]
    Strict,
    /// Return once WAL frames have been enqueued; durability progresses asynchronously.
    Fast,
}

/// Errors surfaced while constructing a read-only snapshot.
#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum SnapshotError {
    /// Manifest layer failed while capturing the snapshot.
    #[error("failed to load manifest snapshot: {0}")]
    Manifest(#[from] ManifestError),
}

/// Immutable read-only view bound to a manifest lease and MVCC snapshot timestamp.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct Snapshot {
    read_view: ReadView,
    manifest: TableSnapshot,
}

#[allow(dead_code)]
impl Snapshot {
    pub(crate) fn from_table_snapshot(read_view: ReadView, manifest: TableSnapshot) -> Self {
        Self {
            read_view,
            manifest,
        }
    }

    /// MVCC visibility guard captured when the snapshot was created.
    pub(crate) fn read_view(&self) -> ReadView {
        self.read_view
    }

    /// Lowest WAL segment that must remain durable for this snapshot.
    pub(crate) fn wal_floor(&self) -> Option<&WalSegmentRef> {
        self.manifest.head.wal_floor.as_ref()
    }

    /// Manifest head describing the table state visible to the snapshot.
    pub(crate) fn head(&self) -> &TableHead {
        &self.manifest.head
    }

    /// Latest committed version included in the snapshot, when available.
    pub(crate) fn latest_version(&self) -> Option<&VersionState> {
        self.manifest.latest_version.as_ref()
    }

    /// Underlying manifest lease keeping table metadata stable while the snapshot is alive.
    pub(crate) fn manifest_snapshot(&self) -> &ManifestLease {
        &self.manifest.manifest_snapshot
    }

    /// Full manifest payload retained by the snapshot for downstream consumers.
    pub(crate) fn table_snapshot(&self) -> &TableSnapshot {
        &self.manifest
    }
}

/// In-memory staging buffer tracking mutations by primary key.
#[allow(dead_code)]
pub(crate) struct StagedMutations {
    /// Snapshot timestamp guarding conflict detection for this transaction.
    snapshot_ts: Timestamp,
    /// Per-key mutation map preserving deterministic commit ordering.
    entries: BTreeMap<KeyOwned, DynMutation<DynRow, ()>>,
}

impl fmt::Debug for StagedMutations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StagedMutations")
            .field("snapshot_ts", &self.snapshot_ts)
            .field("entries", &self.entries.len())
            .finish()
    }
}

#[allow(dead_code)]
impl StagedMutations {
    /// Create a new empty staging buffer tied to the supplied snapshot timestamp.
    pub(crate) fn new(snapshot_ts: Timestamp) -> Self {
        Self {
            snapshot_ts,
            entries: BTreeMap::new(),
        }
    }

    /// Access the snapshot timestamp captured when the transaction began.
    pub(crate) fn snapshot_ts(&self) -> Timestamp {
        self.snapshot_ts
    }

    /// Returns `true` when no mutations have been staged.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of staged entries queued for commit.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Stage an upsert mutation for `key`, replacing any prior staged value.
    pub(crate) fn upsert(&mut self, key: KeyOwned, row: DynRow) {
        self.entries.insert(key, DynMutation::Upsert(row));
    }

    /// Stage a delete mutation for `key`, overwriting any previous staged value.
    pub(crate) fn delete(&mut self, key: KeyOwned) {
        self.entries.insert(key, DynMutation::Delete(()));
    }

    /// Iterate over staged entries in key order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&KeyOwned, &DynMutation<DynRow, ()>)> {
        self.entries.iter()
    }

    /// Return the staged mutation for `key`, if any.
    pub(crate) fn get(&self, key: &KeyOwned) -> Option<&DynMutation<DynRow, ()>> {
        self.entries.get(key)
    }

    fn into_mutations(self) -> (Vec<DynRow>, Vec<KeyOwned>) {
        let mut upserts = Vec::with_capacity(self.entries.len());
        let mut deletes = Vec::new();
        for (key, mutation) in self.entries {
            match mutation {
                DynMutation::Upsert(row) => upserts.push(row),
                DynMutation::Delete(()) => deletes.push(key),
            }
        }
        (upserts, deletes)
    }
}
/// Builder-style transaction used by the dynamic mode.
pub struct Transaction {
    schema: SchemaRef,
    delete_schema: SchemaRef,
    extractor: Arc<dyn KeyProjection>,
    key_components: usize,
    _snapshot: Snapshot,
    staged: StagedMutations,
    commit_ack_mode: CommitAckMode,
}

impl Transaction {
    pub(crate) fn new(
        schema: SchemaRef,
        delete_schema: SchemaRef,
        extractor: Arc<dyn KeyProjection>,
        snapshot: Snapshot,
        commit_ack_mode: CommitAckMode,
    ) -> Self {
        let snapshot_ts = snapshot.read_view().read_ts();
        let key_components = extractor.key_indices().len();
        Self {
            schema,
            delete_schema,
            extractor,
            key_components,
            staged: StagedMutations::new(snapshot_ts),
            _snapshot: snapshot,
            commit_ack_mode,
        }
    }

    /// Snapshot timestamp guarding conflict detection for this transaction.
    pub fn read_ts(&self) -> Timestamp {
        self.staged.snapshot_ts()
    }

    /// Returns `true` when no mutations have been staged.
    pub fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }

    /// Number of staged mutations.
    pub fn len(&self) -> usize {
        self.staged.len()
    }

    /// Abort the transaction, discarding any staged mutations without touching the database.
    pub fn abort(self) {}

    /// Stage an upsert mutation built from a dynamic row that matches the DB schema.
    pub fn upsert(&mut self, row: DynRow) -> Result<(), TransactionError> {
        let key = self.project_key(&row)?;
        self.staged.upsert(key, row);
        Ok(())
    }

    /// Stage multiple dynamic rows produced outside the transaction helper.
    pub fn upsert_rows<I>(&mut self, rows: I) -> Result<(), TransactionError>
    where
        I: IntoIterator<Item = DynRow>,
    {
        for row in rows.into_iter() {
            self.upsert(row)?;
        }
        Ok(())
    }

    /// Stage every row from `batch`, ensuring the schema matches this transaction.
    pub fn upsert_batch(&mut self, batch: &RecordBatch) -> Result<(), TransactionError> {
        self.ensure_batch_schema(&batch.schema())?;
        for row_idx in 0..batch.num_rows() {
            let row = row_from_batch(batch, row_idx).map_err(TransactionError::KeyExtract)?;
            self.upsert(row)?;
        }
        Ok(())
    }

    /// Stage a logical delete for `key`.
    pub fn delete<K>(&mut self, key: K) -> Result<(), TransactionError>
    where
        K: Into<KeyOwned>,
    {
        let owned = key.into();
        self.validate_key(&owned)?;
        self.staged.delete(owned);
        Ok(())
    }

    /// Fetch the latest visible row for `key`, overlaying staged mutations.
    pub fn get<E>(
        &self,
        db: &DB<DynMode, E>,
        key: &KeyOwned,
    ) -> Result<Option<DynRow>, TransactionError>
    where
        E: Executor + Timer,
    {
        if let Some(mutation) = self.staged.get(key) {
            return Ok(match mutation {
                DynMutation::Upsert(row) => Some(clone_dyn_row(row)),
                DynMutation::Delete(_) => None,
            });
        }
        let range = RangeSet::from_ranges(vec![KeyRange::new(
            Bound::Included(key.clone()),
            Bound::Included(key.clone()),
        )]);
        let rows = self.read_mutable_rows(db, &range)?;
        Ok(rows.into_values().next())
    }

    /// Scan `ranges`, returning rows visible at the transaction snapshot with staged overlays.
    pub fn scan<E>(
        &self,
        db: &DB<DynMode, E>,
        ranges: &RangeSet<KeyOwned>,
    ) -> Result<Vec<DynRow>, TransactionError>
    where
        E: Executor + Timer,
    {
        let mut view = self.read_mutable_rows(db, ranges)?;
        self.overlay_staged(ranges, &mut view);
        Ok(view.into_values().collect())
    }

    fn project_key(&self, row: &DynRow) -> Result<KeyOwned, TransactionError> {
        let expected = self.schema.fields().len();
        if row.0.len() != expected {
            return Err(TransactionError::RowArity {
                expected,
                got: row.0.len(),
            });
        }
        let batch = build_record_batch(&self.schema, vec![DynRow(row.0.clone())])
            .map_err(TransactionError::RowEncoding)?;
        let mut keys = self
            .extractor
            .project_view(&batch, &[0])
            .map_err(TransactionError::KeyExtract)?;
        let key_row = keys.pop().ok_or(TransactionError::EmptyProjection)?;
        Ok(key_row.to_owned())
    }

    fn ensure_batch_schema(&self, schema: &SchemaRef) -> Result<(), TransactionError> {
        if schema.as_ref() != self.schema.as_ref() {
            return Err(TransactionError::SchemaMismatch {
                expected: self.schema.clone(),
                actual: schema.clone(),
            });
        }
        Ok(())
    }

    fn validate_key(&self, key: &KeyOwned) -> Result<(), TransactionError> {
        let expected = self.key_components;
        let components = key.as_row().cells();
        if components.len() != expected {
            return Err(TransactionError::KeyComponentMismatch {
                expected,
                actual: components.len(),
            });
        }
        for (component_idx, cell) in components.iter().enumerate() {
            if cell.is_none() {
                return Err(TransactionError::KeyComponentNull {
                    index: component_idx,
                });
            }
        }
        Ok(())
    }

    fn read_mutable_rows<E>(
        &self,
        db: &DB<DynMode, E>,
        ranges: &RangeSet<KeyOwned>,
    ) -> Result<BTreeMap<KeyOwned, DynRow>, TransactionError>
    where
        E: Executor + Timer,
    {
        let read_ts = self.read_ts();
        let mut out = BTreeMap::new();
        let rows = db
            .scan_mutable_rows_at(ranges, None, read_ts)
            .map_err(TransactionError::KeyExtract)?;
        for row in rows {
            let row = row.map_err(TransactionError::KeyExtract)?;
            let key = self.project_key(&row)?;
            out.insert(key, row);
        }
        for (key, row) in db
            .scan_immutable_rows_at(ranges, read_ts)
            .map_err(TransactionError::KeyExtract)?
        {
            out.entry(key).or_insert(row);
        }
        Ok(out)
    }

    fn overlay_staged(&self, ranges: &RangeSet<KeyOwned>, view: &mut BTreeMap<KeyOwned, DynRow>) {
        for (key, mutation) in self.staged.iter() {
            if !ranges.contains(key) {
                continue;
            }
            match mutation {
                DynMutation::Upsert(row) => {
                    view.insert(key.clone(), clone_dyn_row(row));
                }
                DynMutation::Delete(_) => {
                    view.remove(key);
                }
            }
        }
    }

    /// Commit the staged mutations into the supplied DB, ensuring WAL durability first.
    pub async fn commit<E>(self, db: &mut DB<DynMode, E>) -> Result<(), TransactionCommitError>
    where
        E: Executor + Timer + 'static,
    {
        let Transaction {
            schema,
            delete_schema,
            extractor: _,
            key_components,
            staged,
            _snapshot,
            commit_ack_mode,
        } = self;

        if staged.is_empty() {
            return Ok(());
        }

        let snapshot_ts = staged.snapshot_ts();
        let lock_map = db.key_locks().clone();
        let mut key_guards = Vec::with_capacity(staged.len());
        for (key, _) in staged.iter() {
            let guard = lock_map
                .async_lock(key.clone(), AsyncLimit::no_limit())
                .await
                .expect("lock map guards are infallible");
            key_guards.push(guard);
        }

        for (key, _) in staged.iter() {
            if db.mutable_has_conflict(key, snapshot_ts)
                || db.immutable_has_conflict(key, snapshot_ts)
            {
                return Err(TransactionCommitError::WriteConflict(key.clone()));
            }
        }

        let wal = db
            .wal_handle()
            .cloned()
            .ok_or(TransactionCommitError::WalDisabled)?;

        let commit_ts = db.next_commit_ts();
        let provisional_id = wal.next_provisional_id();
        let prev_live_floor = db.wal_live_frame_floor();

        let (rows, delete_keys) = staged.into_mutations();
        let mut upsert_payload = if rows.is_empty() {
            None
        } else {
            let batch =
                build_record_batch(&schema, rows).map_err(TransactionCommitError::RowEncoding)?;
            Some(batch)
        };
        let mut delete_payload = if delete_keys.is_empty() {
            None
        } else {
            Some(build_delete_batch(
                &delete_schema,
                key_components,
                delete_keys,
                commit_ts,
            )?)
        };

        let upsert_refs = upsert_payload.as_ref();
        let delete_ref = delete_payload.as_ref();
        let tickets =
            write_wal_transaction(&wal, provisional_id, upsert_refs, delete_ref, commit_ts).await?;

        match commit_ack_mode {
            CommitAckMode::Strict => {
                let publish_ctx = db.txn_publish_context(prev_live_floor);
                let wal_range = tickets.await_range().await?;
                apply_staged_payloads(db, upsert_payload.take(), delete_payload.take(), commit_ts)?;
                publish_ctx.finalize(wal_range).await?;
            }
            CommitAckMode::Fast => {
                apply_staged_payloads(db, upsert_payload.take(), delete_payload.take(), commit_ts)?;
                let publish_ctx = db.txn_publish_context(prev_live_floor);
                let executor = Arc::clone(db.executor());
                spawn_publish_task(executor, publish_ctx, tickets);
            }
        }

        drop(key_guards);
        Ok(())
    }
}

fn build_record_batch(schema: &SchemaRef, rows: Vec<DynRow>) -> Result<RecordBatch, DynError> {
    let mut builders = DynBuilders::new(schema.clone(), rows.len());
    for row in rows {
        builders.append_option_row(Some(row))?;
    }
    builders.try_finish_into_batch()
}

fn build_delete_batch(
    schema: &SchemaRef,
    expected_components: usize,
    keys: Vec<KeyOwned>,
    commit_ts: Timestamp,
) -> Result<RecordBatch, TransactionCommitError> {
    if keys.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let mut builders = DynBuilders::new(schema.clone(), keys.len());
    for key in keys {
        let components = key.as_row().cells();
        if components.len() != expected_components {
            return Err(TransactionCommitError::InvalidDeleteKey {
                expected: expected_components,
                actual: components.len(),
            });
        }
        let mut row = Vec::with_capacity(expected_components + 1);
        for (idx, cell) in components.iter().enumerate() {
            let value = cell
                .as_ref()
                .ok_or(TransactionCommitError::DeleteComponentNull { index: idx })?;
            row.push(Some(value.clone()));
        }
        row.push(Some(DynCell::U64(commit_ts.get())));
        builders
            .append_option_row(Some(DynRow(row)))
            .map_err(TransactionCommitError::RowEncoding)?;
    }
    builders
        .try_finish_into_batch()
        .map_err(TransactionCommitError::RowEncoding)
}

fn apply_staged_payloads<E>(
    db: &mut DB<DynMode, E>,
    upserts: Option<RecordBatch>,
    deletes: Option<RecordBatch>,
    commit_ts: Timestamp,
) -> Result<(), TransactionCommitError>
where
    E: Executor + Timer,
{
    if let Some(batch) = upserts {
        db.apply_committed_batch(batch, commit_ts)
            .map_err(TransactionCommitError::Apply)?;
    }
    if let Some(batch) = deletes {
        db.apply_committed_deletes(batch)
            .map_err(TransactionCommitError::Apply)?;
    }
    Ok(())
}

fn clone_dyn_row(row: &DynRow) -> DynRow {
    DynRow(row.0.clone())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::UInt64Array;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use fusio::executor::BlockingExecutor;
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        inmem::policy::BatchesThreshold, mode::DynModeConfig, mvcc::Timestamp, scan::RangeSet,
        test_util::build_batch,
    };

    async fn make_db() -> (DB<DynMode, BlockingExecutor>, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
        let executor = Arc::new(BlockingExecutor);
        let db = DB::new(config, executor).await.expect("db");
        (db, schema)
    }

    async fn ingest_rows(
        db: &mut DB<DynMode, BlockingExecutor>,
        schema: &SchemaRef,
        rows: Vec<DynRow>,
    ) {
        let tombstones = vec![false; rows.len()];
        let batch = build_batch(schema.clone(), rows).expect("batch");
        db.ingest_with_tombstones(batch, tombstones)
            .await
            .expect("ingest");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn upsert_batch_stages_rows() {
        let (db, schema) = make_db().await;
        let mut tx = db.begin_transaction().await.expect("tx");

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("k1".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("k2".into())), Some(DynCell::I32(2))]),
        ];
        let batch = build_batch(schema.clone(), rows).expect("batch");
        tx.upsert_batch(&batch).expect("stage batch");
        assert_eq!(tx.len(), 2);

        let fetched = tx
            .get(&db, &KeyOwned::from("k1"))
            .expect("get")
            .expect("row");
        match &fetched.0[1] {
            Some(DynCell::I32(v)) => assert_eq!(*v, 1),
            other => panic!("unexpected cell: {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_prefers_staged_rows() {
        let (mut db, schema) = make_db().await;
        ingest_rows(
            &mut db,
            &schema,
            vec![DynRow(vec![
                Some(DynCell::Str("k1".into())),
                Some(DynCell::I32(10)),
            ])],
        )
        .await;
        let mut tx = db.begin_transaction().await.expect("tx");
        tx.upsert(DynRow(vec![
            Some(DynCell::Str("k1".into())),
            Some(DynCell::I32(42)),
        ]))
        .expect("stage");

        let value = tx
            .get(&db, &KeyOwned::from("k1"))
            .expect("get")
            .expect("row");
        match &value.0[1] {
            Some(DynCell::I32(v)) => assert_eq!(*v, 42),
            other => panic!("unexpected cell: {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scan_merges_staged_and_deleted_rows() {
        let (mut db, schema) = make_db().await;
        ingest_rows(
            &mut db,
            &schema,
            vec![
                DynRow(vec![Some(DynCell::Str("k1".into())), Some(DynCell::I32(1))]),
                DynRow(vec![Some(DynCell::Str("k2".into())), Some(DynCell::I32(2))]),
            ],
        )
        .await;
        let mut tx = db.begin_transaction().await.expect("tx");
        tx.upsert(DynRow(vec![
            Some(DynCell::Str("k1".into())),
            Some(DynCell::I32(11)),
        ]))
        .expect("stage update");
        tx.delete(KeyOwned::from("k2")).expect("stage delete");
        tx.upsert(DynRow(vec![
            Some(DynCell::Str("k3".into())),
            Some(DynCell::I32(3)),
        ]))
        .expect("stage insert");

        let ranges = RangeSet::all();
        let rows = tx.scan(&db, &ranges).expect("scan rows");
        let keys: Vec<String> = rows
            .iter()
            .map(|row| match &row.0[0] {
                Some(DynCell::Str(v)) => v.clone(),
                other => panic!("unexpected key cell: {other:?}"),
            })
            .collect();
        assert_eq!(keys, vec!["k1".to_string(), "k3".to_string()]);

        match &rows[0].0[1] {
            Some(DynCell::I32(v)) => assert_eq!(*v, 11),
            other => panic!("unexpected value: {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delete_hides_rows_from_reads() {
        let (mut db, schema) = make_db().await;
        ingest_rows(
            &mut db,
            &schema,
            vec![DynRow(vec![
                Some(DynCell::Str("k_del".into())),
                Some(DynCell::I32(9)),
            ])],
        )
        .await;
        let mut tx = db.begin_transaction().await.expect("tx");
        tx.delete(KeyOwned::from("k_del")).expect("delete");

        assert!(
            tx.get(&db, &KeyOwned::from("k_del"))
                .expect("get")
                .is_none()
        );
        let ranges = RangeSet::all();
        assert!(tx.scan(&db, &ranges).expect("scan").is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn build_delete_batch_writes_commit_ts() {
        let (db, _schema) = make_db().await;
        let tx = db.begin_transaction().await.expect("tx");
        let key = KeyOwned::from("delete-me");
        let commit_ts = Timestamp::new(777);
        let batch = build_delete_batch(&tx.delete_schema, tx.key_components, vec![key], commit_ts)
            .expect("build delete batch");
        assert_eq!(batch.num_rows(), 1);
        let commit_idx = batch.schema().fields().len() - 1;
        let commits = batch
            .column(commit_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("commit column");
        assert_eq!(commits.value(0), commit_ts.get());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delete_rejects_wrong_key_shape() {
        let (db, _schema) = make_db().await;
        let mut tx = db.begin_transaction().await.expect("tx");
        let key = KeyOwned::tuple(vec![KeyOwned::from("k1"), KeyOwned::from("k2")]);
        let err = tx.delete(key).expect_err("delete should fail");
        match err {
            TransactionError::KeyComponentMismatch { expected, actual } => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
            }
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_reads_sealed_immutable_rows() {
        let (mut db, schema) = make_db().await;
        db.set_seal_policy(Box::new(BatchesThreshold { batches: 1 }));
        ingest_rows(
            &mut db,
            &schema,
            vec![DynRow(vec![
                Some(DynCell::Str("sealed".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .await;
        assert!(db.num_immutable_segments() >= 1);

        let tx = db.begin_transaction().await.expect("tx");
        let ranges = RangeSet::all();
        let rows = tx.scan(&db, &ranges).expect("scan immutables");
        assert_eq!(rows.len(), 1);
        match &rows[0].0[0] {
            Some(DynCell::Str(value)) => assert_eq!(value, "sealed"),
            other => panic!("unexpected key cell {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn commit_conflict_checks_immutables() {
        let (mut db, schema) = make_db().await;
        db.set_seal_policy(Box::new(BatchesThreshold { batches: 1 }));

        ingest_rows(
            &mut db,
            &schema,
            vec![DynRow(vec![
                Some(DynCell::Str("user".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .await;
        let mut tx = db.begin_transaction().await.expect("tx");

        ingest_rows(
            &mut db,
            &schema,
            vec![DynRow(vec![
                Some(DynCell::Str("user".into())),
                Some(DynCell::I32(2)),
            ])],
        )
        .await;

        tx.upsert(DynRow(vec![
            Some(DynCell::Str("user".into())),
            Some(DynCell::I32(3)),
        ]))
        .expect("stage conflicting update");

        let err = tx.commit(&mut db).await.expect_err("conflict expected");
        match err {
            TransactionCommitError::WriteConflict(key) => {
                assert_eq!(key, KeyOwned::from("user"));
            }
            other => panic!("unexpected error {other:?}"),
        }
    }
}

async fn write_wal_transaction<E>(
    wal: &crate::wal::WalHandle<E>,
    provisional_id: u64,
    upserts: Option<&RecordBatch>,
    deletes: Option<&RecordBatch>,
    commit_ts: Timestamp,
) -> Result<WalTxnTickets<E>, TransactionCommitError>
where
    E: Executor + Timer,
{
    let mut tickets = Vec::new();
    let begin_ticket = wal.txn_begin(provisional_id).await?;
    tickets.push(begin_ticket);

    let result = async {
        if let Some(batch) = upserts {
            let ticket = wal.txn_append(provisional_id, batch, commit_ts).await?;
            tickets.push(ticket);
        }
        if let Some(batch) = deletes {
            let ticket = wal.txn_append_delete(provisional_id, batch.clone()).await?;
            tickets.push(ticket);
        }
        let commit_ticket = wal.txn_commit(provisional_id, commit_ts).await?;
        tickets.push(commit_ticket);
        Ok(WalTxnTickets { tickets })
    }
    .await;

    if result.is_err() {
        let _ = wal.txn_abort(provisional_id).await;
    }

    result
}

struct WalTxnTickets<E>
where
    E: Executor + Timer,
{
    tickets: Vec<crate::wal::WalTicket<E>>,
}

impl<E> WalTxnTickets<E>
where
    E: Executor + Timer,
{
    async fn await_range(self) -> Result<WalFrameRange, TransactionCommitError> {
        let mut range = AckRange::default();
        for ticket in self.tickets {
            let ack = ticket.durable().await?;
            range.observe(ack.first_seq, ack.last_seq);
        }
        range.into_range().ok_or_else(|| {
            TransactionCommitError::Wal(WalError::Corrupt("transaction emitted no wal frames"))
        })
    }
}

#[derive(Default)]
struct AckRange {
    first: Option<u64>,
    last: Option<u64>,
}

impl AckRange {
    fn observe(&mut self, first: u64, last: u64) {
        self.first = Some(match self.first {
            Some(existing) => existing.min(first),
            None => first,
        });
        self.last = Some(match self.last {
            Some(existing) => existing.max(last),
            None => last,
        });
    }

    fn into_range(self) -> Option<WalFrameRange> {
        match (self.first, self.last) {
            (Some(first), Some(last)) => Some(WalFrameRange { first, last }),
            _ => None,
        }
    }
}

fn spawn_publish_task<E>(
    executor: Arc<E>,
    publish_ctx: TxnWalPublishContext,
    tickets: WalTxnTickets<E>,
) where
    E: Executor + Timer + 'static,
{
    executor.spawn(async move {
        match tickets.await_range().await {
            Ok(range) => {
                if let Err(err) = publish_ctx.finalize(range).await {
                    eprintln!("transaction post-commit publish failed: {err}");
                }
            }
            Err(err) => {
                eprintln!("transaction wal write failed after fast ack: {err}");
            }
        }
    });
}

impl TxnWalPublishContext {
    async fn finalize(&self, wal_range: WalFrameRange) -> Result<(), TransactionCommitError> {
        {
            let mut guard = self
                .mutable_wal_range
                .lock()
                .expect("mutable wal range lock poisoned");
            match &mut *guard {
                Some(existing) => existing.extend(&wal_range),
                None => *guard = Some(wal_range),
            }
        }

        let live_floor = match (self.prev_live_floor, Some(wal_range.first)) {
            (Some(prev), Some(current)) => Some(prev.min(current)),
            (Some(prev), None) => Some(prev),
            (None, Some(current)) => Some(current),
            (None, None) => None,
        };

        let Some(cfg) = &self.wal_config else {
            return Ok(());
        };

        let existing_floor = self
            .manifest
            .wal_floor(self.manifest_table)
            .await
            .map_err(TransactionCommitError::Manifest)?;
        let refs = manifest_ext::collect_wal_segment_refs(cfg, existing_floor.as_ref(), live_floor)
            .await
            .map_err(TransactionCommitError::Wal)?;
        if refs.is_empty() {
            return Ok(());
        }

        self.manifest
            .apply_version_edits(
                self.manifest_table,
                &[VersionEdit::SetWalSegments { segments: refs }],
            )
            .await
            .map_err(TransactionCommitError::Manifest)?;

        if let Ok(Some(floor)) = self.manifest.wal_floor(self.manifest_table).await
            && let Err(err) = manifest_ext::prune_wal_segments(cfg, &floor).await
        {
            eprintln!(
                "failed to prune wal segments below manifest floor {}: {}",
                floor.seq(),
                err
            );
        }

        Ok(())
    }
}

/// Errors raised while staging transactional mutations.
#[derive(Debug, Error)]
pub enum TransactionError {
    /// Snapshot acquisition failed.
    #[error("failed to capture snapshot: {0}")]
    Snapshot(#[from] SnapshotError),
    /// Row did not match the expected schema arity.
    #[error("row contained {got} values but schema requires {expected}")]
    RowArity {
        /// Number of values required by the schema.
        expected: usize,
        /// Number of values supplied for the row.
        got: usize,
    },
    /// Batch schema differed from the transaction schema.
    #[error("batch schema mismatch: expected {expected:?}, got {actual:?}")]
    SchemaMismatch {
        /// Schema captured when the transaction began.
        expected: SchemaRef,
        /// Schema supplied by the caller.
        actual: SchemaRef,
    },
    /// Failed to encode the supplied dynamic row.
    #[error("failed to encode dynamic row: {0}")]
    RowEncoding(#[from] DynError),
    /// Key extraction failed for the staged row.
    #[error("failed to extract key: {0}")]
    KeyExtract(#[from] KeyExtractError),
    /// Key projection unexpectedly produced no rows.
    #[error("key projection returned no rows")]
    EmptyProjection,
    /// Delete staging received the wrong number of key components.
    #[error("delete expected {expected} key components but got {actual}")]
    KeyComponentMismatch {
        /// Required component count.
        expected: usize,
        /// Supplied component count.
        actual: usize,
    },
    /// A key component unexpectedly contained a null value.
    #[error("key component {index} contained null")]
    KeyComponentNull {
        /// Component index that was null.
        index: usize,
    },
}

/// Errors raised while committing a transaction.
#[derive(Debug, Error)]
pub enum TransactionCommitError {
    /// WAL must be enabled for transactional commits.
    #[error("wal is not configured; enable durability before using transactions")]
    WalDisabled,
    /// Underlying WAL layer failed.
    #[error("wal error: {0}")]
    Wal(#[from] WalError),
    /// Failed to encode staged rows into a RecordBatch.
    #[error("failed to encode batch: {0}")]
    RowEncoding(DynError),
    /// Applying the committed batch into the mutable table failed.
    #[error("failed to apply committed batch: {0}")]
    Apply(#[from] KeyExtractError),
    /// Another transaction committed between snapshot acquisition and commit.
    #[error("write conflict on key {0:?}")]
    WriteConflict(KeyOwned),
    /// Manifest publication failed while updating WAL references.
    #[error("manifest error: {0}")]
    Manifest(#[from] ManifestError),
    /// Delete batch construction observed a mismatched key component count.
    #[error("staged delete key expected {expected} components but got {actual}")]
    InvalidDeleteKey {
        /// Number of key components defined by the table schema.
        expected: usize,
        /// Number of components observed in the staged delete key.
        actual: usize,
    },
    /// Delete batch construction observed a null key component.
    #[error("staged delete key component {index} was null")]
    DeleteComponentNull {
        /// Zero-based component index that was null.
        index: usize,
    },
}
