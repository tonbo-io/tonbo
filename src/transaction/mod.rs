//! Transactional write path: staging, conflict detection, WAL commit, and snapshot-aware scans.
//!
//! This module implements optimistic transactions for the dynamic layout: stage mutations,
//! detect conflicts against mutable/immutable state, publish through the WAL, and expose
//! snapshot-bound scans with read-your-writes semantics. It integrates with the DB’s MVCC
//! clock and manifest/WAL plumbing for durable commits.

use std::{collections::BTreeMap, fmt, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{Fields, SchemaRef};
use fusio::executor::{Executor, Timer};
use lockable::AsyncLimit;
use thiserror::Error;
use typed_arrow_dyn::{
    DynBuilders, DynCell, DynError, DynProjection, DynRow, DynRowOwned, DynRowRaw, DynSchema,
    DynViewError,
};

#[cfg(all(test, feature = "tokio"))]
use crate::manifest::{TableHead, VersionState};
use crate::{
    db::{DBError, DbInner, DynDbHandle, ScanBuilder, TxnWalPublishContext, WalFrameRange},
    extractor::{KeyExtractError, KeyProjection, row_from_batch},
    key::{KeyOwned, KeyTsViewRaw},
    manifest::{ManifestError, TableSnapshot, VersionEdit},
    mutation::DynMutation,
    mvcc::{ReadView, Timestamp},
    query::stream::StreamError,
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

/// Durability class for a transaction based on WAL availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionDurability {
    /// WAL is configured; commit is crash-durable (subject to sync policy).
    Durable,
    /// WAL is disabled; commit mutates in-memory state only and is lost on crash/restart.
    Volatile,
}

/// Errors surfaced while constructing a read-only snapshot.
#[derive(Debug, Error)]

pub enum SnapshotError {
    /// Manifest layer failed while capturing the snapshot.
    #[error("failed to load manifest snapshot: {0}")]
    Manifest(#[from] ManifestError),
}

/// Immutable read-only view bound to a manifest lease and MVCC snapshot timestamp.
#[derive(Debug, Clone)]

pub struct Snapshot {
    read_view: ReadView,
    manifest: TableSnapshot,
}

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

    /// Manifest head describing the table state visible to the snapshot.
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn head(&self) -> &TableHead {
        &self.manifest.head
    }

    /// Latest committed version included in the snapshot, when available.
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn latest_version(&self) -> Option<&VersionState> {
        self.manifest.latest_version.as_ref()
    }

    /// Full manifest payload retained by the snapshot for downstream consumers.
    pub(crate) fn table_snapshot(&self) -> &TableSnapshot {
        &self.manifest
    }

    /// Begin building a scan query using this snapshot for MVCC visibility.
    ///
    /// This is the core scan implementation - both `DB::scan()` and `Transaction::scan()`
    /// ultimately delegate to snapshot-based scanning.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = db.begin_snapshot().await?;
    /// let batches = snapshot.scan(&db)
    ///     .filter(predicate)
    ///     .collect()
    ///     .await?;
    /// ```
    pub fn scan<'a, FS, E>(&'a self, db: &'a crate::db::DB<FS, E>) -> ScanBuilder<'a, FS, E>
    where
        FS: crate::manifest::ManifestFs<E>,
        E: Executor + Timer + Clone + 'static,
        <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        ScanBuilder::from_snapshot_with_db(db, self)
    }
}

/// In-memory staging buffer tracking mutations by primary key.
struct StagedMutations {
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

    pub(crate) fn entries(&self) -> &BTreeMap<KeyOwned, DynMutation<DynRow, ()>> {
        &self.entries
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

/// Iterator-style scan over staged transaction mutations.
pub(crate) struct TransactionScan<'a> {
    staged: &'a BTreeMap<KeyOwned, DynMutation<DynRow, ()>>,
    input_schema: SchemaRef,
    input_dyn_schema: DynSchema,
    fields: Fields,
    visible_ts: Timestamp,
    iter: std::collections::btree_map::Iter<'a, KeyOwned, DynMutation<DynRow, ()>>,
    rows: Vec<DynRowOwned>,
    projection: Option<DynProjection>,
}

impl<'a> fmt::Debug for TransactionScan<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionScan")
            .field("staged_len", &self.staged.len())
            .field("visible_ts", &self.visible_ts)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) enum TransactionScanEntry {
    Row((KeyTsViewRaw, DynRowRaw)),
    Tombstone(KeyTsViewRaw),
}

impl<'a> TransactionScan<'a> {
    pub(crate) fn new(
        staged: &'a BTreeMap<KeyOwned, DynMutation<DynRow, ()>>,
        schema: &SchemaRef,
        visible_ts: Timestamp,
        projection_schema: Option<&SchemaRef>,
    ) -> Result<Self, DynViewError> {
        let input_schema = Arc::clone(schema);
        let input_dyn_schema = DynSchema::from_ref(Arc::clone(schema));
        let (fields, projection) = match projection_schema {
            Some(target) => {
                let projection = if target.as_ref() == schema.as_ref() {
                    None
                } else {
                    Some(DynProjection::from_schema(
                        schema.as_ref(),
                        target.as_ref(),
                    )?)
                };
                (target.fields().clone(), projection)
            }
            None => (schema.fields().clone(), None),
        };
        Ok(Self {
            staged,
            input_schema,
            input_dyn_schema,
            fields,
            visible_ts,
            iter: staged.iter(),
            rows: Vec::new(),
            projection,
        })
    }

    fn materialize_row(&self, row: &DynRow) -> Result<DynRowOwned, DynViewError> {
        if let Some(projection) = &self.projection {
            let mut builders = DynBuilders::new(Arc::clone(&self.input_schema), 1);
            builders
                .append_option_row(Some(clone_dyn_row(row)))
                .map_err(|err| DynViewError::Invalid {
                    column: 0,
                    path: "transaction_scan_projection".to_string(),
                    message: err.to_string(),
                })?;
            let batch = builders
                .try_finish_into_batch()
                .map_err(|err| DynViewError::Invalid {
                    column: 0,
                    path: "transaction_scan_projection".to_string(),
                    message: err.to_string(),
                })?;
            let raw = projection.project_row_raw(&self.input_dyn_schema, &batch, 0)?;
            let projected = raw.into_owned()?;
            DynRowOwned::from_dyn_row(self.fields.clone(), projected)
        } else {
            DynRowOwned::from_dyn_row(self.fields.clone(), clone_dyn_row(row))
        }
    }
}

impl<'a> Iterator for TransactionScan<'a> {
    type Item = Result<TransactionScanEntry, DynViewError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((key, mutation)) = self.iter.next() {
            match mutation {
                DynMutation::Upsert(row) => match self.materialize_row(row) {
                    Ok(owned_row) => {
                        self.rows.push(owned_row);
                        let raw_row = match self.rows.last() {
                            Some(stored) => match stored.as_raw() {
                                Ok(raw) => raw,
                                Err(err) => return Some(Err(err)),
                            },
                            None => {
                                return Some(Err(DynViewError::Invalid {
                                    column: 0,
                                    path: "transaction_scan".to_string(),
                                    message: "staged row buffer unexpectedly empty".to_string(),
                                }));
                            }
                        };
                        let view = KeyTsViewRaw::from_owned(key, self.visible_ts);
                        return Some(Ok(TransactionScanEntry::Row((view, raw_row))));
                    }
                    Err(err) => return Some(Err(err)),
                },
                DynMutation::Delete(_) => {
                    let view = KeyTsViewRaw::from_owned(key, self.visible_ts);
                    return Some(Ok(TransactionScanEntry::Tombstone(view)));
                }
            }
        }
        None
    }
}

impl From<DBError> for TransactionError {
    fn from(err: DBError) -> Self {
        match err {
            DBError::Key(inner) => TransactionError::KeyExtract(inner),
            DBError::Manifest(manifest) => {
                TransactionError::Snapshot(SnapshotError::Manifest(manifest))
            }
            DBError::Stream(stream) => TransactionError::Stream(stream),
            DBError::SsTable(sstable) => {
                TransactionError::Stream(crate::query::stream::StreamError::SsTableIo(sstable))
            }
            DBError::Snapshot(snapshot) => TransactionError::Snapshot(snapshot),
            DBError::DynView(view) => TransactionError::DynKey(view),
            other => TransactionError::Db(other),
        }
    }
}

/// Builder-style transaction used by the dynamic mode.
pub struct Transaction<FS, E>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    handle: DynDbHandle<FS, E>,
    schema: SchemaRef,
    delete_schema: SchemaRef,
    extractor: Arc<dyn KeyProjection>,
    key_components: usize,
    _snapshot: Snapshot,
    staged: StagedMutations,
    commit_ack_mode: CommitAckMode,
    durability: TransactionDurability,
}

impl<FS, E> Transaction<FS, E>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    pub(crate) fn new(
        handle: DynDbHandle<FS, E>,
        schema: SchemaRef,
        delete_schema: SchemaRef,
        extractor: Arc<dyn KeyProjection>,
        snapshot: Snapshot,
        commit_ack_mode: CommitAckMode,
        durability: TransactionDurability,
    ) -> Self {
        let snapshot_ts = snapshot.read_view().read_ts();
        let key_components = extractor.key_indices().len();
        Self {
            handle,
            schema,
            delete_schema,
            extractor,
            key_components,
            staged: StagedMutations::new(snapshot_ts),
            _snapshot: snapshot,
            commit_ack_mode,
            durability,
        }
    }

    /// Snapshot timestamp guarding conflict detection for this transaction.
    pub(crate) fn read_ts(&self) -> Timestamp {
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

    /// Durability class for this transaction.
    pub fn durability(&self) -> TransactionDurability {
        self.durability
    }

    /// Returns `true` when the transaction will write through the WAL.
    pub fn is_durable(&self) -> bool {
        matches!(self.durability, TransactionDurability::Durable)
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
    pub async fn get(&self, key: &KeyOwned) -> Result<Option<DynRow>, TransactionError> {
        if let Some(mutation) = self.staged.get(key) {
            return Ok(match mutation {
                DynMutation::Upsert(row) => Some(clone_dyn_row(row)),
                DynMutation::Delete(_) => None,
            });
        }
        let rows = self.read_mutable_rows(&*self.handle)?;
        Ok(rows.get(key).map(clone_dyn_row))
    }

    /// Begin building a scan query with fluent API.
    ///
    /// Returns a builder that allows chaining `.filter()`, `.projection()`, `.limit()`
    /// before executing the scan. The scan includes uncommitted writes from this
    /// transaction's staging buffer.
    pub fn scan(&self) -> ScanBuilder<'_, FS, E> {
        ScanBuilder::with_transaction_overlay(
            &self.handle,
            &self._snapshot,
            self.staged.entries(),
            &self.schema,
        )
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

    fn read_mutable_rows<C>(
        &self,
        db: &DbInner<C, E>,
    ) -> Result<BTreeMap<KeyOwned, DynRow>, TransactionError>
    where
        C: crate::manifest::ManifestFs<E>,
        <C as fusio::fs::Fs>::File: fusio::durability::FileCommit,
    {
        let read_ts = self.read_ts();
        let mut out = BTreeMap::new();
        let rows = db
            .scan_mutable_rows_at(read_ts)
            .map_err(TransactionError::KeyExtract)?;
        for (key_row, row) in rows {
            out.insert(key_row.to_owned(), row);
        }
        for (key, row) in db
            .scan_immutable_rows_at(read_ts)
            .map_err(TransactionError::KeyExtract)?
        {
            out.entry(key.to_owned()).or_insert(row);
        }
        Ok(out)
    }

    /// Commit the staged mutations into the supplied DB, ensuring WAL durability first.
    pub async fn commit(self) -> Result<(), TransactionCommitError>
    where
        E: Executor + Timer + Clone + 'static,
    {
        let Transaction {
            handle,
            schema,
            delete_schema,
            extractor: _,
            key_components,
            staged,
            _snapshot,
            commit_ack_mode,
            durability,
        } = self;

        if staged.is_empty() {
            return Ok(());
        }

        let snapshot_ts = staged.snapshot_ts();
        let db = handle;
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

        let commit_ts = db.next_commit_ts();
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

        if let Some(wal) = db.wal_handle().cloned() {
            let provisional_id = wal.next_provisional_id();
            let prev_live_floor = db.wal_live_frame_floor();
            let upsert_refs = upsert_payload.as_ref();
            let delete_ref = delete_payload.as_ref();
            let tickets =
                write_wal_transaction(&wal, provisional_id, upsert_refs, delete_ref, commit_ts)
                    .await?;

            match commit_ack_mode {
                CommitAckMode::Strict => {
                    let publish_ctx = db.txn_publish_context(prev_live_floor);
                    let wal_range = tickets.await_range().await?;
                    // Record WAL range BEFORE apply that may trigger auto-seal.
                    // If we record after, a sealed segment would miss this transaction's frames.
                    publish_ctx.record_wal_range(&wal_range);
                    apply_staged_payloads(
                        &*db,
                        upsert_payload.take(),
                        delete_payload.take(),
                        commit_ts,
                    )?;
                    publish_ctx.finalize_manifest(wal_range).await?;
                }
                CommitAckMode::Fast => {
                    // NOTE: Fast mode has a known limitation - if auto-seal triggers during
                    // apply_staged_payloads, the sealed segment's WAL range won't include this
                    // transaction's frames (they're recorded asynchronously). This is acceptable
                    // for Fast mode as it prioritizes latency over strict durability ordering.
                    apply_staged_payloads(
                        &*db,
                        upsert_payload.take(),
                        delete_payload.take(),
                        commit_ts,
                    )?;
                    let publish_ctx = db.txn_publish_context(prev_live_floor);
                    let executor = Arc::clone(db.executor());
                    spawn_publish_task(executor, publish_ctx, tickets);
                }
            }
        } else {
            if matches!(durability, TransactionDurability::Durable) {
                eprintln!(
                    "wal unavailable during commit; proceeding with volatile apply (data not \
                     crash safe)"
                );
            } else {
                eprintln!(
                    "transaction commit applied without WAL; changes are volatile until process \
                     exits"
                );
            }
            apply_staged_payloads(
                &*db,
                upsert_payload.take(),
                delete_payload.take(),
                commit_ts,
            )?;
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

fn apply_staged_payloads<FS, E>(
    db: &DbInner<FS, E>,
    upserts: Option<RecordBatch>,
    deletes: Option<RecordBatch>,
    commit_ts: Timestamp,
) -> Result<(), TransactionCommitError>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
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

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use fusio::{executor::NoopExecutor, mem::fs::InMemoryFs};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        db::DB,
        inmem::policy::BatchesThreshold,
        mode::DynModeConfig,
        mvcc::Timestamp,
        query::{Expr, ScalarValue},
        test::build_batch,
    };

    type TestTx = Transaction<InMemoryFs, NoopExecutor>;
    type TestDb = DB<InMemoryFs, NoopExecutor>;

    async fn make_db() -> (TestDb, SchemaRef) {
        make_db_with_policy(None).await
    }

    async fn make_db_with_policy(
        policy: Option<Arc<dyn crate::inmem::policy::SealPolicy + Send + Sync>>,
    ) -> (TestDb, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let extractor =
            crate::extractor::projection_for_field(schema.clone(), 0).expect("extractor");
        let config = DynModeConfig::new(schema.clone(), extractor).expect("config");
        let executor = Arc::new(NoopExecutor);
        let db = match policy {
            Some(p) => DB::new_with_policy(config, executor, p).await.expect("db"),
            None => DB::new(config, executor).await.expect("db"),
        };
        (db, schema)
    }

    async fn ingest_rows(db: &TestDb, schema: &SchemaRef, rows: Vec<DynRow>) {
        let tombstones = vec![false; rows.len()];
        let batch = build_batch(schema.clone(), rows).expect("batch");
        db.inner()
            .ingest_with_tombstones(batch, tombstones)
            .await
            .expect("ingest");
    }

    fn all_rows_predicate() -> Expr {
        Expr::gt("v", ScalarValue::from(i64::MIN))
    }

    /// Helper to extract (id, value) pairs from scan result batches.
    fn extract_rows(batches: &[RecordBatch]) -> Vec<(String, i32)> {
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id col");
            let vals = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("v col");
            for (id, v) in ids.iter().zip(vals.iter()) {
                if let (Some(id), Some(v)) = (id, v) {
                    rows.push((id.to_string(), v));
                }
            }
        }
        rows
    }

    #[tokio::test(flavor = "current_thread")]
    async fn upsert_batch_stages_rows() {
        let (db, schema) = make_db().await;
        let mut tx: TestTx = db.begin_transaction().await.expect("tx");

        let rows = vec![
            DynRow(vec![Some(DynCell::Str("k1".into())), Some(DynCell::I32(1))]),
            DynRow(vec![Some(DynCell::Str("k2".into())), Some(DynCell::I32(2))]),
        ];
        let batch = build_batch(schema.clone(), rows).expect("batch");
        tx.upsert_batch(&batch).expect("stage batch");
        assert_eq!(tx.len(), 2);

        let fetched = tx
            .get(&KeyOwned::from("k1"))
            .await
            .expect("get")
            .expect("row");
        match &fetched.0[1] {
            Some(DynCell::I32(v)) => assert_eq!(*v, 1),
            other => panic!("unexpected cell: {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_prefers_staged_rows() {
        let (db, schema) = make_db().await;
        ingest_rows(
            &db,
            &schema,
            vec![DynRow(vec![
                Some(DynCell::Str("k1".into())),
                Some(DynCell::I32(10)),
            ])],
        )
        .await;
        let mut tx: TestTx = db.begin_transaction().await.expect("tx");
        tx.upsert(DynRow(vec![
            Some(DynCell::Str("k1".into())),
            Some(DynCell::I32(42)),
        ]))
        .expect("stage");

        let value = tx
            .get(&KeyOwned::from("k1"))
            .await
            .expect("get")
            .expect("row");
        match &value.0[1] {
            Some(DynCell::I32(v)) => assert_eq!(*v, 42),
            other => panic!("unexpected cell: {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scan_merges_staged_and_deleted_rows() {
        let (db, schema) = make_db().await;
        ingest_rows(
            &db,
            &schema,
            vec![
                DynRow(vec![Some(DynCell::Str("k1".into())), Some(DynCell::I32(1))]),
                DynRow(vec![Some(DynCell::Str("k2".into())), Some(DynCell::I32(2))]),
            ],
        )
        .await;
        let mut tx: TestTx = db.begin_transaction().await.expect("tx");
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

        let predicate = all_rows_predicate();
        let batches = tx
            .scan()
            .filter(predicate)
            .collect()
            .await
            .expect("scan rows");
        let rows = extract_rows(&batches);
        let keys: Vec<String> = rows.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec!["k1".to_string(), "k3".to_string()]);
        assert_eq!(rows[0].1, 11);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delete_hides_rows_from_reads() {
        let (db, schema) = make_db().await;
        ingest_rows(
            &db,
            &schema,
            vec![DynRow(vec![
                Some(DynCell::Str("k_del".into())),
                Some(DynCell::I32(9)),
            ])],
        )
        .await;
        let mut tx: TestTx = db.begin_transaction().await.expect("tx");
        tx.delete(KeyOwned::from("k_del")).expect("delete");

        assert!(
            tx.get(&KeyOwned::from("k_del"))
                .await
                .expect("get")
                .is_none()
        );
        let predicate = all_rows_predicate();
        let batches = tx.scan().filter(predicate).collect().await.expect("scan");
        assert!(extract_rows(&batches).is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn build_delete_batch_writes_commit_ts() {
        let (db, _schema) = make_db().await;
        let tx: TestTx = db.begin_transaction().await.expect("tx");
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
        let mut tx: TestTx = db.begin_transaction().await.expect("tx");
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
        let (db, schema) =
            make_db_with_policy(Some(Arc::new(BatchesThreshold { batches: 1 }))).await;
        ingest_rows(
            &db,
            &schema,
            vec![DynRow(vec![
                Some(DynCell::Str("sealed".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .await;
        assert!(db.inner().num_immutable_segments() >= 1);

        let tx: TestTx = db.begin_transaction().await.expect("tx");
        let predicate = all_rows_predicate();
        let batches = tx
            .scan()
            .filter(predicate)
            .collect()
            .await
            .expect("scan immutables");
        let rows = extract_rows(&batches);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "sealed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn commit_conflict_checks_immutables() {
        let (db, schema) =
            make_db_with_policy(Some(Arc::new(BatchesThreshold { batches: 1 }))).await;

        ingest_rows(
            &db,
            &schema,
            vec![DynRow(vec![
                Some(DynCell::Str("user".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .await;
        let mut tx: TestTx = db.begin_transaction().await.expect("tx");

        ingest_rows(
            &db,
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

        let err = tx.commit().await.expect_err("conflict expected");
        match err {
            TransactionCommitError::WriteConflict(key) => {
                assert_eq!(key, KeyOwned::from("user"));
            }
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn commit_without_wal_is_volatile_but_applies() {
        let (db, _schema) = make_db().await;

        let mut tx: TestTx = db.begin_transaction().await.expect("tx");
        assert_eq!(tx.durability(), TransactionDurability::Volatile);

        tx.upsert(DynRow(vec![
            Some(DynCell::Str("k".into())),
            Some(DynCell::I32(1)),
        ]))
        .expect("stage row");

        tx.commit()
            .await
            .expect("commit should succeed without wal");

        let predicate = Expr::is_not_null("id");
        let batches = db
            .scan()
            .filter(predicate)
            .collect()
            .await
            .expect("collect");

        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id col");
            let vals = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("value col");
            for (id, v) in ids.iter().zip(vals.iter()) {
                if let (Some(id), Some(v)) = (id, v) {
                    rows.push((id.to_string(), v));
                }
            }
        }
        assert_eq!(rows, vec![("k".to_string(), 1)]);
    }

    #[test]
    fn transaction_scan_orders_rows_and_skips_deletes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, true),
        ]));
        let mut staged = BTreeMap::new();
        staged.insert(
            KeyOwned::from("a"),
            DynMutation::Upsert(DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I32(1)),
            ])),
        );
        staged.insert(KeyOwned::from("c"), DynMutation::Delete(()));
        staged.insert(
            KeyOwned::from("b"),
            DynMutation::Upsert(DynRow(vec![
                Some(DynCell::Str("b".into())),
                Some(DynCell::I32(2)),
            ])),
        );
        let mut scan =
            TransactionScan::new(&staged, &schema, Timestamp::new(1), None).expect("txn scan");
        let mut seen = Vec::new();
        while let Some(entry) = scan.next() {
            match entry.expect("txn entry") {
                TransactionScanEntry::Row((key_ts, row)) => {
                    let key = key_ts.key().to_owned();
                    let owned = row.into_owned().expect("owned row");
                    let value = match owned.0[1].clone() {
                        Some(DynCell::I32(v)) => v,
                        other => panic!("unexpected cell {other:?}"),
                    };
                    seen.push((key, value));
                }
                TransactionScanEntry::Tombstone(_) => continue,
            }
        }
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, KeyOwned::from("a"));
        assert_eq!(seen[0].1, 1);
        assert_eq!(seen[1].0, KeyOwned::from("b"));
        assert_eq!(seen[1].1, 2);
    }

    #[test]
    fn transaction_scan_yields_all_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, true),
        ]));
        let mut staged = BTreeMap::new();
        staged.insert(
            KeyOwned::from("aa"),
            DynMutation::Upsert(DynRow(vec![
                Some(DynCell::Str("aa".into())),
                Some(DynCell::I32(10)),
            ])),
        );
        staged.insert(
            KeyOwned::from("bb"),
            DynMutation::Upsert(DynRow(vec![
                Some(DynCell::Str("bb".into())),
                Some(DynCell::I32(20)),
            ])),
        );
        staged.insert(
            KeyOwned::from("cc"),
            DynMutation::Upsert(DynRow(vec![
                Some(DynCell::Str("cc".into())),
                Some(DynCell::I32(30)),
            ])),
        );

        let scan =
            TransactionScan::new(&staged, &schema, Timestamp::new(5), None).expect("txn scan");
        let results: Vec<_> = scan
            .filter_map(|entry| match entry.expect("row") {
                TransactionScanEntry::Row((key, row)) => {
                    let owned = row.into_owned().expect("owned");
                    let value = match owned.0[1].clone() {
                        Some(DynCell::I32(v)) => v,
                        other => panic!("unexpected cell {other:?}"),
                    };
                    Some((key.key().to_owned(), value))
                }
                TransactionScanEntry::Tombstone(_) => None,
            })
            .collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, KeyOwned::from("aa"));
        assert_eq!(results[1].0, KeyOwned::from("bb"));
        assert_eq!(results[2].0, KeyOwned::from("cc"));
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
    E: Executor + Timer + Clone,
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
    E: Executor + Timer + Clone,
{
    tickets: Vec<crate::wal::WalTicket<E>>,
}

impl<E> WalTxnTickets<E>
where
    E: Executor + Timer + Clone,
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

fn spawn_publish_task<FS, E>(
    executor: Arc<E>,
    publish_ctx: TxnWalPublishContext<FS, E>,
    tickets: WalTxnTickets<E>,
) where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
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

impl<FS, E> TxnWalPublishContext<FS, E>
where
    FS: crate::manifest::ManifestFs<E>,
    E: Executor + Timer + Clone + 'static,
    <FS as fusio::fs::Fs>::File: fusio::durability::FileCommit,
{
    /// Record WAL range to the mutable memtable's accumulated range.
    ///
    /// This must be called BEFORE any operation that may trigger auto-seal,
    /// otherwise the sealed segment's WAL range would be incomplete.
    fn record_wal_range(&self, wal_range: &WalFrameRange) {
        let mut guard = self
            .mutable_wal_range
            .lock()
            .expect("mutable wal range lock poisoned");
        match &mut *guard {
            Some(existing) => existing.extend(wal_range),
            None => *guard = Some(*wal_range),
        }
    }

    /// Update manifest with WAL segment information.
    ///
    /// Call this after memtable operations to persist WAL segment metadata.
    async fn finalize_manifest(
        &self,
        wal_range: WalFrameRange,
    ) -> Result<(), TransactionCommitError> {
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

    /// Combined record and finalize for async publish tasks.
    ///
    /// This is used by the Fast mode async task which handles both
    /// WAL range recording and manifest update together.
    async fn finalize(&self, wal_range: WalFrameRange) -> Result<(), TransactionCommitError> {
        self.record_wal_range(&wal_range);
        self.finalize_manifest(wal_range).await
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
    /// Planning or stream setup failed inside the DB.
    #[error("db error: {0}")]
    Db(DBError),
    /// Merge/stream execution failed.
    #[error("stream error: {0}")]
    Stream(#[from] StreamError),
    /// Dyn key error.
    #[error("stream error: {0}")]
    DynKey(#[from] DynViewError),
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
