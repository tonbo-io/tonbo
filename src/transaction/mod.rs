//! Transactional write scaffolding (work in progress).
//!
//! The initial revision introduces the core data structures used to stage
//! dynamic mutations before they are committed through the WAL. Follow-up
//! patches will wire these pieces into `DB::begin_transaction`, WAL plumbing,
//! and recovery.

use std::{collections::BTreeMap, fmt};

use fusio_manifest::snapshot::Snapshot as ManifestLease;
use thiserror::Error;
use typed_arrow_dyn::DynCell;

use crate::{
    key::KeyOwned,
    manifest::{ManifestError, TableHead, TableSnapshot, VersionState, WalSegmentRef},
    mutation::DynMutation,
    mvcc::{ReadView, Timestamp},
};

/// Errors surfaced while constructing a read-only snapshot.
#[derive(Debug, Error)]
#[allow(dead_code)]
pub(crate) enum SnapshotError {
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
    entries: BTreeMap<KeyOwned, DynMutation<Vec<Option<DynCell>>>>,
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

    /// Stage an upsert mutation for `key`, replacing any prior staged value.
    pub(crate) fn upsert(&mut self, key: KeyOwned, row: Vec<Option<DynCell>>) {
        self.entries.insert(key, DynMutation::Upsert(row));
    }

    /// Stage a delete mutation for `key`, overwriting any previous staged value.
    pub(crate) fn delete(&mut self, key: KeyOwned) {
        self.entries.insert(key, DynMutation::Delete(()));
    }

    /// Iterate over staged entries in key order.
    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = (&KeyOwned, &DynMutation<Vec<Option<DynCell>>>)> {
        self.entries.iter()
    }
}
