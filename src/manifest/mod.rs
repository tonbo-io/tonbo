//! Manifest domain types and thin wrapper around `fusio-manifest`.
//!
//! This module provides the strongly typed key/value pairs Tonbo stores inside
//! `fusio-manifest` as well as a convenience wrapper that wires the underlying
//! stores together.

use std::{collections::BTreeMap, time::Duration};

use fusio_manifest::types::Error as FusioManifestError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    fs::{FileId, generate_file_id},
    ondisk::sstable::{SsTableDescriptor, SsTableId, SsTableStats},
};

/// Error type surfaced by Tonbo's manifest layer.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// Error originating from the underlying `fusio-manifest` crate.
    #[error(transparent)]
    Backend(#[from] FusioManifestError),
}

/// Convenience result alias for manifest operations.
pub type ManifestResult<T> = Result<T, ManifestError>;

/// Identifier associated with a logical Tonbo table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableId(FileId);

impl TableId {
    /// Create a new identifier using a freshly generated file identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(generate_file_id())
    }

    /// Wrap an existing file identifier.
    #[must_use]
    pub fn from_file_id(value: FileId) -> Self {
        Self(value)
    }

    /// Access the underlying file identifier.
    #[must_use]
    pub fn as_file_id(self) -> FileId {
        self.0
    }
}

impl Default for TableId {
    fn default() -> Self {
        Self::new()
    }
}

/// Identifier associated with a manifest version of a table.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct VersionId(u64);

impl VersionId {
    /// Construct a version identifier from a raw integer.
    #[must_use]
    pub const fn from(value: u64) -> Self {
        Self(value)
    }

    /// Access the raw version value.
    #[must_use]
    pub const fn inner(self) -> u64 {
        self.0
    }
}

/// Logical manifest keys tracked inside `fusio-manifest`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ManifestKey {
    /// Global catalog state, including allocator metadata.
    CatalogRoot,
    /// Static metadata describing a table.
    TableMeta {
        /// Identifier of the table described by this entry.
        table_id: TableId,
    },
    /// Mutable head pointer for a table containing the latest version info.
    TableHead {
        /// Identifier of the table whose head is being stored.
        table_id: TableId,
    },
    /// Immutable state for a specific table version.
    TableVersion {
        /// Table identifier.
        table_id: TableId,
        /// Version identifier associated with this entry.
        version: VersionId,
    },
    /// Lowest WAL sequence that must be retained.
    WalFloor {
        /// Table identifier.
        table_id: TableId,
    },
    /// Stored garbage-collection plan for a table.
    GcPlan {
        /// Table identifier.
        table_id: TableId,
    },
}

/// Serde-serializable manifest value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload")]
pub enum ManifestValue {
    /// Catalog state payload.
    Catalog(CatalogState),
    /// Table metadata payload.
    TableMeta(TableMeta),
    /// Table head payload.
    TableHead(TableHead),
    /// Immutable version payload.
    TableVersion(VersionState),
    /// WAL retention floor payload.
    WalFloor(WalFloorState),
    /// Table garbage-collection plan payload.
    GcPlan(GcPlanState),
}

/// Snapshot of catalog-level metadata stored in the manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CatalogState {
    /// Mapping of table identifiers to their catalog entries.
    pub tables: BTreeMap<TableId, TableCatalogEntry>,
    /// Monotonic counter used to derive human-friendly table ordinals.
    pub next_table_ordinal: u64,
}

/// Entry describing a table inside the catalog root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCatalogEntry {
    /// Human-readable name of the table.
    pub name: String,
}

impl TableCatalogEntry {
    /// Build a new entry from the provided table name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

/// Static metadata for a Tonbo table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMeta {
    /// Identifier of the table.
    pub table_id: TableId,
    /// Human-readable name of the table.
    pub name: String,
    /// Schema fingerprint used for compatibility validation.
    pub schema_fingerprint: String,
    /// Columns participating in the primary key.
    pub primary_key_columns: Vec<String>,
    /// Optional retention policy overrides.
    pub retention: Option<TableRetionConfig>,
    /// Monotonic schema version number.
    pub schema_version: u32,
}

/// Retention policy knobs for a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRetionConfig {
    /// Maximum number of committed versions to retain.
    pub max_versions: u64,
    /// Maximum age to retain versions and their WAL segments.
    pub max_ttl: Duration,
}

impl Default for TableRetionConfig {
    fn default() -> Self {
        Self {
            max_versions: 5,
            max_ttl: Duration::from_secs(12 * 60 * 60),
        }
    }
}

/// Mutable head view for a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableHead {
    /// Identifier of the table.
    pub table_id: TableId,
    /// Current committed version, if any.
    pub current_version: Option<VersionId>,
    /// Current schema version.
    pub schema_version: u32,
    /// Next SST identifier to allocate when flushing.
    pub next_sst_id: SsTableId,
    /// Lowest WAL sequence that must be retained.
    pub wal_floor: Option<u64>,
    /// Last manifest transaction identifier applied to the head.
    pub last_manifest_txn: Option<u64>,
}

/// Immutable version payload describing a committed state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionState {
    /// Table identifier.
    pub table_id: TableId,
    /// Version identifier.
    pub version_id: VersionId,
    /// Commit timestamp in milliseconds since the Unix epoch.
    pub commit_ts_ms: u64,
    /// SST entries materialised for this version.
    pub ssts: Vec<SstEntry>,
    /// WAL fragments referenced by this version.
    pub wal_segments: Vec<WalSegmentRef>,
    /// Upper bound for tombstones included in this version.
    pub tombstone_watermark: Option<u64>,
    /// Aggregated statistics for the table at this version.
    pub stats: Option<SsTableStats>,
}

/// Descriptor of an SST entry committed into a version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstEntry {
    /// Descriptor carrying range and sizing metadata.
    pub descriptor: SsTableDescriptor,
    /// Mandatory statistics for the SST.
    pub stats: SsTableStats,
    /// Mandatory WAL segments composed to build the SST.
    pub wal_segments: Vec<FileId>,
}

impl PartialEq for SstEntry {
    fn eq(&self, other: &Self) -> bool {
        self.descriptor.id() == other.descriptor.id()
    }
}

/// Reference to WAL fragments that produced a version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentRef {
    /// Monotonic WAL sequence number.
    pub seq: u64,
    /// Identifier of the WAL file.
    pub file_id: FileId,
    /// First frame index (inclusive).
    pub first_frame: u64,
    /// Last frame index (inclusive).
    pub last_frame: u64,
}

/// WAL retention floor state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFloorState {
    /// Lowest WAL sequence that must be retained for recovery.
    pub wal_floor_seq: u64,
}

/// Garbage-collection plan for a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GcPlanState {
    /// SST identifiers that can be removed.
    pub obsolete_sst_ids: Vec<SsTableId>,
    /// WAL sequence numbers that can be truncated.
    pub obsolete_wal_segments: Vec<u64>,
}

/// Convenience alias for a manifest that stores Tonbo-specific key/value pairs.
pub type Manifest<HS, SS, CS, LS, E, R> =
    fusio_manifest::manifest::Manifest<ManifestKey, ManifestValue, HS, SS, CS, LS, E, R>;
