//! Manifest integration layered on top of `fusio-manifest`.
//!
//! The implementation is split across smaller modules:
//! - `domain`: strongly typed keys and values stored in the manifests.
//! - `codec`: codec markers binding key/value pairs for individual manifest instances.
//! - `driver`: thin wrapper around `fusio-manifest` providing higher-level helpers.
//! - `version`: logic for applying manifest `VersionEdit`s.
//! - `bootstrap`: convenience helpers for spinning up in-memory manifests during tests.

pub(crate) mod bootstrap;
pub(crate) mod codec;
mod domain;
mod driver;
mod version;

pub(crate) use bootstrap::{
    TableSnapshot, TonboManifest, init_fs_manifest, init_in_memory_manifest,
};
#[cfg(test)]
pub(crate) use domain::TableHead;
pub(crate) use domain::{
    GcPlanState, GcSstRef, SstEntry, TableDefinition, TableId, VersionState, WalSegmentRef,
};
pub(crate) use driver::{ManifestError, ManifestResult};
pub(crate) use version::VersionEdit;
