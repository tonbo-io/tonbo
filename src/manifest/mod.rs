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

pub(crate) use bootstrap::{TonboManifest, init_in_memory_manifest};
pub(crate) use domain::{SstEntry, TableHead, TableId, VersionState, WalSegmentRef};
pub(crate) use driver::{ManifestError, ManifestResult, TableSnapshot};
pub(crate) use version::VersionEdit;
