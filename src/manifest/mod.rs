//! Manifest domain types and Tonbo-specific wrapper around `fusio-manifest`.

pub(crate) mod bootstrap;
pub(crate) mod codec;
mod domain;
mod driver;
mod version;

pub(crate) use bootstrap::{InMemoryManifest, init_in_memory_manifest};
pub(crate) use domain::{SstEntry, TableId, WalSegmentRef};
pub use driver::ManifestError;
pub(crate) use driver::ManifestResult;
pub(crate) use version::VersionEdit;
