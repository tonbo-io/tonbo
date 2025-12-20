//! Manifest coordination atop `fusio-manifest` for versioned metadata.
//!
//! Internals are generic, but we expose concrete helpers for the supported
//! filesystem backends instead of a new abstraction layer.

use fusio::{
    dynamic::{MaybeSend, MaybeSync},
    executor::{Executor, Timer},
    fs::{Fs, FsCas},
};

pub(crate) mod bootstrap;
pub(crate) mod codec;
mod domain;
mod driver;
mod version;

/// Filesystem bound required by manifest store implementations.
pub trait ManifestFs<E>: Fs + FsCas + Clone + MaybeSend + MaybeSync + 'static
where
    E: Executor + Timer + Clone + 'static,
    Self: fusio_manifest::ObjectHead,
    <Self as Fs>::File: fusio::durability::FileCommit,
{
}

impl<FS, E> ManifestFs<E> for FS
where
    FS: Fs + FsCas + Clone + MaybeSend + MaybeSync + 'static,
    E: Executor + Timer + Clone + 'static,
    FS: fusio_manifest::ObjectHead,
    <FS as Fs>::File: fusio::durability::FileCommit,
{
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) use bootstrap::init_fs_manifest_in_memory;
#[cfg(any(test, feature = "test"))]
pub(crate) use bootstrap::init_in_memory_manifest;
pub(crate) use bootstrap::{TableSnapshot, TonboManifest};
#[cfg(all(test, feature = "tokio"))]
pub(crate) use domain::TableHead;
pub use domain::VersionState;
pub(crate) use domain::{
    GcPlanState, GcSstRef, SstEntry, TableDefinition, TableId, TableMeta, WalSegmentRef,
};
pub use driver::ManifestError;
pub(crate) use driver::ManifestResult;
pub(crate) use version::VersionEdit;
