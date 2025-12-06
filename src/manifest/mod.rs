//! Manifest integration layered on top of `fusio-manifest`.
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
pub(crate) use bootstrap::{
    TableSnapshot, TonboManifest, init_fs_manifest, init_in_memory_manifest,
};
#[cfg(all(test, feature = "tokio"))]
pub(crate) use domain::TableHead;
pub(crate) use domain::{
    GcPlanState, GcSstRef, SstEntry, TableDefinition, TableId, VersionState, WalSegmentRef,
};
pub(crate) use driver::{ManifestError, ManifestResult};
pub(crate) use version::VersionEdit;
