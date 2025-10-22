//! Storage glue that relies directly on fusio traits.

use std::sync::Arc;

use fusio::{DynFs, fs::OpenOptions, path::Path};

use crate::wal::{WalError, WalResult};

/// Shared storage facade for WAL segments backed by fusio.
#[derive(Clone)]
pub struct WalStorage {
    /// Filesystem implementation used for segment operations.
    fs: Arc<dyn DynFs>,
}

impl WalStorage {
    /// Create a new storage facade over the provided filesystem.
    pub fn new(fs: Arc<dyn DynFs>) -> Self {
        Self { fs }
    }

    /// Access the underlying filesystem.
    pub fn fs(&self) -> &Arc<dyn DynFs> {
        &self.fs
    }

    /// Open (or create) a WAL segment starting at the provided sequence.
    pub async fn open_segment(&self, _seq: u64) -> WalResult<WalSegment> {
        Err(WalError::Unimplemented("WalStorage::open_segment"))
    }

    /// Remove an existing WAL segment by path.
    pub async fn remove_segment(&self, _path: &Path) -> WalResult<()> {
        Err(WalError::Unimplemented("WalStorage::remove_segment"))
    }

    /// Expose convenience to create the WAL directory structure.
    pub async fn ensure_dir(&self, _path: &Path) -> WalResult<()> {
        Err(WalError::Unimplemented("WalStorage::ensure_dir"))
    }

    /// Provide default open options for writable segments.
    pub fn write_options() -> OpenOptions {
        OpenOptions::default().write(true).create(true)
    }
}

/// Handle representing an opened WAL segment file.
#[derive(Debug)]
pub struct WalSegment {
    /// Placeholder for the opened file until implementation lands.
    _todo: (),
}
