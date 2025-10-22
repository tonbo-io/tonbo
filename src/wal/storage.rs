//! Storage glue that relies directly on fusio traits.

use std::sync::Arc;

use fusio::{
    DynFs,
    dynamic::fs::DynFile,
    fs::OpenOptions,
    path::{Path, PathPart},
};

use crate::wal::{WalError, WalResult};

/// Shared storage facade for WAL segments backed by fusio.
#[derive(Clone)]
pub struct WalStorage {
    /// Filesystem implementation used for segment operations.
    fs: Arc<dyn DynFs>,
    /// Root directory under which WAL segments are stored.
    root: Path,
}

impl WalStorage {
    /// Create a new storage facade over the provided filesystem.
    pub fn new(fs: Arc<dyn DynFs>, root: Path) -> Self {
        Self { fs, root }
    }

    /// Access the underlying filesystem.
    pub fn fs(&self) -> &Arc<dyn DynFs> {
        &self.fs
    }

    /// Root directory for WAL segments.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Open (or create) a WAL segment starting at the provided sequence.
    pub async fn open_segment(&self, seq: u64) -> WalResult<WalSegment> {
        self.ensure_dir(self.root()).await?;

        let segment_path = self.segment_path(seq)?;
        let file = self
            .fs
            .open_options(&segment_path, Self::write_options())
            .await
            .map_err(|err| {
                WalError::Backend(format!(
                    "failed to open wal segment {}: {}",
                    segment_path, err
                ))
            })?;

        Ok(WalSegment::new(segment_path, file))
    }

    /// Remove an existing WAL segment by path.
    pub async fn remove_segment(&self, path: &Path) -> WalResult<()> {
        self.fs.remove(path).await.map_err(|err| {
            WalError::Backend(format!("failed to remove wal segment {}: {}", path, err))
        })
    }

    /// Expose convenience to create the WAL directory structure.
    pub async fn ensure_dir(&self, path: &Path) -> WalResult<()> {
        self.fs.create_dir_all(path).await.map_err(|err| {
            WalError::Backend(format!("failed to ensure wal directory {}: {}", path, err))
        })
    }

    /// Provide default open options for writable segments.
    pub fn write_options() -> OpenOptions {
        OpenOptions::default().read(false).write(true).create(true)
    }

    fn segment_path(&self, seq: u64) -> WalResult<Path> {
        let filename = format!("wal-{seq:020}.tonwal");
        let part = PathPart::parse(&filename).map_err(|err| {
            WalError::Backend(format!("invalid wal segment name {filename}: {err}"))
        })?;
        Ok(self.root.child(part))
    }
}

/// Handle representing an opened WAL segment file.
pub struct WalSegment {
    path: Path,
    file: Box<dyn DynFile>,
}

impl WalSegment {
    fn new(path: Path, file: Box<dyn DynFile>) -> Self {
        Self { path, file }
    }

    /// Return the path to the underlying segment.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Access the writable segment handle.
    pub fn file_mut(&mut self) -> &mut Box<dyn DynFile> {
        &mut self.file
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fusio::{Read, Write, fs::OpenOptions, impls::mem::fs::InMemoryFs, path::Path};
    use futures::executor::block_on;

    use super::*;

    #[test]
    fn ensure_dir_is_idempotent() {
        block_on(async {
            let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
            let root = Path::parse("wal").expect("valid wal root");
            let storage = WalStorage::new(fs, root.clone());

            storage
                .ensure_dir(&root)
                .await
                .expect("first create succeeds");
            storage
                .ensure_dir(&root)
                .await
                .expect("second create succeeds");
        });
    }

    #[test]
    fn open_segment_persists_and_removes() {
        block_on(async {
            let fs: Arc<dyn DynFs> = Arc::new(InMemoryFs::new());
            let root = Path::parse("wal").expect("valid wal root");
            let storage = WalStorage::new(Arc::clone(&fs), root.clone());

            let mut segment = storage.open_segment(1).await.expect("open segment");
            let data = vec![1_u8, 2, 3];
            let (write_res, _buf) = segment.file_mut().write_all(data.clone()).await;
            write_res.expect("write succeeds");
            segment.file_mut().flush().await.expect("flush succeeds");

            let segment_path = segment.path().clone();
            drop(segment);

            let mut reopened = storage
                .fs()
                .open_options(&segment_path, OpenOptions::default())
                .await
                .expect("reopen for read");
            let (read_res, contents) = reopened.read_to_end_at(Vec::new(), 0).await;
            read_res.expect("read succeeds");
            assert_eq!(contents, data);

            storage
                .remove_segment(&segment_path)
                .await
                .expect("remove succeeds");
            let reopen_result = storage
                .fs()
                .open_options(&segment_path, OpenOptions::default())
                .await;
            assert!(reopen_result.is_err(), "segment should be gone");
        });
    }
}
