use std::{
    collections::{BinaryHeap, HashMap},
    mem,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_lock::RwLock;
use async_trait::async_trait;
use flume::Sender;
use fusio::{fs::FileMeta, DynFs};
use fusio_log::{Logger, Options};
use futures_util::StreamExt;
use itertools::Itertools;

use super::{TransactionTs, MAX_LEVEL};
use crate::{
    fs::{generate_file_id, manager::StoreManager, parse_file_id, FileId, FileType},
    manifest::{ManifestStorage, ManifestStorageError},
    ondisk::sstable::SsTableID,
    record::{Record, Schema},
    scope::Scope,
    version::{
        cleaner::CleanTag, edit::VersionEdit, timestamp::Timestamp, Version, VersionError,
        VersionRef,
    },
    DbOption,
};

// Need to implement `PartialOrd` and `Ord` for heap insertion
struct CmpMeta(FileMeta);

impl Eq for CmpMeta {}

impl PartialEq<Self> for CmpMeta {
    fn eq(&self, other: &Self) -> bool {
        self.0.path.eq(&other.0.path)
    }
}

impl PartialOrd<Self> for CmpMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CmpMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.path.cmp(&other.0.path)
    }
}

/// Mutable state for version tracking in the engine
pub(crate) struct VersionSetInner<R>
where
    R: Record,
{
    // Current version of the DB
    current: VersionRef<R>,
    // Active WAL file id
    log_id: FileId,
    // List of WAL file ids that can be deleted
    deleted_wal: Vec<FileId>,
    // List of SST file ids that can be deleted
    deleted_sst: Vec<SsTableID>,
}

/// Coordinator for tracking and managing versions of on-disk state
///
/// `VersionSet` keeps track of the `VersionRef` which specifies which WALs
/// and SST files currently make up the database. When a reader does a scan
/// it will use the `VersionRef` to return the `Version` which contains the
/// frozen view of its own timestamp and file lists.
pub(crate) struct VersionSet<R>
where
    R: Record,
{
    // Current snapshot version
    inner: Arc<RwLock<VersionSetInner<R>>>,
    // Channel sender for deleting WAL/SST
    clean_sender: Sender<CleanTag>,
    // Counter for version change
    timestamp: Arc<AtomicU32>,
    option: Arc<DbOption>,
    manager: Arc<StoreManager>,
}

impl<R> Clone for VersionSet<R>
where
    R: Record,
{
    fn clone(&self) -> Self {
        VersionSet {
            inner: self.inner.clone(),
            clean_sender: self.clean_sender.clone(),
            timestamp: self.timestamp.clone(),
            option: self.option.clone(),
            manager: self.manager.clone(),
        }
    }
}

impl<R> TransactionTs for VersionSet<R>
where
    R: Record,
{
    fn load_ts(&self) -> Timestamp {
        self.timestamp.load(Ordering::Acquire).into()
    }

    fn increase_ts(&self) -> Timestamp {
        (self.timestamp.fetch_add(1, Ordering::Release) + 1).into()
    }
}

impl<R> VersionSet<R>
where
    R: Record,
{
    /// Creates a new `VersionSet` by checking the previous version log and applies previous edits
    /// to the new log if they exist.
    pub(crate) async fn new(
        clean_sender: Sender<CleanTag>,
        option: Arc<DbOption>,
        manager: Arc<StoreManager>,
    ) -> Result<Self, VersionError> {
        let fs = manager.base_fs();
        let version_dir = option.version_log_dir_path();
        let mut log_stream = fs.list(&version_dir).await?;
        let mut log_binary_heap = BinaryHeap::with_capacity(3);

        // Only keep the two most recent version-logs. If a crash happened while
        // writing the newest snapshot, it may corrupt. Therefore the second newest
        // file is guaranteed to be a complete + correct version. Delete any older
        // logs that are older than the second newest.
        while let Some(result) = log_stream.next().await {
            let file_meta = result?;

            log_binary_heap.push(CmpMeta(file_meta));

            // Keep only two logs, pop and remove the path of all the other logs
            if log_binary_heap.len() > 2 {
                if let Some(old_meta) = log_binary_heap.pop() {
                    fs.remove(&old_meta.0.path).await?;
                }
            }
        }

        let second_log_id = log_binary_heap.pop();
        let latest_log_id = log_binary_heap.pop();

        // If both ids are valid we want to use the second log id instead because it
        // is guaranteed to be safe.
        if let (Some(log_id), Some(_)) = (&latest_log_id, &second_log_id) {
            fs.remove(&log_id.0.path).await?;
        }

        let mut edits = vec![];

        let log_id = match second_log_id
            .or(latest_log_id)
            .map(|file_meta| parse_file_id(&file_meta.0.path, FileType::Log))
            .transpose()?
            .flatten()
        {
            // If the log id exists we retrieve `VersionEdit`s from the version log
            Some(log_id) => {
                let recover_edits = VersionEdit::<<R::Schema as Schema>::Key>::recover(
                    option.version_log_path(log_id),
                    option.base_fs.clone(),
                )
                .await;
                edits = recover_edits;
                log_id
            }
            // If the log id does not already exist, we generate a new one and create version log
            // path for it
            None => {
                let log_id = generate_file_id();
                let base_fs = manager.base_fs();
                let mut log = Self::open_version_log(&option, base_fs.clone(), log_id).await?;
                log.close().await?;
                log_id
            }
        };
        drop(log_stream);

        let timestamp = Arc::new(AtomicU32::default());
        let set = VersionSet::<R> {
            inner: Arc::new(RwLock::new(VersionSetInner {
                current: Arc::new(Version::<R> {
                    ts: Timestamp::from(0),
                    level_slice: [const { Vec::new() }; MAX_LEVEL],
                    clean_sender: clean_sender.clone(),
                    option: option.clone(),
                    timestamp: timestamp.clone(),
                    log_length: 0,
                }),
                log_id,
                deleted_wal: Default::default(),
                deleted_sst: Default::default(),
            })),
            clean_sender,
            timestamp,
            option,
            manager,
        };

        // Only generate a new manifest if there is no rewrites
        if edits.is_empty() {
            set.compact_log().await?;
        } else {
            // Apply any existing edits from the previous version logs
            set.apply_edits(edits, None, true).await?;
        }

        Ok(set)
    }

    /// Applies a sequence of `VersionEdit`s to the `VersionSet`, updating both
    /// the persistent manifest and the in‑memory snapshot. During normal operation
    /// (when `is_recover` is false), edits are appended to the version log; during
    /// recovery, existing edits are replayed without writing back to disk.
    async fn apply_edits(
        &self,
        mut version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
        is_recover: bool,
    ) -> Result<(), VersionError> {
        let timestamp = &self.timestamp;
        let option = &self.option;
        let mut guard = self.inner.write().await;
        let mut new_version = Version::clone(&guard.current);
        let log_id = &mut guard.log_id;
        let edit_len = new_version.log_length + version_edits.len() as u32;

        let mut log =
            Self::open_version_log(&self.option, self.manager.local_fs().clone(), *log_id).await?;

        // Update the log length during non recovery operations. Recovery operations are
        // only for recovering the operations in the WAL.
        if !is_recover {
            version_edits.push(VersionEdit::NewLogLength { len: edit_len });
            log.write_batch(version_edits.iter())
                .await
                .map_err(VersionError::Logger)?;
        }

        // Batch `add SST` operations because adds can be consecutive
        let mut batch_add: HashMap<u8, Vec<Scope<<R::Schema as Schema>::Key>>> = HashMap::new();
        for version_edit in version_edits {
            match version_edit {
                // [`VersionEdit::Add`]: the WAL is garbage collected and we push the new
                // SST into the specified level
                VersionEdit::Add { mut scope, level } => {
                    // TODO: remove after apply
                    if let Some(wal_ids) = scope.wal_ids.take() {
                        guard.deleted_wal.extend(wal_ids);
                    }

                    if level == 0 {
                        new_version.level_slice[0].push(scope);
                    } else {
                        batch_add.entry(level).or_default().push(scope);
                    }
                }
                // [`VersionEdit::Remove`]: the specified SST is removed in `level_slice`
                VersionEdit::Remove { gen, level } => {
                    if let Some(i) = new_version.level_slice[level as usize]
                        .iter()
                        .position(|scope| scope.gen == gen)
                    {
                        new_version.level_slice[level as usize].remove(i);
                    }
                    if is_recover {
                        // issue: https://github.com/tonbo-io/tonbo/issues/123
                        guard.deleted_sst.push(SsTableID::new(gen, level as usize));
                    }
                }
                // [`VersionEdit::LatestTimestamp`]: update the latest timestamp
                VersionEdit::LatestTimeStamp { ts } => {
                    if is_recover {
                        // Start from last persisted timestamp
                        timestamp.store(u32::from(ts), Ordering::Release);
                    }
                    new_version.ts = ts;
                }
                // [`VersionEdit::NewLogLength`]: the version log is updated
                VersionEdit::NewLogLength { len } => {
                    new_version.log_length = len;
                }
            }
        }

        // Due to many compaction add operations being consecutive, this checks if the
        // SSTs can be splice inserted instead of inserting each one individually
        if !batch_add.is_empty() {
            for (level, mut scopes) in batch_add.into_iter() {
                scopes.sort_unstable_by_key(|scope| scope.min.clone());
                let sort_runs = &mut new_version.level_slice[level as usize];

                let merged: Vec<_> = scopes
                    .iter()
                    .cloned()
                    .merge_by(sort_runs.iter().cloned(), |a, b| a.min <= b.min)
                    .collect();
                *sort_runs = merged;
            }
        }

        if let Some(delete_gens) = delete_gens {
            guard.deleted_sst.extend(delete_gens);
        }
        log.close().await?;

        guard.current = Arc::new(new_version);

        drop(guard);

        // Rewrite + clean if edit is not empty or during recovery
        if edit_len >= option.version_log_snapshot_threshold || is_recover {
            self.compact_log().await?;
            self.clean().await?;
        }
        Ok(())
    }

    /// Creates a new manifest file and deletes the old one
    async fn compact_log(&self) -> Result<(), VersionError> {
        let mut guard = self.inner.write().await;
        let mut new_version = Version::clone(&guard.current);
        let fs = self.manager.local_fs();
        let log_id = &mut guard.log_id;
        let old_log_id = mem::replace(log_id, generate_file_id());

        new_version.log_length = 0;
        let edits = new_version.to_edits();
        let mut log = Self::open_version_log(&self.option, fs.clone(), *log_id).await?;
        log.write_batch(edits.iter())
            .await
            .map_err(VersionError::Logger)?;
        log.close().await?;

        fs.remove(&self.option.version_log_path(old_log_id)).await?;
        self.sync(*log_id, old_log_id, edits.iter()).await?;

        guard.current = Arc::new(new_version);

        Ok(())
    }

    // Delete the remaining WAL and SST files
    async fn clean(&self) -> Result<(), VersionError> {
        let mut guard = self.inner.write().await;
        let version = Version::clone(&guard.current);
        if !guard.deleted_wal.is_empty() {
            for wal_id in guard.deleted_wal.iter() {
                // may have been removed after multiple starts
                let _ = self
                    .manager
                    .base_fs()
                    .remove(&self.option.wal_path(*wal_id))
                    .await;
            }
            guard.deleted_wal.clear();
        }
        if !guard.deleted_sst.is_empty() {
            version
                .clean_sender
                .send_async(CleanTag::Add {
                    ts: version.ts,
                    gens: guard.deleted_sst.clone(),
                })
                .await
                .map_err(VersionError::Send)?;
            guard.deleted_sst.clear();
        }

        guard.current = Arc::new(version);
        Ok(())
    }

    async fn sync<'r>(
        &self,
        log_id: FileId,
        old_log_id: FileId,
        edits: impl ExactSizeIterator<Item = &'r VersionEdit<<R::Schema as Schema>::Key>>,
    ) -> Result<(), VersionError> {
        if self.manager.base_fs().file_system() != self.manager.local_fs().file_system() {
            // push local manifest to base file system
            let base_fs = self.manager.base_fs();
            let mut log = Self::open_version_log(&self.option, base_fs.clone(), log_id).await?;

            log.write_batch(edits).await?;
            log.close().await?;
            base_fs
                .remove(&self.option.version_log_path(old_log_id))
                .await?;
        }

        Ok(())
    }

    // Opens or creates the version log file that holds `VersionEdit` records
    async fn open_version_log(
        option: &DbOption,
        fs: Arc<dyn DynFs>,
        gen: FileId,
    ) -> Result<Logger<VersionEdit<<R::Schema as Schema>::Key>>, VersionError> {
        Options::new(option.version_log_path(gen))
            .build_with_fs(fs)
            .await
            .map_err(VersionError::Logger)
    }

    /// Deletes all on-disk data for this store version
    async fn destroy_log(&mut self) -> Result<(), VersionError> {
        let log_dir_path = self.option.version_log_dir_path();
        let log_fs = self.manager.base_fs();
        let mut log_stream = log_fs.list(&log_dir_path).await?;
        while let Ok(meta) = log_stream.next().await.transpose() {
            match meta {
                Some(meta) => log_fs.remove(&meta.path).await?,
                None => break,
            }
        }

        Ok(())
    }

    async fn destroy_levels(&mut self) -> Result<(), VersionError> {
        let log_fs = self.manager.base_fs();
        for level in 0..MAX_LEVEL {
            let level_path = self
                .option
                .level_fs_path(level)
                .unwrap_or(&self.option.base_path);
            let fs = self.manager.get_fs(level_path);
            let mut stream = fs.list(level_path).await?;
            while let Ok(meta) = stream.next().await.transpose() {
                match meta {
                    Some(meta) => {
                        let path = std::path::Path::new(meta.path.as_ref());
                        if path.is_file() {
                            log_fs.remove(&meta.path).await?;
                        }
                    }
                    None => break,
                }
            }
        }

        Ok(())
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<R> ManifestStorage<R> for VersionSet<R>
where
    R: Record,
{
    async fn current(&self) -> VersionRef<R> {
        self.inner.read().await.current.clone()
    }

    async fn recover(
        &self,
        version_edits: Vec<VersionEdit<<<R as Record>::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
    ) -> Result<(), ManifestStorageError> {
        Ok(self.apply_edits(version_edits, delete_gens, true).await?)
    }

    async fn update(
        &self,
        version_edits: Vec<VersionEdit<<<R as Record>::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
    ) -> Result<(), ManifestStorageError> {
        Ok(self.apply_edits(version_edits, delete_gens, false).await?)
    }

    async fn rewrite(&self) -> Result<(), ManifestStorageError> {
        Ok(self.compact_log().await?)
    }

    async fn destroy(&mut self) -> Result<(), ManifestStorageError> {
        self.destroy_log().await?;
        self.destroy_levels().await?;

        Ok(())
    }
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests {
    use std::sync::Arc;

    use async_lock::RwLock;
    use flume::{bounded, Sender};
    use fusio::path::Path;
    use fusio_dispatch::FsOptions;
    use futures_util::StreamExt;
    use tempfile::TempDir;

    use crate::{
        fs::{generate_file_id, manager::StoreManager},
        record::{test::StringSchema, Record},
        scope::Scope,
        version::{
            cleaner::CleanTag,
            edit::VersionEdit,
            set::{VersionSet, VersionSetInner},
            TransactionTs, Version, VersionError,
        },
        DbOption,
    };

    pub(crate) async fn build_version_set<R>(
        version: Version<R>,
        clean_sender: Sender<CleanTag>,
        option: Arc<DbOption>,
        manager: Arc<StoreManager>,
    ) -> Result<VersionSet<R>, VersionError>
    where
        R: Record,
    {
        let log_id = generate_file_id();

        let timestamp = version.timestamp.clone();

        Ok(VersionSet::<R> {
            inner: Arc::new(RwLock::new(VersionSetInner {
                current: Arc::new(version),
                log_id,
                deleted_wal: Default::default(),
                deleted_sst: Default::default(),
            })),
            clean_sender,
            timestamp,
            option,
            manager,
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn timestamp_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let (sender, _) = bounded(1);
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        ));
        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();

        let version_set: VersionSet<String> =
            VersionSet::new(sender.clone(), option.clone(), manager.clone())
                .await
                .unwrap();

        version_set
            .apply_edits(
                vec![VersionEdit::LatestTimeStamp { ts: 20_u32.into() }],
                None,
                false,
            )
            .await
            .unwrap();

        drop(version_set);

        let version_set: VersionSet<String> =
            VersionSet::new(sender.clone(), option.clone(), manager)
                .await
                .unwrap();
        assert_eq!(version_set.load_ts(), 20_u32.into());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_apply_edits() {
        let temp_dir = TempDir::new().unwrap();
        let path = Path::from_filesystem_path(temp_dir.path()).unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let (sender, _) = bounded(1);
        let mut option = DbOption::new(path, &StringSchema);
        option.version_log_snapshot_threshold = 1000;

        let option = Arc::new(option);
        manager
            .local_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();

        let version_set: VersionSet<String> =
            VersionSet::new(sender.clone(), option.clone(), manager.clone())
                .await
                .unwrap();
        let gen_0 = generate_file_id();
        let gen_1 = generate_file_id();
        let gen_2 = generate_file_id();

        version_set
            .apply_edits(
                vec![VersionEdit::Add {
                    level: 0,
                    scope: Scope {
                        min: "0".to_string(),
                        max: "1".to_string(),
                        gen: gen_0,
                        wal_ids: None,
                        file_size: 7,
                    },
                }],
                None,
                false,
            )
            .await
            .unwrap();

        version_set
            .apply_edits(
                vec![VersionEdit::Add {
                    level: 0,
                    scope: Scope {
                        min: "2".to_string(),
                        max: "3".to_string(),
                        gen: gen_1,
                        wal_ids: None,
                        file_size: 7,
                    },
                }],
                None,
                false,
            )
            .await
            .unwrap();
        version_set
            .apply_edits(
                vec![VersionEdit::Add {
                    level: 0,
                    scope: Scope {
                        min: "4".to_string(),
                        max: "5".to_string(),
                        gen: gen_2,
                        wal_ids: None,
                        file_size: 7,
                    },
                }],
                None,
                false,
            )
            .await
            .unwrap();

        {
            let guard = version_set.inner.write().await;
            let edits = VersionEdit::<String>::recover(
                option.version_log_path(guard.log_id),
                option.base_fs.clone(),
            )
            .await;

            assert_eq!(edits.len(), 8);
            assert_eq!(
                edits[2..],
                [
                    VersionEdit::Add {
                        level: 0,
                        scope: Scope {
                            min: "0".to_string(),
                            max: "1".to_string(),
                            gen: gen_0,
                            wal_ids: None,
                            file_size: 7
                        },
                    },
                    VersionEdit::NewLogLength { len: 1 },
                    VersionEdit::Add {
                        level: 0,
                        scope: Scope {
                            min: "2".to_string(),
                            max: "3".to_string(),
                            gen: gen_1,
                            wal_ids: None,
                            file_size: 7
                        },
                    },
                    VersionEdit::NewLogLength { len: 2 },
                    VersionEdit::Add {
                        level: 0,
                        scope: Scope {
                            min: "4".to_string(),
                            max: "5".to_string(),
                            gen: gen_2,
                            wal_ids: None,
                            file_size: 7
                        },
                    },
                    VersionEdit::NewLogLength { len: 3 },
                ]
            );
        }

        // test recover case
        {
            // ignore error originatey by cleaner
            let _ = version_set
                .apply_edits(
                    vec![
                        VersionEdit::Remove {
                            level: 0,
                            gen: gen_0,
                        },
                        VersionEdit::Remove {
                            level: 0,
                            gen: gen_2,
                        },
                    ],
                    None,
                    true,
                )
                .await;
            let guard = version_set.inner.write().await;
            let edits = guard.current.to_edits();

            assert_eq!(edits.len(), 3);
            assert_eq!(
                edits,
                vec![
                    VersionEdit::Add {
                        level: 0,
                        scope: Scope {
                            min: "2".to_string(),
                            max: "3".to_string(),
                            gen: gen_1,
                            wal_ids: None,
                            file_size: 7
                        },
                    },
                    VersionEdit::LatestTimeStamp { ts: 0.into() },
                    VersionEdit::NewLogLength { len: 0 }
                ]
            );
        }

        drop(version_set);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_apply_edits_batch_add_out_of_index() {
        let temp_dir = TempDir::new().unwrap();
        let path = Path::from_filesystem_path(temp_dir.path()).unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let (sender, _) = bounded(1);
        let mut option = DbOption::new(path, &StringSchema);
        option.version_log_snapshot_threshold = u32::MAX;
        let option = Arc::new(option);

        manager
            .local_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();

        let version_set: VersionSet<String> =
            VersionSet::new(sender.clone(), option.clone(), manager.clone())
                .await
                .unwrap();

        let gen_d = generate_file_id();
        {
            let mut guard = version_set.inner.write().await;
            let mut v = Version::clone(&guard.current);
            v.level_slice[1].push(Scope {
                min: "4".to_string(),
                max: "4".to_string(),
                gen: gen_d,
                wal_ids: None,
                file_size: 0,
            });
            guard.current = Arc::new(v);
        }

        let gen_a = generate_file_id();
        let gen_b = generate_file_id();
        let gen_c = generate_file_id();
        version_set
            .apply_edits(
                vec![
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "2".to_string(),
                            max: "2".to_string(),
                            gen: gen_b,
                            wal_ids: None,
                            file_size: 0,
                        },
                    },
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "1".to_string(),
                            max: "1".to_string(),
                            gen: gen_a,
                            wal_ids: None,
                            file_size: 0,
                        },
                    },
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "5".to_string(),
                            max: "5".to_string(),
                            gen: gen_c,
                            wal_ids: None,
                            file_size: 0,
                        },
                    },
                ],
                None,
                true,
            )
            .await
            .unwrap();

        {
            let guard = version_set.inner.read().await;
            let keys: Vec<_> = guard.current.level_slice[1]
                .iter()
                .map(|scope| scope.min.clone())
                .collect();
            assert_eq!(keys, vec!["1", "2", "4", "5"]);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_apply_edits_batch_add() {
        let temp_dir = TempDir::new().unwrap();
        let path = Path::from_filesystem_path(temp_dir.path()).unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let (sender, _) = bounded(1);
        let mut option = DbOption::new(path, &StringSchema);
        option.version_log_snapshot_threshold = u32::MAX;
        let option = Arc::new(option);

        manager
            .local_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();

        let version_set: VersionSet<String> =
            VersionSet::new(sender.clone(), option.clone(), manager.clone())
                .await
                .unwrap();

        let gen_d = generate_file_id();
        {
            let mut guard = version_set.inner.write().await;
            let mut v = Version::clone(&guard.current);
            v.level_slice[1].push(Scope {
                min: "4".to_string(),
                max: "4".to_string(),
                gen: gen_d,
                wal_ids: None,
                file_size: 0,
            });
            v.level_slice[1].push(Scope {
                min: "8".to_string(),
                max: "8".to_string(),
                gen: gen_d,
                wal_ids: None,
                file_size: 0,
            });
            guard.current = Arc::new(v);
        }

        let gen_a = generate_file_id();
        let gen_b = generate_file_id();
        let gen_c = generate_file_id();
        version_set
            .apply_edits(
                vec![
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "2".to_string(),
                            max: "2".to_string(),
                            gen: gen_b,
                            wal_ids: None,
                            file_size: 0,
                        },
                    },
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "5".to_string(),
                            max: "5".to_string(),
                            gen: gen_a,
                            wal_ids: None,
                            file_size: 0,
                        },
                    },
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "7".to_string(),
                            max: "7".to_string(),
                            gen: gen_c,
                            wal_ids: None,
                            file_size: 0,
                        },
                    },
                ],
                None,
                true,
            )
            .await
            .unwrap();

        {
            let guard = version_set.inner.read().await;
            let keys: Vec<_> = guard.current.level_slice[1]
                .iter()
                .map(|scope| scope.min.clone())
                .collect();
            assert_eq!(keys, vec!["2", "4", "5", "7", "8"]);
        }
    }

    async fn version_log_snap_shot(base_option: FsOptions, path: Path) {
        let manager = Arc::new(StoreManager::new(base_option, vec![]).unwrap());
        let (sender, _) = bounded(1);
        let mut option = DbOption::new(path, &StringSchema);
        option.version_log_snapshot_threshold = 4;

        let option = Arc::new(option);
        manager
            .local_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();

        let version_set: VersionSet<String> =
            VersionSet::new(sender.clone(), option.clone(), manager.clone())
                .await
                .unwrap();
        let gen_0 = generate_file_id();
        let gen_1 = generate_file_id();
        let gen_2 = generate_file_id();

        version_set
            .apply_edits(
                vec![
                    VersionEdit::Add {
                        level: 0,
                        scope: Scope {
                            min: "0".to_string(),
                            max: "1".to_string(),
                            gen: gen_0,
                            wal_ids: None,
                            file_size: 7,
                        },
                    },
                    VersionEdit::Add {
                        level: 0,
                        scope: Scope {
                            min: "2".to_string(),
                            max: "3".to_string(),
                            gen: gen_1,
                            wal_ids: None,
                            file_size: 7,
                        },
                    },
                    VersionEdit::Add {
                        level: 0,
                        scope: Scope {
                            min: "4".to_string(),
                            max: "5".to_string(),
                            gen: gen_2,
                            wal_ids: None,
                            file_size: 7,
                        },
                    },
                    VersionEdit::Remove {
                        level: 0,
                        gen: gen_0,
                    },
                    VersionEdit::Remove {
                        level: 0,
                        gen: gen_2,
                    },
                ],
                None,
                false,
            )
            .await
            .unwrap();

        let guard = version_set.inner.write().await;

        let edits = VersionEdit::<String>::recover(
            option.version_log_path(guard.log_id),
            option.base_fs.clone(),
        )
        .await;

        assert_eq!(edits.len(), 3);
        assert_eq!(
            edits,
            vec![
                VersionEdit::Add {
                    level: 0,
                    scope: Scope {
                        min: "2".to_string(),
                        max: "3".to_string(),
                        gen: gen_1,
                        wal_ids: None,
                        file_size: 7
                    },
                },
                VersionEdit::LatestTimeStamp { ts: 0.into() },
                VersionEdit::NewLogLength { len: 0 }
            ]
        );
        drop(guard);
        drop(version_set);

        let version_dir_path = option.version_log_dir_path();
        let mut stream = manager.base_fs().list(&version_dir_path).await.unwrap();
        let mut logs = Vec::new();

        while let Some(log) = stream.next().await {
            logs.push(log.unwrap());
        }
        logs.sort_by(|meta_a, meta_b| meta_a.path.cmp(&meta_b.path));

        let edits =
            VersionEdit::<String>::recover(logs.pop().unwrap().path, option.base_fs.clone()).await;

        assert_eq!(edits.len(), 3);
        assert_eq!(
            edits,
            vec![
                VersionEdit::Add {
                    level: 0,
                    scope: Scope {
                        min: "2".to_string(),
                        max: "3".to_string(),
                        gen: gen_1,
                        wal_ids: None,
                        file_size: 7
                    },
                },
                VersionEdit::LatestTimeStamp { ts: 0.into() },
                VersionEdit::NewLogLength { len: 0 }
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_version_log_snap_shot() {
        let temp_dir = TempDir::new().unwrap();
        version_log_snap_shot(
            FsOptions::Local,
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        )
        .await;
    }

    #[ignore = "s3"]
    #[cfg(all(feature = "aws", feature = "tokio-http"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_s3_version_log_snap_shot() {
        use fusio::remotes::aws::AwsCredential;

        if option_env!("AWS_ACCESS_KEY_ID").is_none()
            || option_env!("AWS_SECRET_ACCESS_KEY").is_none()
        {
            eprintln!("can not get `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`");
            return;
        }
        let key_id = std::option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = std::option_env!("AWS_SECRET_ACCESS_KEY")
            .unwrap()
            .to_string();
        let token = std::option_env!("AWS_SESSION_TOKEN").map(|v| v.to_string());
        let bucket = std::env::var("BUCKET_NAME").expect("expected s3 bucket not to be empty");
        let region = std::env::var("AWS_REGION").expect("expected s3 region not to be empty");

        let fs_option = FsOptions::S3 {
            bucket,
            credential: Some(AwsCredential {
                key_id,
                secret_key,
                token,
            }),
            endpoint: None,
            sign_payload: None,
            checksum: None,
            region: Some(region),
        };

        let temp_dir = TempDir::new().unwrap();
        version_log_snap_shot(
            fs_option,
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn version_level_sort() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        ));

        let (sender, _) = bounded(1);
        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();

        let version_set: VersionSet<String> =
            VersionSet::new(sender.clone(), option.clone(), manager)
                .await
                .unwrap();
        let gen_0 = generate_file_id();
        let gen_1 = generate_file_id();
        let gen_2 = generate_file_id();
        let gen_3 = generate_file_id();
        version_set
            .apply_edits(
                vec![VersionEdit::Add {
                    level: 1,
                    scope: Scope {
                        min: "4".to_string(),
                        max: "6".to_string(),
                        gen: gen_0,
                        wal_ids: None,
                        file_size: 7,
                    },
                }],
                None,
                false,
            )
            .await
            .unwrap();
        version_set
            .apply_edits(
                vec![
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "1".to_string(),
                            max: "3".to_string(),
                            gen: gen_1,
                            wal_ids: None,
                            file_size: 7,
                        },
                    },
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "7".to_string(),
                            max: "9".to_string(),
                            gen: gen_2,
                            wal_ids: None,
                            file_size: 7,
                        },
                    },
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "0".to_string(),
                            max: "0".to_string(),
                            gen: gen_3,
                            wal_ids: None,
                            file_size: 7,
                        },
                    },
                ],
                None,
                false,
            )
            .await
            .unwrap();
        let guard = version_set.inner.read().await;
        dbg!(guard.current.level_slice.clone());
        let slice: Vec<String> = guard.current.level_slice[1]
            .iter()
            .map(|scope| scope.min.clone())
            .collect();
        assert_eq!(
            slice,
            vec![
                "0".to_string(),
                "1".to_string(),
                "4".to_string(),
                "7".to_string()
            ]
        );
    }
}
