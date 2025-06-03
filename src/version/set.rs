use std::{
    collections::BinaryHeap,
    mem,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_lock::RwLock;
use flume::Sender;
use fusio::{fs::FileMeta, DynFs};
use futures_util::StreamExt;

use super::{TransactionTs, MAX_LEVEL};
use crate::{
    fs::{
        filename, generate_file_id, logger::Logger, manager::StoreManager, parse_file_id, FileId,
        FileType,
    },
    record::{Record, Schema},
    timestamp::Timestamp,
    version::{cleaner::CleanTag, edit::VersionEdit, Version, VersionError, VersionRef},
    DbOption,
};

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

pub(crate) struct VersionSetInner<R>
where
    R: Record,
{
    current: VersionRef<R>,
    log_id: FileId,
    deleted_wal: Vec<FileId>,
    deleted_sst: Vec<(FileId, usize)>,
}

pub(crate) struct VersionSet<R>
where
    R: Record,
{
    inner: Arc<RwLock<VersionSetInner<R>>>,
    clean_sender: Sender<CleanTag>,
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
    pub(crate) async fn new(
        clean_sender: Sender<CleanTag>,
        option: Arc<DbOption>,
        manager: Arc<StoreManager>,
    ) -> Result<Self, VersionError<R>> {
        let fs = manager.base_fs();
        let version_dir = option.version_log_dir_path();
        let mut log_stream = fs.list(&version_dir).await?;
        let mut log_binary_heap = BinaryHeap::with_capacity(3);

        // when there are multiple logs, this means that a downtime occurred during the
        // `version_log_snap_shot` process, the second newest file has the highest data
        // integrity, so it is used as the version log, and the older log is deleted first
        // to avoid midway downtime, which will cause the second newest file to become the
        // first newest after restart.
        while let Some(result) = log_stream.next().await {
            let file_meta = result?;

            log_binary_heap.push(CmpMeta(file_meta));

            if log_binary_heap.len() > 2 {
                if let Some(old_meta) = log_binary_heap.pop() {
                    fs.remove(&old_meta.0.path).await?;
                }
            }
        }

        let second_log_id = log_binary_heap.pop();
        let latest_log_id = log_binary_heap.pop();

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
            Some(log_id) => {
                let recover_edits = VersionEdit::<<R::Schema as Schema>::Key>::recover(
                    option.version_log_path(log_id),
                    option.base_fs.clone(),
                )
                .await;
                edits = recover_edits;
                log_id
            }
            None => {
                let log_id = generate_file_id();
                let base_fs = manager.base_fs();
                let mut log = Self::open_version_log(&option, base_fs.clone(), log_id).await?;
                log.flush().await?;
                log.close().await?;
                log_id
            }
        };

        let timestamp = Arc::new(AtomicU32::default());
        drop(log_stream);
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
        set.apply_edits(edits, None, true).await?;

        Ok(set)
    }

    pub(crate) async fn current(&self) -> VersionRef<R> {
        self.inner.read().await.current.clone()
    }

    pub(crate) async fn apply_edits(
        &self,
        mut version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<(FileId, usize)>>,
        is_recover: bool,
    ) -> Result<(), VersionError<R>> {
        let timestamp = &self.timestamp;
        let option = &self.option;
        let mut guard = self.inner.write().await;
        let mut new_version = Version::clone(&guard.current);
        let log_id = &mut guard.log_id;
        let edit_len = new_version.log_length + version_edits.len() as u32;

        let mut log =
            Self::open_version_log(&self.option, self.manager.base_fs().clone(), *log_id).await?;

        if !is_recover {
            version_edits.push(VersionEdit::NewLogLength { len: edit_len });
            log.write_batch(version_edits.iter())
                .await
                .map_err(VersionError::Logger)?;
        }

        for version_edit in version_edits {
            match version_edit {
                VersionEdit::Add { mut scope, level } => {
                    // TODO: remove after apply
                    if let Some(wal_ids) = scope.wal_ids.take() {
                        guard.deleted_wal.extend(wal_ids);
                    }
                    if level == 0 {
                        new_version.level_slice[level as usize].push(scope);
                    } else {
                        // TODO: Add is often consecutive, so repeated queries can be avoided
                        let sort_runs = &mut new_version.level_slice[level as usize];
                        let pos = sort_runs
                            .binary_search_by(|s| s.min.cmp(&scope.min))
                            .unwrap_or_else(|index| index);
                        sort_runs.insert(pos, scope);
                    }
                }
                VersionEdit::Remove { gen, level } => {
                    if let Some(i) = new_version.level_slice[level as usize]
                        .iter()
                        .position(|scope| scope.gen == gen)
                    {
                        new_version.level_slice[level as usize].remove(i);
                    }
                    if is_recover {
                        // issue: https://github.com/tonbo-io/tonbo/issues/123
                        guard.deleted_sst.push((gen, level as usize));
                    }
                }
                VersionEdit::LatestTimeStamp { ts } => {
                    if is_recover {
                        timestamp.store(u32::from(ts), Ordering::Release);
                    }
                    new_version.ts = ts;
                }
                VersionEdit::NewLogLength { len } => {
                    new_version.log_length = len;
                }
            }
        }
        if let Some(delete_gens) = delete_gens {
            guard.deleted_sst.extend(delete_gens);
        }
        log.close().await?;

        guard.current = Arc::new(new_version);

        drop(guard);
        if edit_len >= option.version_log_snapshot_threshold {
            self.rewrite().await?;
            self.clean().await?;
            log.remove().await?;
        }
        Ok(())
    }

    pub(crate) async fn rewrite(&self) -> Result<(), VersionError<R>> {
        let mut guard = self.inner.write().await;
        let mut new_version = Version::clone(&guard.current);
        let log_id = &mut guard.log_id;
        let _old_log_id = mem::replace(log_id, generate_file_id());

        new_version.log_length = 0;
        let edits = new_version.to_edits();

        let mut log =
            Self::open_version_log(&self.option, self.manager.base_fs().clone(), *log_id).await?;
        log.write_batch(edits.iter())
            .await
            .map_err(VersionError::Logger)?;
        log.flush().await?;
        log.close().await?;

        guard.current = Arc::new(new_version);

        Ok(())
    }

    async fn clean(&self) -> Result<(), VersionError<R>> {
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

    async fn open_version_log(
        option: &DbOption,
        fs: Arc<dyn DynFs>,
        gen: FileId,
    ) -> Result<Logger<VersionEdit<<R::Schema as Schema>::Key>>, VersionError<R>> {
        let log = Logger::<VersionEdit<<R::Schema as Schema>::Key>>::open(
            option.version_log_dir_path(),
            fs,
            filename(gen, FileType::Log).as_str(),
            0,
        )
        .await?;
        Ok(log)
    }

    pub(crate) async fn destroy(self) -> Result<(), VersionError<R>> {
        let log_dir_path = self.option.version_log_dir_path();
        let log_fs = self.manager.base_fs();
        let mut log_stream = log_fs.list(&log_dir_path).await?;
        while let Ok(meta) = log_stream.next().await.transpose() {
            match meta {
                Some(meta) => log_fs.remove(&meta.path).await?,
                None => break,
            }
        }

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
    ) -> Result<VersionSet<R>, VersionError<R>>
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

    async fn version_log_rewrite(path: Path, base_fs: FsOptions) {
        let option = Arc::new(DbOption::new(path, &StringSchema).base_fs(base_fs.clone()));
        let (sender, _) = bounded(1);

        let mut version =
            Version::<String>::new(option.clone(), sender.clone(), Arc::new(0.into()));
        let (gen1, gen2, gen3) = (generate_file_id(), generate_file_id(), generate_file_id());
        version.level_slice[0] = vec![
            Scope {
                min: "0000".to_string(),
                max: "0100".to_string(),
                gen: gen1,
                wal_ids: None,
            },
            Scope {
                min: "0010".to_string(),
                max: "0300".to_string(),
                gen: gen2,
                wal_ids: None,
            },
        ];
        version.level_slice[1] = vec![Scope {
            min: "0001".to_string(),
            max: "1200".to_string(),
            gen: gen3,
            wal_ids: None,
        }];

        let manager = Arc::new(StoreManager::new(base_fs, vec![]).unwrap());

        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();

        let version_set: VersionSet<String> =
            VersionSet::new(sender.clone(), option.clone(), manager.clone())
                .await
                .unwrap();
        let old_log_id = {
            let mut guard = version_set.inner.write().await;
            guard.current = Arc::new(version);
            guard.log_id
        };

        version_set.rewrite().await.unwrap();
        let log_id = {
            let guard = version_set.inner.write().await;
            guard.log_id
        };

        assert_ne!(old_log_id, log_id);

        let version_log_dir = option.version_log_dir_path();
        let file_stream = manager.base_fs().list(&version_log_dir).await.unwrap();
        assert_eq!(file_stream.count().await, 2);

        let edits =
            VersionEdit::<String>::recover(option.version_log_path(log_id), option.base_fs.clone())
                .await;
        let expected = vec![
            VersionEdit::Add {
                level: 0,
                scope: Scope {
                    min: "0000".to_string(),
                    max: "0100".to_string(),
                    gen: gen1,
                    wal_ids: None,
                },
            },
            VersionEdit::Add {
                level: 0,
                scope: Scope {
                    min: "0010".to_string(),
                    max: "0300".to_string(),
                    gen: gen2,
                    wal_ids: None,
                },
            },
            VersionEdit::Add {
                level: 1,
                scope: Scope {
                    min: "0001".to_string(),
                    max: "1200".to_string(),
                    gen: gen3,
                    wal_ids: None,
                },
            },
            VersionEdit::LatestTimeStamp { ts: 0.into() },
            VersionEdit::NewLogLength { len: 0 },
        ];
        assert_eq!(expected, edits);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_version_log_rewrite() {
        let tmp_dir = TempDir::new().unwrap();
        version_log_rewrite(Path::new(tmp_dir.path()).unwrap(), FsOptions::Local).await;
    }

    #[cfg(all(feature = "aws", feature = "tokio-http"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_s3_version_log_rewrite() {
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

        let path = Path::from_url_path(format!("rewrite/{}", generate_file_id())).unwrap();
        version_log_rewrite(path, fs_option).await;
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

    async fn version_log_snap_shot(base_option: FsOptions, path: Path) {
        let manager = Arc::new(StoreManager::new(base_option.clone(), vec![]).unwrap());
        let (sender, _) = bounded(1);
        let mut option = DbOption::new(path, &StringSchema).base_fs(base_option);
        option.version_log_snapshot_threshold = 4;

        let option = Arc::new(option);
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
                        },
                    },
                    VersionEdit::Add {
                        level: 0,
                        scope: Scope {
                            min: "2".to_string(),
                            max: "3".to_string(),
                            gen: gen_1,
                            wal_ids: None,
                        },
                    },
                    VersionEdit::Add {
                        level: 0,
                        scope: Scope {
                            min: "4".to_string(),
                            max: "5".to_string(),
                            gen: gen_2,
                            wal_ids: None,
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

        let path = Path::from_url_path(format!("snap/{}", generate_file_id())).unwrap();
        version_log_snap_shot(fs_option, path).await;
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
                        },
                    },
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "7".to_string(),
                            max: "9".to_string(),
                            gen: gen_2,
                            wal_ids: None,
                        },
                    },
                    VersionEdit::Add {
                        level: 1,
                        scope: Scope {
                            min: "0".to_string(),
                            max: "0".to_string(),
                            gen: gen_3,
                            wal_ids: None,
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
