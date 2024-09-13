use std::{
    io::SeekFrom,
    mem,
    pin::pin,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_lock::RwLock;
use flume::Sender;
use futures_util::StreamExt;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::{TransactionTs, MAX_LEVEL};
use crate::{
    fs::{FileId, FileProvider, FileType},
    record::Record,
    serdes::Encode,
    timestamp::Timestamp,
    version::{cleaner::CleanTag, edit::VersionEdit, Version, VersionError, VersionRef},
    DbOption,
};

pub(crate) struct VersionSetInner<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    current: VersionRef<R, FP>,
    log_with_id: (FP::File, FileId),
}

pub(crate) struct VersionSet<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    inner: Arc<RwLock<VersionSetInner<R, FP>>>,
    clean_sender: Sender<CleanTag>,
    timestamp: Arc<AtomicU32>,
    option: Arc<DbOption<R>>,
}

impl<R, FP> Clone for VersionSet<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    fn clone(&self) -> Self {
        VersionSet {
            inner: self.inner.clone(),
            clean_sender: self.clean_sender.clone(),
            timestamp: self.timestamp.clone(),
            option: self.option.clone(),
        }
    }
}

impl<R, FP> TransactionTs for VersionSet<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    fn load_ts(&self) -> Timestamp {
        self.timestamp.load(Ordering::Acquire).into()
    }

    fn increase_ts(&self) -> Timestamp {
        (self.timestamp.fetch_add(1, Ordering::Release) + 1).into()
    }
}

impl<R, FP> VersionSet<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) async fn new(
        clean_sender: Sender<CleanTag>,
        option: Arc<DbOption<R>>,
    ) -> Result<Self, VersionError<R>> {
        let mut log_stream = pin!(FP::list(
            option.version_log_dir_path(),
            FileType::Log,
            true
        )?);
        let mut first_log_id = None;
        let mut version_log_id = None;
        let mut version_log = None;

        // when there are multiple logs, this means that a downtime occurred during the
        // `version_log_snap_shot` process, the second newest file has the highest data
        // integrity, so it is used as the version log, and the older log is deleted first
        // to avoid midway downtime, which will cause the second newest file to become the
        // first newest after restart.
        let mut i = 0;
        while let Some(result) = log_stream.next().await {
            let (log, log_id) = result?;

            if i <= 1 {
                version_log = Some(log);
                first_log_id = mem::replace(&mut version_log_id, Some(log_id));
            } else {
                FP::remove(option.version_log_path(&log_id)).await?;
            }

            i += 1;
        }
        if let Some(log_id) = first_log_id {
            FP::remove(option.version_log_path(&log_id)).await?;
        }

        let (mut log, log_id) = if let (Some(log), Some(log_id)) = (version_log, version_log_id) {
            (log, log_id)
        } else {
            let log_id = FileId::new();
            let log = FP::open(option.version_log_path(&log_id)).await?;
            (log, log_id)
        };

        let edits = VersionEdit::recover(&mut log).await;
        log.seek(SeekFrom::End(0)).await?;

        let timestamp = Arc::new(AtomicU32::default());
        let set = VersionSet::<R, FP> {
            inner: Arc::new(RwLock::new(VersionSetInner {
                current: Arc::new(Version::<R, FP> {
                    ts: Timestamp::from(0),
                    level_slice: [const { Vec::new() }; MAX_LEVEL],
                    clean_sender: clean_sender.clone(),
                    option: option.clone(),
                    timestamp: timestamp.clone(),
                    log_length: 0,
                    _p: Default::default(),
                }),
                log_with_id: (log, log_id),
            })),
            clean_sender,
            timestamp,
            option,
        };
        set.apply_edits(edits, None, true).await?;

        Ok(set)
    }

    pub(crate) async fn current(&self) -> VersionRef<R, FP> {
        self.inner.read().await.current.clone()
    }

    pub(crate) async fn apply_edits(
        &self,
        mut version_edits: Vec<VersionEdit<R::Key>>,
        delete_gens: Option<Vec<FileId>>,
        is_recover: bool,
    ) -> Result<(), VersionError<R>> {
        let timestamp = &self.timestamp;
        let option = &self.option;
        let mut guard = self.inner.write().await;
        let mut new_version = Version::clone(&guard.current);
        let (log, log_id) = &mut guard.log_with_id;
        let edit_len = new_version.log_length + version_edits.len() as u32;

        if !is_recover {
            version_edits.push(VersionEdit::NewLogLength { len: edit_len });
        }
        for version_edit in version_edits {
            if !is_recover {
                version_edit
                    .encode(log)
                    .await
                    .map_err(VersionError::Encode)?;
            }
            match version_edit {
                VersionEdit::Add { mut scope, level } => {
                    if let Some(wal_ids) = scope.wal_ids.take() {
                        for wal_id in wal_ids {
                            // may have been removed after multiple starts
                            let _ = FP::remove(option.wal_path(&wal_id)).await;
                        }
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
                        new_version
                            .clean_sender
                            .send_async(CleanTag::RecoverClean { gen })
                            .await
                            .map_err(VersionError::Send)?;
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
            new_version
                .clean_sender
                .send_async(CleanTag::Add {
                    ts: new_version.ts,
                    gens: delete_gens,
                })
                .await
                .map_err(VersionError::Send)?;
        }
        log.flush().await?;
        if edit_len >= option.version_log_snapshot_threshold {
            let old_log_id = mem::replace(log_id, FileId::new());
            let _ = mem::replace(log, FP::open(option.version_log_path(log_id)).await?);

            new_version.log_length = 0;
            for new_edit in new_version.to_edits() {
                new_edit.encode(log).await.map_err(VersionError::Encode)?;
            }
            log.flush().await?;
            FP::remove(option.version_log_path(&old_log_id)).await?;
        }
        guard.current = Arc::new(new_version);
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{io::SeekFrom, pin::pin, sync::Arc};

    use async_lock::RwLock;
    use flume::{bounded, Sender};
    use futures_util::StreamExt;
    use tempfile::TempDir;
    use tokio::io::AsyncSeekExt;

    use crate::{
        executor::tokio::TokioExecutor,
        fs::{FileId, FileProvider, FileType},
        record::Record,
        scope::Scope,
        version::{
            cleaner::CleanTag,
            edit::VersionEdit,
            set::{VersionSet, VersionSetInner},
            TransactionTs, Version, VersionError,
        },
        DbOption,
    };

    pub(crate) async fn build_version_set<R, FP>(
        version: Version<R, FP>,
        clean_sender: Sender<CleanTag>,
        option: Arc<DbOption<R>>,
    ) -> Result<VersionSet<R, FP>, VersionError<R>>
    where
        R: Record,
        FP: FileProvider,
    {
        let log_id = FileId::new();
        let mut log = FP::open(option.version_log_path(&log_id)).await?;
        log.seek(SeekFrom::End(0)).await?;

        let timestamp = version.timestamp.clone();
        Ok(VersionSet::<R, FP> {
            inner: Arc::new(RwLock::new(VersionSetInner {
                current: Arc::new(version),
                log_with_id: (log, log_id),
            })),
            clean_sender,
            timestamp,
            option,
        })
    }

    #[tokio::test]
    async fn timestamp_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let (sender, _) = bounded(1);
        let option = Arc::new(DbOption::from(temp_dir.path()));
        TokioExecutor::create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();

        let version_set: VersionSet<String, TokioExecutor> =
            VersionSet::new(sender.clone(), option.clone())
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

        let version_set: VersionSet<String, TokioExecutor> =
            VersionSet::new(sender.clone(), option.clone())
                .await
                .unwrap();
        assert_eq!(version_set.load_ts(), 20_u32.into());
    }

    #[tokio::test]
    async fn version_log_snap_shot() {
        let temp_dir = TempDir::new().unwrap();
        let (sender, _) = bounded(1);
        let mut option = DbOption::from(temp_dir.path());
        option.version_log_snapshot_threshold = 4;

        let option = Arc::new(option);
        TokioExecutor::create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();

        let version_set: VersionSet<String, TokioExecutor> =
            VersionSet::new(sender.clone(), option.clone())
                .await
                .unwrap();
        let gen_0 = FileId::new();
        let gen_1 = FileId::new();
        let gen_2 = FileId::new();

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

        let mut guard = version_set.inner.write().await;
        let log = &mut guard.log_with_id.0;

        log.seek(SeekFrom::Start(0)).await.unwrap();
        let edits = VersionEdit::<String>::recover(log).await;

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

        let (mut log, _) =
            pin!(TokioExecutor::list(option.version_log_dir_path(), FileType::Log, true).unwrap())
                .next()
                .await
                .unwrap()
                .unwrap();
        let edits = VersionEdit::<String>::recover(&mut log).await;

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

    #[tokio::test]
    async fn version_level_sort() {
        let temp_dir = TempDir::new().unwrap();
        let option = DbOption::from(temp_dir.path());
        let option = Arc::new(option);

        let (sender, _) = bounded(1);
        TokioExecutor::create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();

        let version_set: VersionSet<String, TokioExecutor> =
            VersionSet::new(sender.clone(), option.clone())
                .await
                .unwrap();
        let gen_0 = FileId::new();
        let gen_1 = FileId::new();
        let gen_2 = FileId::new();
        let gen_3 = FileId::new();
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
