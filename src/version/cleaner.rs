use std::{collections::BTreeMap, sync::Arc};

use flume::{Receiver, Sender};

use crate::{
    fs::{manager::StoreManager, FileId},
    record::Record,
    timestamp::Timestamp,
    DbError, DbOption,
};

pub enum CleanTag {
    Add {
        ts: Timestamp,
        gens: Vec<(FileId, usize)>,
    },
    Clean {
        ts: Timestamp,
    },
    RecoverClean {
        wal_id: FileId,
    },
}

pub(crate) struct Cleaner<R>
where
    R: Record,
{
    tag_recv: Receiver<CleanTag>,
    gens_map: BTreeMap<Timestamp, (Vec<(FileId, usize)>, bool)>,
    option: Arc<DbOption<R>>,
    manager: Arc<StoreManager>,
}

impl<R> Cleaner<R>
where
    R: Record,
{
    pub(crate) fn new(
        option: Arc<DbOption<R>>,
        manager: Arc<StoreManager>,
    ) -> (Self, Sender<CleanTag>) {
        let (tag_send, tag_recv) = flume::bounded(option.clean_channel_buffer);

        (
            Cleaner {
                tag_recv,
                gens_map: Default::default(),
                option,
                manager,
            },
            tag_send,
        )
    }

    pub(crate) async fn listen(&mut self) -> Result<(), DbError<R>> {
        while let Ok(tag) = self.tag_recv.recv_async().await {
            match tag {
                CleanTag::Add { ts, gens } => {
                    let _ = self.gens_map.insert(ts, (gens, false));
                }
                CleanTag::Clean { ts: version_num } => {
                    if let Some((_, dropped)) = self.gens_map.get_mut(&version_num) {
                        *dropped = true;
                    }
                    while let Some((first_version, (gens, dropped))) = self.gens_map.pop_first() {
                        if !dropped {
                            let _ = self.gens_map.insert(first_version, (gens, false));
                            break;
                        }
                        for (gen, level) in gens {
                            let fs = self
                                .option
                                .level_fs_path(level)
                                .map(|path| self.manager.get_fs(path))
                                .unwrap_or(self.manager.base_fs());
                            fs.remove(&self.option.table_path(&gen)).await?;
                        }
                    }
                }
                CleanTag::RecoverClean { wal_id: gen } => {
                    let fs = self
                        .option
                        .level_fs_path(0)
                        .map(|path| self.manager.get_fs(path))
                        .unwrap_or(self.manager.base_fs());
                    fs.remove(&self.option.table_path(&gen)).await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{sync::Arc, time::Duration};

    use fusio::{
        local::TokioFs,
        path::{path_to_local, Path},
    };
    use tempfile::TempDir;
    use tokio::time::sleep;
    use tracing::error;

    use crate::{
        executor::{tokio::TokioExecutor, Executor},
        fs::{default_open_options, manager::StoreManager, FileId},
        tests::Test,
        version::cleaner::{CleanTag, Cleaner},
        DbOption,
    };

    #[tokio::test]
    async fn test_cleaner() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(Arc::new(TokioFs), vec![]));
        let option = Arc::new(DbOption::from(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        let gen_0 = FileId::new();
        let gen_1 = FileId::new();
        let gen_2 = FileId::new();
        let gen_3 = FileId::new();
        let fs = option
            .level_fs_path(0)
            .map(|path| manager.get_fs(path))
            .unwrap_or(manager.base_fs());
        {
            fs.open_options(&option.table_path(&gen_0), default_open_options())
                .await
                .unwrap();
            fs.open_options(&option.table_path(&gen_1), default_open_options())
                .await
                .unwrap();
            fs.open_options(&option.table_path(&gen_2), default_open_options())
                .await
                .unwrap();
            fs.open_options(&option.table_path(&gen_3), default_open_options())
                .await
                .unwrap();
        }

        let (mut cleaner, tx) = Cleaner::<Test>::new(option.clone(), manager.clone());

        let executor = TokioExecutor::new();

        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });

        tx.send_async(CleanTag::Add {
            ts: 1.into(),
            gens: vec![(gen_1, 0)],
        })
        .await
        .unwrap();
        tx.send_async(CleanTag::Add {
            ts: 0.into(),
            gens: vec![(gen_0, 0)],
        })
        .await
        .unwrap();
        tx.send_async(CleanTag::Add {
            ts: 2.into(),
            gens: vec![(gen_2, 0)],
        })
        .await
        .unwrap();

        tx.send_async(CleanTag::Clean { ts: 2.into() })
            .await
            .unwrap();

        // FIXME
        assert!(path_to_local(&option.table_path(&gen_0)).unwrap().exists());
        assert!(path_to_local(&option.table_path(&gen_1)).unwrap().exists());
        assert!(path_to_local(&option.table_path(&gen_2)).unwrap().exists());
        assert!(path_to_local(&option.table_path(&gen_3)).unwrap().exists());

        tx.send_async(CleanTag::Clean { ts: 0.into() })
            .await
            .unwrap();
        sleep(Duration::from_millis(10)).await;
        assert!(!path_to_local(&option.table_path(&gen_0)).unwrap().exists());
        assert!(path_to_local(&option.table_path(&gen_1)).unwrap().exists());
        assert!(path_to_local(&option.table_path(&gen_2)).unwrap().exists());
        assert!(path_to_local(&option.table_path(&gen_3)).unwrap().exists());

        tx.send_async(CleanTag::Clean { ts: 1.into() })
            .await
            .unwrap();
        sleep(Duration::from_millis(10)).await;
        assert!(!path_to_local(&option.table_path(&gen_1)).unwrap().exists());
        assert!(!path_to_local(&option.table_path(&gen_2)).unwrap().exists());
        assert!(path_to_local(&option.table_path(&gen_3)).unwrap().exists());

        tx.send_async(CleanTag::RecoverClean { wal_id: gen_3 })
            .await
            .unwrap();
        sleep(Duration::from_millis(10)).await;
        assert!(!path_to_local(&option.table_path(&gen_3)).unwrap().exists());
    }
}
