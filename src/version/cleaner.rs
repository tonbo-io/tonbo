use std::{collections::BTreeMap, io, marker::PhantomData, sync::Arc};

use flume::{Receiver, Sender};

use crate::{
    fs::{FileId, FileProvider},
    record::Record,
    timestamp::Timestamp,
    DbOption,
};

pub enum CleanTag {
    Add { ts: Timestamp, gens: Vec<FileId> },
    Clean { ts: Timestamp },
    RecoverClean { gen: FileId },
}

pub(crate) struct Cleaner<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    tag_recv: Receiver<CleanTag>,
    gens_map: BTreeMap<Timestamp, (Vec<FileId>, bool)>,
    option: Arc<DbOption<R>>,
    _p: PhantomData<FP>,
}

impl<R, FP> Cleaner<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) fn new(option: Arc<DbOption<R>>) -> (Self, Sender<CleanTag>) {
        let (tag_send, tag_recv) = flume::bounded(option.clean_channel_buffer);

        (
            Cleaner {
                tag_recv,
                gens_map: Default::default(),
                option,
                _p: Default::default(),
            },
            tag_send,
        )
    }

    pub(crate) async fn listen(&mut self) -> Result<(), io::Error> {
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
                        for gen in gens {
                            FP::remove(self.option.table_path(&gen)).await?;
                        }
                    }
                }
                CleanTag::RecoverClean { gen } => {
                    FP::remove(self.option.table_path(&gen)).await?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{sync::Arc, time::Duration};

    use tempfile::TempDir;
    use tokio::time::sleep;
    use tracing::error;

    use crate::{
        executor::{tokio::TokioExecutor, Executor},
        fs::{FileId, FileProvider},
        tests::Test,
        version::cleaner::{CleanTag, Cleaner},
        DbOption,
    };

    #[tokio::test]
    async fn test_cleaner() {
        let temp_dir = TempDir::new().unwrap();
        let option = Arc::new(DbOption::from(temp_dir.path()));

        let gen_0 = FileId::new();
        let gen_1 = FileId::new();
        let gen_2 = FileId::new();
        let gen_3 = FileId::new();
        {
            TokioExecutor::open(option.table_path(&gen_0))
                .await
                .unwrap();
            TokioExecutor::open(option.table_path(&gen_1))
                .await
                .unwrap();
            TokioExecutor::open(option.table_path(&gen_2))
                .await
                .unwrap();
            TokioExecutor::open(option.table_path(&gen_3))
                .await
                .unwrap();
        }

        let (mut cleaner, tx) = Cleaner::<Test, TokioExecutor>::new(option.clone());

        let executor = TokioExecutor::new();

        executor.spawn(async move {
            if let Err(err) = cleaner.listen().await {
                error!("[Cleaner Error]: {}", err)
            }
        });

        tx.send_async(CleanTag::Add {
            ts: 1.into(),
            gens: vec![gen_1],
        })
        .await
        .unwrap();
        tx.send_async(CleanTag::Add {
            ts: 0.into(),
            gens: vec![gen_0],
        })
        .await
        .unwrap();
        tx.send_async(CleanTag::Add {
            ts: 2.into(),
            gens: vec![gen_2],
        })
        .await
        .unwrap();

        tx.send_async(CleanTag::Clean { ts: 2.into() })
            .await
            .unwrap();
        assert!(TokioExecutor::file_exist(option.table_path(&gen_0))
            .await
            .unwrap());
        assert!(TokioExecutor::file_exist(option.table_path(&gen_1))
            .await
            .unwrap());
        assert!(TokioExecutor::file_exist(option.table_path(&gen_2))
            .await
            .unwrap());
        assert!(TokioExecutor::file_exist(option.table_path(&gen_3))
            .await
            .unwrap());

        tx.send_async(CleanTag::Clean { ts: 0.into() })
            .await
            .unwrap();
        sleep(Duration::from_millis(1)).await;
        assert!(!TokioExecutor::file_exist(option.table_path(&gen_0))
            .await
            .unwrap());
        assert!(TokioExecutor::file_exist(option.table_path(&gen_1))
            .await
            .unwrap());
        assert!(TokioExecutor::file_exist(option.table_path(&gen_2))
            .await
            .unwrap());
        assert!(TokioExecutor::file_exist(option.table_path(&gen_3))
            .await
            .unwrap());

        tx.send_async(CleanTag::Clean { ts: 1.into() })
            .await
            .unwrap();
        sleep(Duration::from_millis(1)).await;
        assert!(!TokioExecutor::file_exist(option.table_path(&gen_1))
            .await
            .unwrap());
        assert!(!TokioExecutor::file_exist(option.table_path(&gen_2))
            .await
            .unwrap());
        assert!(TokioExecutor::file_exist(option.table_path(&gen_3))
            .await
            .unwrap());

        tx.send_async(CleanTag::RecoverClean { gen: gen_3 })
            .await
            .unwrap();
        sleep(Duration::from_millis(1)).await;
        assert!(!TokioExecutor::file_exist(option.table_path(&gen_3))
            .await
            .unwrap());
    }
}
