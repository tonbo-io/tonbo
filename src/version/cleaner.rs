use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use flume::{Receiver, Sender};
use object_store::ObjectStore;
use thiserror::Error;

use crate::{
    fs::{
        store_manager::{StoreManager, StoreManagerError},
        FileId, FileProvider,
    },
    record::Record,
    timestamp::Timestamp,
    DbOption,
};

pub enum CleanTag {
    Add {
        ts: Timestamp,
        gens: Vec<(FileId, usize)>,
    },
    Clean {
        ts: Timestamp,
    },
}

pub(crate) struct Cleaner<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    tag_recv: Receiver<CleanTag>,
    gens_map: BTreeMap<Timestamp, (Vec<(FileId, usize)>, bool)>,
    option: Arc<DbOption<R>>,
    store_manager: Arc<StoreManager>,
    _p: PhantomData<FP>,
}

impl<R, FP> Cleaner<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) fn new(
        option: Arc<DbOption<R>>,
        store_manager: Arc<StoreManager>,
    ) -> (Self, Sender<CleanTag>) {
        let (tag_send, tag_recv) = flume::bounded(option.clean_channel_buffer);

        (
            Cleaner {
                tag_recv,
                gens_map: Default::default(),
                option,
                store_manager,
                _p: Default::default(),
            },
            tag_send,
        )
    }

    pub(crate) async fn listen(&mut self) -> Result<(), CleanerError> {
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
                            self.option
                                .level_store(level, &self.store_manager)?
                                .delete(&self.option.table_path(&gen))
                                .await?
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CleanerError {
    #[error("cleaner object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("cleaner store manager error: {0}")]
    StoreManagerError(#[from] StoreManagerError),
}

// TODO: TestCase
