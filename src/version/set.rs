use std::{io::SeekFrom, sync::Arc};

use async_lock::RwLock;
use futures_channel::mpsc::Sender;
use futures_util::{AsyncSeekExt, AsyncWriteExt, SinkExt};

use crate::{
    executor::Executor,
    fs::FileId,
    record::Record,
    serdes::Encode,
    version::{cleaner::CleanTag, edit::VersionEdit, Version, VersionError, VersionRef},
    DbOption,
};

pub(crate) struct VersionSetInner<R, E>
where
    R: Record,
    E: Executor,
{
    current: VersionRef<R, E>,
    log: E::File,
}

pub(crate) struct VersionSet<R, E>
where
    R: Record,
    E: Executor,
{
    inner: Arc<RwLock<VersionSetInner<R, E>>>,
    clean_sender: Sender<CleanTag>,
    option: Arc<DbOption>,
}

impl<R, E> Clone for VersionSet<R, E>
where
    R: Record,
    E: Executor,
{
    fn clone(&self) -> Self {
        VersionSet {
            inner: self.inner.clone(),
            clean_sender: self.clean_sender.clone(),
            option: self.option.clone(),
        }
    }
}

impl<R, E> VersionSet<R, E>
where
    R: Record,
    E: Executor,
{
    pub(crate) async fn new(
        clean_sender: Sender<CleanTag>,
        option: Arc<DbOption>,
    ) -> Result<Self, VersionError<R>> {
        let mut log = E::open(option.version_path())
            .await
            .map_err(VersionError::Io)?;
        let edits = VersionEdit::recover(&mut log).await;
        log.seek(SeekFrom::End(0)).await.map_err(VersionError::Io)?;

        let set = VersionSet::<R, E> {
            inner: Arc::new(RwLock::new(VersionSetInner {
                current: Arc::new(Version::<R, E> {
                    num: 0,
                    level_slice: Version::<R, E>::level_slice_new(),
                    clean_sender: clean_sender.clone(),
                    option: option.clone(),
                    _p: Default::default(),
                }),
                log,
            })),
            clean_sender,
            option,
        };
        set.apply_edits(edits, None, true).await?;

        Ok(set)
    }

    pub(crate) async fn current(&self) -> VersionRef<R, E> {
        self.inner.read().await.current.clone()
    }

    pub(crate) async fn apply_edits(
        &self,
        version_edits: Vec<VersionEdit<R::Key>>,
        delete_gens: Option<Vec<FileId>>,
        is_recover: bool,
    ) -> Result<(), VersionError<R>> {
        let mut guard = self.inner.write().await;

        let mut new_version = Version::<R, E>::clone(&guard.current);

        for version_edit in version_edits {
            if !is_recover {
                version_edit
                    .encode(&mut guard.log)
                    .await
                    .map_err(VersionError::Encode)?;
            }
            match version_edit {
                VersionEdit::Add { mut scope, level } => {
                    if let Some(wal_ids) = scope.wal_ids.take() {
                        for wal_id in wal_ids {
                            E::remove(self.option.wal_path(&wal_id))
                                .await
                                .map_err(VersionError::Io)?;
                        }
                    }
                    new_version.level_slice[level as usize].push(scope);
                }
                VersionEdit::Remove { gen, level } => {
                    if let Some(i) = new_version.level_slice[level as usize]
                        .iter()
                        .position(|scope| scope.gen == gen)
                    {
                        new_version.level_slice[level as usize].remove(i);
                    }
                }
            }
        }
        if let Some(delete_gens) = delete_gens {
            new_version
                .clean_sender
                .send(CleanTag::Add {
                    version_num: new_version.num,
                    gens: delete_gens,
                })
                .await
                .map_err(VersionError::Send)?;
        }
        guard.log.flush().await.map_err(VersionError::Io)?;
        guard.current = Arc::new(new_version);
        Ok(())
    }
}
