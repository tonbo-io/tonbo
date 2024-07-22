use std::{
    io::SeekFrom,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_lock::RwLock;
use flume::Sender;
use futures_util::{AsyncSeekExt, AsyncWriteExt};

use super::MAX_LEVEL;
use crate::{
    fs::{FileId, FileProvider},
    oracle::Timestamp,
    record::Record,
    serdes::Encode,
    version::{cleaner::CleanTag, edit::VersionEdit, Version, VersionError, VersionRef},
    DbOption,
};

static GLOBAL_TIMESTAMP: AtomicU32 = AtomicU32::new(0);

pub(crate) struct VersionSetInner<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    current: VersionRef<R, FP>,
    log: FP::File,
}

pub(crate) struct VersionSet<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    inner: Arc<RwLock<VersionSetInner<R, FP>>>,
    clean_sender: Sender<CleanTag>,
    option: Arc<DbOption>,
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
            option: self.option.clone(),
        }
    }
}

impl<R, FP> VersionSet<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) async fn new(
        clean_sender: Sender<CleanTag>,
        option: Arc<DbOption>,
    ) -> Result<Self, VersionError<R>> {
        let mut log = FP::open(option.version_path()).await?;
        let edits = VersionEdit::recover(&mut log).await;
        log.seek(SeekFrom::End(0)).await?;

        let set = VersionSet::<R, FP> {
            inner: Arc::new(RwLock::new(VersionSetInner {
                current: Arc::new(Version::<R, FP> {
                    ts: Timestamp::from(0),
                    level_slice: [const { Vec::new() }; MAX_LEVEL],
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

    pub(crate) fn transaction_ts() -> Timestamp {
        GLOBAL_TIMESTAMP.fetch_add(1, Ordering::Release).into()
    }

    pub(crate) async fn current(&self) -> VersionRef<R, FP> {
        self.inner.read().await.current.clone()
    }

    pub(crate) async fn apply_edits(
        &self,
        version_edits: Vec<VersionEdit<R::Key>>,
        delete_gens: Option<Vec<FileId>>,
        is_recover: bool,
    ) -> Result<(), VersionError<R>> {
        let mut guard = self.inner.write().await;

        let mut new_version = Version::clone(&guard.current);

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
                            FP::remove(self.option.wal_path(&wal_id))
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
                VersionEdit::LatestTimeStamp { ts } => {
                    if is_recover {
                        GLOBAL_TIMESTAMP.store(u32::from(ts) + 1, Ordering::Release);
                    }
                    new_version.ts = ts;
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
        guard.log.flush().await.map_err(VersionError::Io)?;
        guard.current = Arc::new(new_version);
        Ok(())
    }
}
