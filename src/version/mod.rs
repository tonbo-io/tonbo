mod cleaner;
pub(crate) mod edit;
pub(crate) mod set;

use std::{marker::PhantomData, ops::Bound, sync::Arc};

use flume::{SendError, Sender};
use thiserror::Error;
use tracing::error;

use crate::{
    fs::{FileId, FileProvider},
    ondisk::sstable::SsTable,
    oracle::{timestamp::TimestampedRef, Timestamp},
    record::Record,
    scope::Scope,
    serdes::Encode,
    stream::{record_batch::RecordBatchEntry, ScanStream},
    version::cleaner::CleanTag,
    DbOption,
};

pub(crate) const MAX_LEVEL: usize = 7;

pub(crate) type VersionRef<R, FP> = Arc<Version<R, FP>>;

pub(crate) struct Version<R, FP>
where
    R: Record,
{
    num: usize,
    pub(crate) level_slice: [Vec<Scope<R::Key>>; MAX_LEVEL],
    clean_sender: Sender<CleanTag>,
    option: Arc<DbOption>,
    _p: PhantomData<FP>,
}

impl<R, FP> Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) fn new(option: Arc<DbOption>, clean_sender: Sender<CleanTag>) -> Self {
        Version {
            num: 0,
            level_slice: [const { Vec::new() }; MAX_LEVEL],
            clean_sender,
            option: option.clone(),
            _p: Default::default(),
        }
    }

    pub(crate) fn option(&self) -> &Arc<DbOption> {
        &self.option
    }
}

impl<R, FP> Clone for Version<R, FP>
where
    R: Record,
{
    fn clone(&self) -> Self {
        let mut level_slice = [const { Vec::new() }; MAX_LEVEL];

        for (level, scopes) in self.level_slice.iter().enumerate() {
            level_slice[level].clone_from(scopes);
        }

        Self {
            num: self.num,
            level_slice,
            clean_sender: self.clean_sender.clone(),
            option: self.option.clone(),
            _p: Default::default(),
        }
    }
}

impl<R, FP> Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) async fn query(
        &self,
        key: &TimestampedRef<R::Key>,
    ) -> Result<Option<RecordBatchEntry<R>>, VersionError<R>> {
        for scope in self.level_slice[0].iter().rev() {
            if !scope.contains(key.value()) {
                continue;
            }
            if let Some(entry) = self.table_query(key, &scope.gen).await? {
                return Ok(Some(entry));
            }
        }
        for level in self.level_slice[1..6].iter() {
            if level.is_empty() {
                continue;
            }
            let index = Self::scope_search(key.value(), level);
            if !level[index].contains(key.value()) {
                continue;
            }
            if let Some(entry) = self.table_query(key, &level[index].gen).await? {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    async fn table_query(
        &self,
        key: &TimestampedRef<<R as Record>::Key>,
        gen: &FileId,
    ) -> Result<Option<RecordBatchEntry<R>>, VersionError<R>> {
        let file = FP::open(self.option.table_path(gen))
            .await
            .map_err(VersionError::Io)?;
        SsTable::<R, FP>::open(file)
            .get(key)
            .await
            .map_err(VersionError::Parquet)
    }

    pub(crate) fn scope_search(key: &R::Key, level: &[Scope<R::Key>]) -> usize {
        level
            .binary_search_by(|scope| scope.min.cmp(key))
            .unwrap_or_else(|index| index.saturating_sub(1))
    }

    pub(crate) fn tables_len(&self, level: usize) -> usize {
        self.level_slice[level].len()
    }

    pub(crate) async fn iters<'iters>(
        &self,
        iters: &mut Vec<ScanStream<'iters, R, FP>>,
        range: (Bound<&'iters R::Key>, Bound<&'iters R::Key>),
        ts: Timestamp,
        limit: Option<usize>,
    ) -> Result<(), VersionError<R>> {
        for scope in self.level_slice[0].iter() {
            let file = FP::open(self.option.table_path(&scope.gen))
                .await
                .map_err(VersionError::Io)?;
            let table = SsTable::open(file);

            iters.push(ScanStream::SsTable {
                inner: table
                    .scan(range, ts, limit)
                    .await
                    .map_err(VersionError::Parquet)?,
            })
        }
        for scopes in self.level_slice[1..].iter() {
            if scopes.is_empty() {
                continue;
            }
            let _gens = scopes.iter().map(|scope| scope.gen).collect::<Vec<_>>();
            todo!("level stream")
            // iters.push(EStreamImpl::Level(
            //     LevelStream::new(option, gens, lower, upper).await?,
            // ));
        }
        Ok(())
    }
}

impl<R, FP> Drop for Version<R, FP>
where
    R: Record,
{
    fn drop(&mut self) {
        if let Err(err) = self.clean_sender.send(CleanTag::Clean {
            version_num: self.num,
        }) {
            error!("[Version Drop Error]: {}", err)
        }
    }
}

#[derive(Debug, Error)]
pub enum VersionError<R>
where
    R: Record,
{
    #[error("version encode error: {0}")]
    Encode(#[source] <R::Key as Encode>::Error),
    #[error("version io error: {0}")]
    Io(#[source] std::io::Error),
    #[error("version parquet error: {0}")]
    Parquet(#[source] parquet::errors::ParquetError),
    #[error("version send error: {0}")]
    Send(#[source] SendError<CleanTag>),
}
