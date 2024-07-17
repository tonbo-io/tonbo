mod cleaner;
mod edit;
mod set;

use std::{marker::PhantomData, ops::Bound, sync::Arc};

use flume::{SendError, Sender};
use thiserror::Error;
use tracing::error;

use crate::{
    executor::Executor,
    fs::{AsyncFile, FileId},
    ondisk::sstable::SsTable,
    oracle::{timestamp::TimestampedRef, Timestamp},
    record::Record,
    scope::Scope,
    serdes::Encode,
    stream::{record_batch::RecordBatchEntry, ScanStream},
    version::cleaner::CleanTag,
    DbOption,
};

const MAX_LEVEL: usize = 7;

pub(crate) type VersionRef<R, E> = Arc<Version<R, E>>;

pub(crate) struct Version<R, E>
where
    R: Record,
{
    pub(crate) num: usize,
    pub(crate) level_slice: [Vec<Scope<R::Key>>; MAX_LEVEL],
    pub(crate) clean_sender: Sender<CleanTag>,
    pub(crate) option: Arc<DbOption>,
    _p: PhantomData<E>,
}

impl<R, E> Clone for Version<R, E>
where
    R: Record,
    E: Executor,
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

impl<R, E> Version<R, E>
where
    R: Record,
    E: Executor,
{
    pub(crate) async fn query(
        &self,
        key: &TimestampedRef<R::Key>,
    ) -> Result<Option<RecordBatchEntry<R>>, VersionError<R>> {
        for scope in self.level_slice[0].iter().rev() {
            if !scope.is_between(key.value()) {
                continue;
            }
            if let Some(entry) = Self::table_query(self, key, &scope.gen).await? {
                return Ok(Some(entry));
            }
        }
        for level in self.level_slice[1..6].iter() {
            if level.is_empty() {
                continue;
            }
            let index = Self::scope_search(key.value(), level);
            if !level[index].is_between(key.value()) {
                continue;
            }
            if let Some(entry) = Self::table_query(self, key, &level[index].gen).await? {
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
        let file = E::open(self.option.table_path(gen))
            .await
            .map_err(VersionError::Io)?
            .boxed();
        let table = SsTable::open(file);
        table.get(key).await.map_err(VersionError::Parquet)
    }

    pub(crate) fn scope_search(key: &R::Key, level: &[Scope<R::Key>]) -> usize {
        level
            .binary_search_by(|scope| scope.min.cmp(key))
            .unwrap_or_else(|index| index.saturating_sub(1))
    }

    pub(crate) fn tables_len(&self, level: usize) -> usize {
        self.level_slice[level].len()
    }

    pub(crate) async fn iters<'a>(
        &self,
        iters: &mut Vec<ScanStream<'a, R>>,
        range: (Bound<&'a R::Key>, Bound<&'a R::Key>),
        ts: Timestamp,
    ) -> Result<(), VersionError<R>> {
        for scope in self.level_slice[0].iter() {
            let file = E::open(self.option.table_path(&scope.gen))
                .await
                .map_err(VersionError::Io)?
                .boxed();
            let table = SsTable::open(file);

            iters.push(ScanStream::SsTable {
                inner: table.scan(range, ts).await.map_err(VersionError::Parquet)?,
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

impl<R, E> Drop for Version<R, E>
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
