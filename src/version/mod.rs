pub(crate) mod cleaner;
pub(crate) mod edit;
pub(crate) mod set;

use std::{
    collections::VecDeque,
    marker::PhantomData,
    ops::Bound,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use flume::{SendError, Sender};
use parquet::arrow::ProjectionMask;
use thiserror::Error;
use tracing::error;

use crate::{
    fs::{FileId, FileProvider},
    ondisk::sstable::SsTable,
    record::Record,
    scope::Scope,
    serdes::Encode,
    stream::{level::LevelStream, record_batch::RecordBatchEntry, ScanStream},
    timestamp::{Timestamp, TimestampedRef},
    version::{cleaner::CleanTag, edit::VersionEdit},
    DbOption,
};

pub(crate) const MAX_LEVEL: usize = 7;

pub(crate) type VersionRef<R, FP> = Arc<Version<R, FP>>;

pub(crate) trait TransactionTs {
    fn load_ts(&self) -> Timestamp;

    fn increase_ts(&self) -> Timestamp;
}

#[derive(Debug)]
pub(crate) struct Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    ts: Timestamp,
    pub(crate) level_slice: [Vec<Scope<R::Key>>; MAX_LEVEL],
    clean_sender: Sender<CleanTag>,
    option: Arc<DbOption<R>>,
    timestamp: Arc<AtomicU32>,
    log_length: u32,
    _p: PhantomData<FP>,
}

impl<R, FP> Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    #[cfg(test)]
    pub(crate) fn new(
        option: Arc<DbOption<R>>,
        clean_sender: Sender<CleanTag>,
        timestamp: Arc<AtomicU32>,
    ) -> Self {
        Version {
            ts: Timestamp::from(0),
            level_slice: [const { Vec::new() }; MAX_LEVEL],
            clean_sender,
            option: option.clone(),
            timestamp,
            log_length: 0,
            _p: Default::default(),
        }
    }

    pub(crate) fn option(&self) -> &Arc<DbOption<R>> {
        &self.option
    }
}

impl<R, FP> TransactionTs for Version<R, FP>
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

impl<R, FP> Clone for Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    fn clone(&self) -> Self {
        let mut level_slice = [const { Vec::new() }; MAX_LEVEL];

        for (level, scopes) in self.level_slice.iter().enumerate() {
            level_slice[level].clone_from(scopes);
        }

        Self {
            ts: self.ts,
            level_slice,
            clean_sender: self.clean_sender.clone(),
            option: self.option.clone(),
            timestamp: self.timestamp.clone(),
            log_length: self.log_length,
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
        projection_mask: ProjectionMask,
    ) -> Result<Option<RecordBatchEntry<R>>, VersionError<R>> {
        for scope in self.level_slice[0].iter().rev() {
            if !scope.contains(key.value()) {
                continue;
            }
            if let Some(entry) = self
                .table_query(key, &scope.gen, projection_mask.clone())
                .await?
            {
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
            if let Some(entry) = self
                .table_query(key, &level[index].gen, projection_mask.clone())
                .await?
            {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    async fn table_query(
        &self,
        key: &TimestampedRef<<R as Record>::Key>,
        gen: &FileId,
        projection_mask: ProjectionMask,
    ) -> Result<Option<RecordBatchEntry<R>>, VersionError<R>> {
        let file = FP::open(self.option.table_path(gen))
            .await
            .map_err(VersionError::Io)?;
        SsTable::<R, FP>::open(file)
            .get(key, projection_mask)
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

    pub(crate) async fn streams<'streams>(
        &self,
        streams: &mut Vec<ScanStream<'streams, R, FP>>,
        range: (Bound<&'streams R::Key>, Bound<&'streams R::Key>),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> Result<(), VersionError<R>> {
        for scope in self.level_slice[0].iter() {
            if !scope.meets_range(range) {
                continue;
            }
            let file = FP::open(self.option.table_path(&scope.gen))
                .await
                .map_err(VersionError::Io)?;
            let table = SsTable::open(file);

            streams.push(ScanStream::SsTable {
                inner: table
                    .scan(range, ts, limit, projection_mask.clone())
                    .await
                    .map_err(VersionError::Parquet)?,
            })
        }
        for scopes in self.level_slice[1..].iter() {
            if scopes.is_empty() {
                continue;
            }

            let gens: VecDeque<FileId> = scopes
                .iter()
                .filter(|scope| scope.meets_range(range))
                .map(Scope::gen)
                .collect();
            if gens.is_empty() {
                continue;
            }

            streams.push(ScanStream::Level {
                // SAFETY: checked scopes no empty
                inner: LevelStream::new(self, gens, range, ts, limit, projection_mask.clone())
                    .unwrap(),
            });
        }
        Ok(())
    }

    pub(crate) fn to_edits(&self) -> Vec<VersionEdit<R::Key>> {
        let mut edits = Vec::new();

        for (level, scopes) in self.level_slice.iter().enumerate() {
            for scope in scopes {
                edits.push(VersionEdit::Add {
                    level: level as u8,
                    scope: scope.clone(),
                })
            }
        }
        edits.push(VersionEdit::LatestTimeStamp { ts: self.load_ts() });
        edits.push(VersionEdit::NewLogLength { len: 0 });
        edits
    }
}

impl<R, FP> Drop for Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    fn drop(&mut self) {
        if let Err(err) = self.clean_sender.send(CleanTag::Clean { ts: self.ts }) {
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
    Io(#[from] std::io::Error),
    #[error("version parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("version send error: {0}")]
    Send(#[from] SendError<CleanTag>),
}
