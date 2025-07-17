pub(crate) mod cleaner;
pub(crate) mod edit;
pub(crate) mod set;

use std::{
    marker::PhantomData,
    ops::Bound,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use common::{util::compare, Keys, PrimaryKey};
use flume::{SendError, Sender};
use fusio::DynFs;
use fusio_log::error::LogError;
use parquet::arrow::ProjectionMask;
use thiserror::Error;
use tracing::error;

use crate::{
    context::Context,
    fs::{manager::StoreManager, FileId, FileType},
    ondisk::sstable::SsTable,
    record::Record,
    scope::Scope,
    stream::{level::LevelStream, record_batch::RecordBatchEntry, ScanStream},
    timestamp::{Timestamp, TsRef},
    version::{cleaner::CleanTag, edit::VersionEdit},
    DbOption, ParquetLru,
};

pub(crate) const MAX_LEVEL: usize = 7;

pub(crate) type VersionRef<R> = Arc<Version<R>>;

pub(crate) trait TransactionTs {
    fn load_ts(&self) -> Timestamp;

    fn increase_ts(&self) -> Timestamp;
}

#[derive(Debug)]
pub(crate) struct Version<R>
where
    R: Record,
{
    ts: Timestamp,
    pub(crate) level_slice: [Vec<Scope>; MAX_LEVEL],
    clean_sender: Sender<CleanTag>,
    option: Arc<DbOption>,
    timestamp: Arc<AtomicU32>,
    log_length: u32,
    _mark: PhantomData<R>,
}

impl<R> Version<R>
where
    R: Record,
{
    #[cfg(test)]
    #[allow(unused)]
    pub(crate) fn new(
        option: Arc<DbOption>,
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
            _mark: PhantomData,
        }
    }

    pub(crate) fn option(&self) -> &Arc<DbOption> {
        &self.option
    }
}

impl<R> TransactionTs for Version<R>
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

impl<R> Clone for Version<R>
where
    R: Record,
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
            _mark: PhantomData,
        }
    }
}

impl<R> Version<R>
where
    R: Record,
{
    pub(crate) async fn query(
        &self,
        manager: &StoreManager,
        key: &TsRef<PrimaryKey>,
        projection_mask: ProjectionMask,
        parquet_lru: ParquetLru,
    ) -> Result<Option<RecordBatchEntry<R>>, VersionError> {
        let level_0_path = self
            .option
            .level_fs_path(0)
            .unwrap_or(&self.option.base_path);
        let level_0_fs = manager.get_fs(level_0_path);
        for scope in self.level_slice[0].iter().rev() {
            if !scope.contains(key.value().keys()) {
                continue;
            }
            if let Some(entry) = self
                .table_query(
                    level_0_fs,
                    key,
                    0,
                    scope.gen,
                    projection_mask.clone(),
                    parquet_lru.clone(),
                )
                .await?
            {
                return Ok(Some(entry));
            }
        }
        for (i, sort_runs) in self.level_slice[1..MAX_LEVEL].iter().enumerate() {
            let leve = i + 1;
            let level_path = self
                .option
                .level_fs_path(leve)
                .unwrap_or(&self.option.base_path);
            let level_fs = manager.get_fs(level_path);
            if sort_runs.is_empty() {
                continue;
            }
            let index = Self::scope_search(key.value().keys(), sort_runs);
            if !sort_runs[index].contains(key.value().keys()) {
                continue;
            }
            if let Some(entry) = self
                .table_query(
                    level_fs,
                    key,
                    leve,
                    sort_runs[index].gen,
                    projection_mask.clone(),
                    parquet_lru.clone(),
                )
                .await?
            {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    async fn table_query(
        &self,
        store: &Arc<dyn DynFs>,
        key: &TsRef<PrimaryKey>,
        level: usize,
        gen: FileId,
        projection_mask: ProjectionMask,
        parquet_lru: ParquetLru,
    ) -> Result<Option<RecordBatchEntry<R>>, VersionError> {
        let file = store
            .open_options(
                &self.option.table_path(gen, level),
                FileType::Parquet.open_options(true),
            )
            .await
            .map_err(VersionError::Fusio)?;
        SsTable::<R>::open(parquet_lru, gen, file)
            .await?
            .get(key, projection_mask)
            .await
            .map_err(VersionError::Parquet)
    }

    pub(crate) fn scope_search(key: &Keys, level: &[Scope]) -> usize {
        level
            .binary_search_by(|scope| compare(&scope.min, key))
            .unwrap_or_else(|index| index.saturating_sub(1))
    }

    pub(crate) fn tables_len(&self, level: usize) -> usize {
        self.level_slice[level].len()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn streams<'streams>(
        &self,
        ctx: &Context<R>,
        streams: &mut Vec<ScanStream<'streams, R>>,
        range: (Bound<&'streams PrimaryKey>, Bound<&'streams PrimaryKey>),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> Result<(), VersionError> {
        let range = (range.0.map(|v| v.keys()), range.1.map(|v| v.keys()));
        let level_0_path = self
            .option
            .level_fs_path(0)
            .unwrap_or(&self.option.base_path);
        let level_0_fs = ctx.manager.get_fs(level_0_path);
        for scope in self.level_slice[0].iter() {
            if !scope.meets_range(range) {
                continue;
            }
            let file = level_0_fs
                .open_options(
                    &self.option.table_path(scope.gen, 0),
                    FileType::Parquet.open_options(true),
                )
                .await
                .map_err(VersionError::Fusio)?;
            let table = SsTable::open(ctx.parquet_lru.clone(), scope.gen, file).await?;

            streams.push(ScanStream::SsTable {
                inner: table
                    .scan(range, ts, limit, projection_mask.clone())
                    .await
                    .map_err(VersionError::Parquet)?,
            })
        }
        for (i, scopes) in self.level_slice[1..].iter().enumerate() {
            if scopes.is_empty() {
                continue;
            }
            let level_path = self
                .option
                .level_fs_path(i + 1)
                .unwrap_or(&self.option.base_path);
            let level_fs = ctx.manager.get_fs(level_path);

            let (mut start, mut end) = (None, None);

            for (idx, scope) in scopes.iter().enumerate() {
                if scope.meets_range(range) {
                    if start.is_none() {
                        start = Some(idx);
                    }
                    end = Some(idx);
                }
            }
            if start.is_none() {
                continue;
            }

            streams.push(ScanStream::Level {
                // SAFETY: checked scopes no empty
                inner: LevelStream::new(
                    self,
                    i + 1,
                    start.unwrap(),
                    end.unwrap(),
                    range,
                    ts,
                    limit,
                    projection_mask.clone(),
                    level_fs.clone(),
                    ctx.parquet_lru.clone(),
                )
                .unwrap(),
            });
        }
        Ok(())
    }

    pub(crate) fn to_edits(&self) -> Vec<VersionEdit> {
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

impl<R> Drop for Version<R>
where
    R: Record,
{
    fn drop(&mut self) {
        if let Err(err) = self.clean_sender.send(CleanTag::Clean { ts: self.ts }) {
            error!("[Version Drop Error]: {}", err)
        }
    }
}

#[derive(Debug, Error)]
pub enum VersionError {
    #[error("version encode error: {0}")]
    Encode(#[source] fusio::Error),
    #[error("version io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("version parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("version fusio error: {0}")]
    Fusio(#[from] fusio::Error),
    #[error("version ulid decode error: {0}")]
    UlidDecode(#[from] ulid::DecodeError),
    #[error("version send error: {0}")]
    Send(#[from] SendError<CleanTag>),
    #[error("log error: {0}")]
    Logger(#[from] LogError),
}
