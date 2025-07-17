use std::{
    collections::{Bound, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common::{Keys, Value};
use fusio::{
    dynamic::{DynFile, MaybeSendFuture},
    path::Path,
    DynFs, Error,
};
use futures_core::Stream;
use parquet::{arrow::ProjectionMask, errors::ParquetError};
use parquet_lru::DynLruCache;
use ulid::Ulid;

use crate::{
    fs::{FileId, FileType},
    ondisk::{scan::SsTableScan, sstable::SsTable},
    record::Record,
    scope::Scope,
    stream::record_batch::RecordBatchEntry,
    timestamp::Timestamp,
    version::Version,
    DbOption,
};

enum FutureStatus<'level, R>
where
    R: Record,
{
    Init(FileId),
    Ready(SsTableScan<'level, R>),
    OpenFile(
        Ulid,
        Pin<Box<dyn MaybeSendFuture<Output = Result<Box<dyn DynFile>, Error>> + 'level>>,
    ),
    OpenSst(Pin<Box<dyn MaybeSendFuture<Output = Result<SsTable<R>, Error>> + 'level>>),
    LoadStream(
        Pin<Box<dyn Future<Output = Result<SsTableScan<'level, R>, ParquetError>> + Send + 'level>>,
    ),
}

pub(crate) struct LevelStream<'level, R>
where
    R: Record,
{
    lower: Bound<&'level Keys>,
    upper: Bound<&'level Keys>,
    ts: Timestamp,
    level: usize,
    option: Arc<DbOption>,
    gens: VecDeque<FileId>,
    limit: Option<usize>,
    projection_mask: ProjectionMask,
    status: FutureStatus<'level, R>,
    fs: Arc<dyn DynFs>,
    path: Option<Path>,
    parquet_lru: Arc<dyn DynLruCache<Ulid> + Send + Sync>,
}

impl<'level, R> LevelStream<'level, R>
where
    R: Record,
{
    // Kould: only used by Compaction now, and the start and end of the sstables range are known
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        version: &Version<R>,
        level: usize,
        start: usize,
        end: usize,
        range: (Bound<&'level Keys>, Bound<&'level Keys>),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
        fs: Arc<dyn DynFs>,
        parquet_lru: Arc<dyn DynLruCache<Ulid> + Send + Sync>,
    ) -> Option<Self> {
        let (lower, upper) = range;
        let mut gens: VecDeque<FileId> = version.level_slice[level][start..end + 1]
            .iter()
            .map(Scope::gen)
            .collect();
        let first_gen = gens.pop_front()?;
        let status = FutureStatus::Init(first_gen);

        Some(LevelStream {
            lower,
            upper,
            ts,
            level,
            option: version.option().clone(),
            gens,
            limit,
            projection_mask,
            status,
            fs,
            path: None,
            parquet_lru,
        })
    }
}

impl<'level, R> Stream for LevelStream<'level, R>
where
    R: Record,
{
    type Item = Result<RecordBatchEntry<R>, ParquetError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return match &mut self.status {
                FutureStatus::Init(gen) => {
                    let gen = *gen;
                    self.path = Some(self.option.table_path(gen, self.level));

                    let reader = self.fs.open_options(
                        self.path.as_ref().unwrap(),
                        FileType::Parquet.open_options(true),
                    );
                    #[allow(clippy::missing_transmute_annotations)]
                    let reader = unsafe {
                        std::mem::transmute::<
                            _,
                            Pin<
                                Box<
                                    dyn MaybeSendFuture<Output = Result<Box<dyn DynFile>, Error>>
                                        + 'static,
                                >,
                            >,
                        >(reader)
                    };
                    self.status = FutureStatus::OpenFile(gen, reader);
                    continue;
                }
                FutureStatus::Ready(stream) => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(None) => match self.gens.pop_front() {
                        None => Poll::Ready(None),
                        Some(gen) => {
                            self.path = Some(self.option.table_path(gen, self.level));

                            let reader = self.fs.open_options(
                                self.path.as_ref().unwrap(),
                                FileType::Parquet.open_options(true),
                            );
                            #[allow(clippy::missing_transmute_annotations)]
                            let reader = unsafe {
                                std::mem::transmute::<
                                    _,
                                    Pin<
                                        Box<
                                            dyn MaybeSendFuture<
                                                    Output = Result<Box<dyn DynFile>, Error>,
                                                > + 'static,
                                        >,
                                    >,
                                >(reader)
                            };
                            self.status = FutureStatus::OpenFile(gen, reader);
                            continue;
                        }
                    },
                    Poll::Ready(Some(result)) => {
                        if let Some(limit) = &mut self.limit {
                            *limit -= 1;
                        }
                        Poll::Ready(Some(result))
                    }
                    Poll::Pending => Poll::Pending,
                },
                FutureStatus::OpenFile(id, file_future) => match Pin::new(file_future).poll(cx) {
                    Poll::Ready(Ok(file)) => {
                        let id = *id;
                        self.status = FutureStatus::OpenSst(Box::pin(SsTable::open(
                            self.parquet_lru.clone(),
                            id,
                            file,
                        )));
                        continue;
                    }
                    Poll::Ready(Err(err)) => {
                        Poll::Ready(Some(Err(ParquetError::External(Box::new(err)))))
                    }
                    Poll::Pending => Poll::Pending,
                },
                FutureStatus::OpenSst(sst_future) => match Pin::new(sst_future).poll(cx) {
                    Poll::Ready(Ok(sst)) => {
                        self.status = FutureStatus::LoadStream(Box::pin(sst.scan(
                            (self.lower, self.upper),
                            self.ts,
                            self.limit,
                            self.projection_mask.clone(),
                        )));
                        continue;
                    }
                    Poll::Ready(Err(err)) => {
                        Poll::Ready(Some(Err(ParquetError::External(Box::new(err)))))
                    }
                    Poll::Pending => Poll::Pending,
                },
                FutureStatus::LoadStream(stream_future) => match Pin::new(stream_future).poll(cx) {
                    Poll::Ready(Ok(scan)) => {
                        self.status = FutureStatus::Ready(scan);
                        continue;
                    }
                    Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                    Poll::Pending => Poll::Pending,
                },
            };
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{collections::Bound, sync::Arc};

    use fusio::path::Path;
    use fusio_dispatch::FsOptions;
    use futures_util::StreamExt;
    use parquet::arrow::{ArrowSchemaConverter, ProjectionMask};
    use parquet_lru::NoCache;
    use tempfile::TempDir;

    use crate::{
        compaction::tests::build_version, fs::manager::StoreManager, stream::level::LevelStream,
        tests::Test, DbOption,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn projection_scan() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StoreManager::new(FsOptions::Local, vec![]).unwrap();
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let schema = Arc::new(Test::schema());
        let (_, version) = build_version(&option, &manager, &schema).await;

        {
            let mut level_stream_1 = LevelStream::new(
                &version,
                0,
                0,
                1,
                (Bound::Unbounded, Bound::Unbounded),
                1_u32.into(),
                None,
                ProjectionMask::roots(
                    &ArrowSchemaConverter::new()
                        .convert(schema.arrow_schema())
                        .unwrap(),
                    [0, 1, 2, 3],
                ),
                manager.base_fs().clone(),
                Arc::new(NoCache::default()),
            )
            .unwrap();

            let entry_0 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_0.get().unwrap().vu32.is_some());
            assert!(entry_0.get().unwrap().vbool.is_none());
            let entry_1 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_1.get().unwrap().vu32.is_some());
            assert!(entry_1.get().unwrap().vbool.is_none());
            let entry_2 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_2.get().unwrap().vu32.is_some());
            assert!(entry_2.get().unwrap().vbool.is_none());
            let entry_3 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_3.get().unwrap().vu32.is_some());
            assert!(entry_3.get().unwrap().vbool.is_none());
            let entry_4 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_4.get().unwrap().vu32.is_some());
            assert!(entry_4.get().unwrap().vbool.is_none());
            let entry_5 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_5.get().unwrap().vu32.is_some());
            assert!(entry_5.get().unwrap().vbool.is_none());
        }
        {
            let mut level_stream_1 = LevelStream::new(
                &version,
                0,
                0,
                1,
                (Bound::Unbounded, Bound::Unbounded),
                1_u32.into(),
                None,
                ProjectionMask::roots(
                    &ArrowSchemaConverter::new()
                        .convert(schema.arrow_schema())
                        .unwrap(),
                    [0, 1, 2, 4],
                ),
                manager.base_fs().clone(),
                Arc::new(NoCache::default()),
            )
            .unwrap();

            let entry_0 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_0.get().unwrap().vu32.is_none());
            assert!(entry_0.get().unwrap().vbool.is_some());
            let entry_1 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_1.get().unwrap().vu32.is_none());
            assert!(entry_1.get().unwrap().vbool.is_some());
            let entry_2 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_2.get().unwrap().vu32.is_none());
            assert!(entry_2.get().unwrap().vbool.is_some());
            let entry_3 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_3.get().unwrap().vu32.is_none());
            assert!(entry_3.get().unwrap().vbool.is_some());
            let entry_4 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_4.get().unwrap().vu32.is_none());
            assert!(entry_4.get().unwrap().vbool.is_some());
            let entry_5 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_5.get().unwrap().vu32.is_none());
            assert!(entry_5.get().unwrap().vbool.is_some());
        }
        {
            let mut level_stream_1 = LevelStream::new(
                &version,
                0,
                0,
                1,
                (Bound::Unbounded, Bound::Unbounded),
                1_u32.into(),
                None,
                ProjectionMask::roots(
                    &ArrowSchemaConverter::new()
                        .convert(schema.arrow_schema())
                        .unwrap(),
                    [0, 1, 2],
                ),
                manager.base_fs().clone(),
                Arc::new(NoCache::default()),
            )
            .unwrap();

            let entry_0 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_0.get().unwrap().vu32.is_none());
            assert!(entry_0.get().unwrap().vbool.is_none());
            let entry_1 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_1.get().unwrap().vu32.is_none());
            assert!(entry_1.get().unwrap().vbool.is_none());
            let entry_2 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_2.get().unwrap().vu32.is_none());
            assert!(entry_2.get().unwrap().vbool.is_none());
            let entry_3 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_3.get().unwrap().vu32.is_none());
            assert!(entry_3.get().unwrap().vbool.is_none());
            let entry_4 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_4.get().unwrap().vu32.is_none());
            assert!(entry_4.get().unwrap().vbool.is_none());
            let entry_5 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_5.get().unwrap().vu32.is_none());
            assert!(entry_5.get().unwrap().vbool.is_none());
        }
    }
}
