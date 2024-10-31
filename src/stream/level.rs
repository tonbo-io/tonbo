use std::{
    collections::{Bound, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use fusio::{
    dynamic::{DynFile, MaybeSendFuture},
    path::Path,
    DynFs, Error,
};
use futures_core::Stream;
use parquet::{arrow::ProjectionMask, errors::ParquetError};
use pin_project_lite::pin_project;
use tonbo_ext_reader::CacheReader;

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

enum FutureStatus<'level, R, C>
where
    R: Record,
    C: CacheReader,
{
    Init(FileId),
    Ready(SsTableScan<'level, R, C>),
    OpenFile(Pin<Box<dyn MaybeSendFuture<Output = Result<Box<dyn DynFile>, Error>> + 'level>>),
    OpenSst(Pin<Box<dyn Future<Output = Result<SsTable<R, C>, Error>> + Send + 'level>>),
    LoadStream(
        Pin<
            Box<
                dyn Future<Output = Result<SsTableScan<'level, R, C>, ParquetError>>
                    + Send
                    + 'level,
            >,
        >,
    ),
}

pin_project! {
    pub(crate) struct LevelStream<'level, R, C>
    where
        R: Record,
        C: CacheReader,
    {
        lower: Bound<&'level R::Key>,
        upper: Bound<&'level R::Key>,
        ts: Timestamp,
        level: usize,
        option: Arc<DbOption<R>>,
        meta_cache: C::MetaCache,
        range_cache: C::RangeCache,
        gens: VecDeque<FileId>,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
        status: FutureStatus<'level, R, C>,
        fs: Arc<dyn DynFs>,
        path: Option<(Path, FileId)>,
    }
}

impl<'level, R, C> LevelStream<'level, R, C>
where
    R: Record,
    C: CacheReader,
{
    // Kould: only used by Compaction now, and the start and end of the sstables range are known
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        version: &Version<R, C>,
        level: usize,
        start: usize,
        end: usize,
        range: (Bound<&'level R::Key>, Bound<&'level R::Key>),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
        fs: Arc<dyn DynFs>,
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
            meta_cache: version.meta_cache(),
            range_cache: version.range_cache(),
            gens,
            limit,
            projection_mask,
            status,
            fs,
            path: None,
        })
    }
}

impl<'level, R, C> Stream for LevelStream<'level, R, C>
where
    R: Record,
    C: CacheReader + 'static,
{
    type Item = Result<RecordBatchEntry<R>, ParquetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            return match &mut this.status {
                FutureStatus::Init(gen) => {
                    let gen = *gen;
                    *this.path = Some((this.option.table_path(&gen, *this.level), gen));

                    let reader = this.fs.open_options(
                        &this.path.as_ref().unwrap().0,
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
                    *this.status = FutureStatus::OpenFile(reader);
                    continue;
                }
                FutureStatus::Ready(stream) => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(None) => match this.gens.pop_front() {
                        None => Poll::Ready(None),
                        Some(gen) => {
                            *this.path = Some((this.option.table_path(&gen, *this.level), gen));

                            let reader = this.fs.open_options(
                                &this.path.as_ref().unwrap().0,
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
                            *this.status = FutureStatus::OpenFile(reader);
                            continue;
                        }
                    },
                    Poll::Ready(Some(result)) => {
                        if let Some(limit) = &mut this.limit {
                            *limit -= 1;
                        }
                        Poll::Ready(Some(result))
                    }
                    Poll::Pending => Poll::Pending,
                },
                FutureStatus::OpenFile(file_future) => match Pin::new(file_future).poll(cx) {
                    Poll::Ready(Ok(file)) => {
                        let meta_cache = this.meta_cache.clone();
                        let range_cache = this.range_cache.clone();
                        let (_, gen) = this.path.clone().unwrap();
                        let future =
                            async move { SsTable::open(file, gen, range_cache, meta_cache).await };
                        *this.status = FutureStatus::OpenSst(Box::pin(future));
                        continue;
                    }
                    Poll::Ready(Err(err)) => {
                        Poll::Ready(Some(Err(ParquetError::External(Box::new(err)))))
                    }
                    Poll::Pending => Poll::Pending,
                },
                FutureStatus::OpenSst(sst_future) => match Pin::new(sst_future).poll(cx) {
                    Poll::Ready(Ok(sst)) => {
                        *this.status = FutureStatus::LoadStream(Box::pin(sst.scan(
                            (*this.lower, *this.upper),
                            *this.ts,
                            *this.limit,
                            this.projection_mask.clone(),
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
                        *this.status = FutureStatus::Ready(scan);
                        continue;
                    }
                    Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                    Poll::Pending => Poll::Pending,
                },
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::Bound, sync::Arc};

    use fusio::path::Path;
    use fusio_dispatch::FsOptions;
    use futures_util::StreamExt;
    use parquet::arrow::{arrow_to_parquet_schema, ProjectionMask};
    use tempfile::TempDir;

    use crate::{
        compaction::tests::build_version, fs::manager::StoreManager, record::Record,
        stream::level::LevelStream, tests::Test, DbOption,
    };

    #[tokio::test]
    async fn projection_scan() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StoreManager::new(FsOptions::Local, vec![]).unwrap();
        let option = Arc::new(DbOption::from(
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

        let (_, version) = build_version(&option, &manager).await;

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
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2, 3],
                ),
                manager.base_fs().clone(),
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
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2, 4],
                ),
                manager.base_fs().clone(),
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
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2],
                ),
                manager.base_fs().clone(),
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
