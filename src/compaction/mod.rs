pub(crate) mod leveled;
use std::{pin::Pin, sync::Arc};

use fusio::DynFs;
use fusio_parquet::writer::AsyncWriter;
use futures_util::StreamExt;
use leveled::LeveledCompactor;
use log::error;
use parquet::arrow::AsyncArrowWriter;
use thiserror::Error;
use tokio::sync::oneshot;
use ulid::Ulid;

use crate::{
    executor::Executor,
    fs::{generate_file_id, FileType},
    inmem::immutable::{ArrowArrays, Builder},
    record::{KeyRef, Record, Schema as RecordSchema},
    scope::Scope,
    stream::{merge::MergeStream, ScanStream},
    transaction::CommitError,
    version::{edit::VersionEdit, VersionError},
    DbOption,
};

pub(crate) enum Compactor<R, E>
where
    R: Record,
    E: Executor + Send + Sync + 'static,
{
    Leveled(LeveledCompactor<R, E>),
}

#[derive(Debug)]
pub enum CompactTask {
    Freeze,
    Flush(Option<oneshot::Sender<()>>),
}

impl<R, E> Compactor<R, E>
where
    R: Record + Send + Sync,
    E: Executor + Send + Sync + 'static,
{
    pub(crate) async fn check_then_compaction(
        &mut self,
        is_manual: bool,
        executor: Arc<E>,
    ) -> Result<(), CompactionError<R>> {
        match self {
            Compactor::Leveled(leveled) => leveled.check_then_compaction(is_manual, executor).await,
        }
    }

    async fn build_tables<'scan>(
        option: &Arc<DbOption>,
        version_edits: &mut Vec<VersionEdit<<R::Schema as RecordSchema>::Key>>,
        level: usize,
        streams: Vec<ScanStream<'scan, R>>,
        schema: &Arc<R::Schema>,
        fs: &Arc<dyn DynFs>,
        cache: &Arc<dyn DynFs>,
        executor: &Arc<E>,
        cached_to_local: bool,
    ) -> Result<(), CompactionError<R>> {
        let mut stream = MergeStream::<R>::from_vec(streams, u32::MAX.into()).await?;

        // Kould: is the capacity parameter necessary?
        let mut builder =
            <R::Schema as RecordSchema>::Columns::builder(schema.arrow_schema().clone(), 8192);
        let mut min = None;
        let mut max = None;
        let (manifest_tx, manifest_cx) = flume::unbounded();
        while let Some(result) = Pin::new(&mut stream).next().await {
            let entry = result?;
            let key = entry.key();

            if min.is_none() {
                min = Some(key.value.clone().to_key())
            }
            max = Some(key.value.clone().to_key());
            builder.push(key, entry.value());

            if builder.written_size() >= option.max_sst_file_size {
                let columns = Arc::new(builder.finish(None));
                let gen = generate_file_id();
                if cached_to_local {
                    let option = option.clone();
                    let schema = schema.clone();
                    let mut min = min.clone();
                    let mut max = max.clone();
                    let cache = cache.clone();
                    let columns = columns.clone();
                    let manifest_tx = manifest_tx.clone();
                    executor.spawn(async move {
                        if let Err(e) = Self::build_table(
                            &option,
                            gen,
                            level,
                            &columns,
                            &mut min,
                            &mut max,
                            &schema,
                            &cache,
                            &manifest_tx,
                            false,
                            cached_to_local,
                        )
                        .await
                        {
                            error!("Build Table Error: {}", e);
                        }
                    });
                }
                Self::build_table(
                    &option,
                    gen,
                    level,
                    &columns,
                    &mut min,
                    &mut max,
                    &schema,
                    &fs,
                    &manifest_tx,
                    true,
                    !cached_to_local,
                )
                .await?;
            }
        }
        if builder.written_size() > 0 {
            let columns = Arc::new(builder.finish(None));
            let gen = generate_file_id();
            if cached_to_local {
                let option = option.clone();
                let schema = schema.clone();
                let mut min = min.clone();
                let mut max = max.clone();
                let cache = cache.clone();
                let columns = columns.clone();
                let manifest_tx = manifest_tx.clone();
                executor.spawn(async move {
                    if let Err(e) = Self::build_table(
                        &option,
                        gen,
                        level,
                        &columns,
                        &mut min,
                        &mut max,
                        &schema,
                        &cache,
                        &manifest_tx,
                        false,
                        cached_to_local,
                    )
                    .await
                    {
                        error!("Build Table Error: {}", e);
                    }
                });
            }
            Self::build_table(
                &option,
                gen,
                level,
                &columns,
                &mut min,
                &mut max,
                &schema,
                &fs,
                &manifest_tx,
                true,
                !cached_to_local,
            )
            .await?;
        }
        drop(manifest_tx);
        while let Ok(version_edit) = manifest_cx.recv() {
            version_edits.push(version_edit);
        }
        Ok(())
    }

    fn full_scope<'a>(
        meet_scopes: &[&'a Scope<<R::Schema as RecordSchema>::Key>],
    ) -> Result<
        (
            &'a <R::Schema as RecordSchema>::Key,
            &'a <R::Schema as RecordSchema>::Key,
        ),
        CompactionError<R>,
    > {
        let lower = &meet_scopes.first().ok_or(CompactionError::EmptyLevel)?.min;
        let upper = &meet_scopes.last().ok_or(CompactionError::EmptyLevel)?.max;
        Ok((lower, upper))
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_table(
        option: &DbOption,
        gen: Ulid,
        level: usize,
        columns: &Arc<<<R as Record>::Schema as RecordSchema>::Columns>,
        min: &mut Option<<R::Schema as RecordSchema>::Key>,
        max: &mut Option<<R::Schema as RecordSchema>::Key>,
        schema: &Arc<R::Schema>,
        fs: &Arc<dyn DynFs>,
        manifest_tx: &flume::Sender<VersionEdit<<R::Schema as RecordSchema>::Key>>,
        upload_to_s3: bool,
        send_manifest: bool,
    ) -> Result<(), CompactionError<R>> {
        debug_assert!(min.is_some());
        debug_assert!(max.is_some());

        let path = if upload_to_s3 {
            &option.table_path(gen, level)
        } else {
            &option.cached_table_path(gen)
        };
        let mut writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(
                fs.open_options(path, FileType::Parquet.open_options(false))
                    .await?,
            ),
            schema.arrow_schema().clone(),
            Some(option.write_parquet_properties.clone()),
        )?;
        writer.write(columns.as_record_batch()).await?;
        writer.close().await?;

        if send_manifest {
            manifest_tx
                .send(VersionEdit::Add {
                    level: level as u8,
                    scope: Scope {
                        min: min.take().ok_or(CompactionError::EmptyLevel)?,
                        max: max.take().ok_or(CompactionError::EmptyLevel)?,
                        gen,
                        wal_ids: None,
                    },
                })
                .map_err(|_| CompactionError::ChannelClose)?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CompactionError<R>
where
    R: Record,
{
    #[error("compaction io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("compaction parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("compaction fusio error: {0}")]
    Fusio(#[from] fusio::Error),
    #[error("compaction version error: {0}")]
    Version(#[from] VersionError<R>),
    #[error("compaction logger error: {0}")]
    Logger(#[from] fusio_log::error::LogError),
    #[error("compaction channel is closed")]
    ChannelClose,
    #[error("database error: {0}")]
    Commit(#[from] CommitError<R>),
    #[error("the level being compacted does not have a table")]
    EmptyLevel,
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests {
    use std::sync::{atomic::AtomicU32, Arc};

    use flume::bounded;
    use fusio::DynFs;
    use fusio_parquet::writer::AsyncWriter;
    use parquet::arrow::AsyncArrowWriter;

    use crate::{
        fs::{generate_file_id, manager::StoreManager, FileId, FileType},
        inmem::{
            immutable::{tests::TestSchema, Immutable},
            mutable::MutableMemTable,
        },
        record::{Record, Schema},
        scope::Scope,
        tests::Test,
        timestamp::Timestamp,
        trigger::TriggerFactory,
        version::Version,
        wal::log::LogType,
        DbError, DbOption,
    };

    async fn build_immutable<R>(
        option: &DbOption,
        records: Vec<(LogType, R, Timestamp)>,
        schema: &Arc<R::Schema>,
        fs: &Arc<dyn DynFs>,
    ) -> Result<Immutable<<R::Schema as Schema>::Columns>, DbError<R>>
    where
        R: Record + Send,
    {
        let trigger = TriggerFactory::create(option.trigger_type);

        let mutable: MutableMemTable<R> =
            MutableMemTable::new(option, trigger, fs.clone(), schema.clone()).await?;

        for (log_ty, record, ts) in records {
            let _ = mutable.insert(log_ty, record, ts).await?;
        }
        Ok(mutable.into_immutable().await.unwrap().1)
    }

    pub(crate) async fn build_parquet_table<R>(
        option: &DbOption,
        gen: FileId,
        records: Vec<(LogType, R, Timestamp)>,
        schema: &Arc<R::Schema>,
        level: usize,
        fs: &Arc<dyn DynFs>,
        build_cache: bool,
    ) -> Result<(), DbError<R>>
    where
        R: Record + Send,
    {
        let immutable = build_immutable::<R>(option, records, schema, fs).await?;
        let path = if build_cache {
            &option.cached_table_path(gen)
        } else {
            &option.table_path(gen, level)
        };
        let mut writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(
                fs.open_options(path, FileType::Parquet.open_options(false))
                    .await?,
            ),
            schema.arrow_schema().clone(),
            None,
        )?;
        writer.write(immutable.as_record_batch()).await?;
        writer.close().await?;

        Ok(())
    }

    pub(crate) async fn build_version(
        option: &Arc<DbOption>,
        manager: &StoreManager,
        schema: &Arc<TestSchema>,
        test_cache: bool,
    ) -> ((FileId, FileId, FileId, FileId, FileId), Version<Test>) {
        let level_0_cache = option.level_fs_cached(0);
        let level_0_fs = if test_cache && level_0_cache {
            option
                .level_fs_path(0)
                .map(|path| manager.get_cache(path))
                .unwrap_or(manager.base_fs())
        } else {
            option
                .level_fs_path(0)
                .map(|path| manager.get_fs(path))
                .unwrap_or(manager.base_fs())
        };

        let level_1_cache = option.level_fs_cached(1);
        let level_1_fs = if test_cache && level_1_cache {
            option
                .level_fs_path(1)
                .map(|path| manager.get_cache(path))
                .unwrap_or(manager.base_fs())
        } else {
            option
                .level_fs_path(1)
                .map(|path| manager.get_fs(path))
                .unwrap_or(manager.base_fs())
        };

        // level 0
        let table_gen_1 = generate_file_id();
        let table_gen_2 = generate_file_id();
        build_parquet_table::<Test>(
            option,
            table_gen_1,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: 1.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    1.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 2.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    1.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 3.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
            ],
            schema,
            0,
            level_0_fs,
            test_cache,
        )
        .await
        .unwrap();
        build_parquet_table::<Test>(
            option,
            table_gen_2,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: 4.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    1.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 5.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    1.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 6.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
            ],
            schema,
            0,
            level_0_fs,
            test_cache,
        )
        .await
        .unwrap();

        // level 1
        let table_gen_3 = generate_file_id();
        let table_gen_4 = generate_file_id();
        let table_gen_5 = generate_file_id();
        build_parquet_table::<Test>(
            option,
            table_gen_3,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: 1.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 2.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 3.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
            ],
            schema,
            1,
            level_1_fs,
            test_cache,
        )
        .await
        .unwrap();
        build_parquet_table::<Test>(
            option,
            table_gen_4,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: 4.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 5.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 6.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
            ],
            schema,
            1,
            level_1_fs,
            test_cache,
        )
        .await
        .unwrap();
        build_parquet_table::<Test>(
            option,
            table_gen_5,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: 7.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 8.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 9.to_string(),
                        vu32: 0,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
            ],
            schema,
            1,
            level_1_fs,
            test_cache,
        )
        .await
        .unwrap();

        let (sender, _) = bounded(1);
        let mut version =
            Version::<Test>::new(option.clone(), sender, Arc::new(AtomicU32::default()));
        version.level_slice[0].push(Scope {
            min: 1.to_string(),
            max: 3.to_string(),
            gen: table_gen_1,
            wal_ids: None,
        });
        version.level_slice[0].push(Scope {
            min: 4.to_string(),
            max: 6.to_string(),
            gen: table_gen_2,
            wal_ids: None,
        });
        version.level_slice[1].push(Scope {
            min: 1.to_string(),
            max: 3.to_string(),
            gen: table_gen_3,
            wal_ids: None,
        });
        version.level_slice[1].push(Scope {
            min: 4.to_string(),
            max: 6.to_string(),
            gen: table_gen_4,
            wal_ids: None,
        });
        version.level_slice[1].push(Scope {
            min: 7.to_string(),
            max: 9.to_string(),
            gen: table_gen_5,
            wal_ids: None,
        });
        (
            (
                table_gen_1,
                table_gen_2,
                table_gen_3,
                table_gen_4,
                table_gen_5,
            ),
            version,
        )
    }
}
