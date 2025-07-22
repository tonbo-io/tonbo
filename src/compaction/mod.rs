pub(crate) mod error;
pub(crate) mod leveled;
use std::{pin::Pin, sync::Arc};

use fusio::DynFs;
use fusio_parquet::writer::AsyncWriter;
use futures_util::StreamExt;
use leveled::LeveledCompactor;
use parquet::arrow::AsyncArrowWriter;
use tokio::sync::oneshot;

use crate::{
    compaction::error::CompactionError,
    fs::{generate_file_id, FileType},
    inmem::immutable::{ArrowArrays, Builder},
    record::{KeyRef, Record, Schema as RecordSchema},
    scope::Scope,
    stream::{merge::MergeStream, ScanStream},
    transaction::CommitError,
    version::{edit::VersionEdit, VersionError},
    DbOption,
};

pub(crate) enum Compactor<R>
where
    R: Record,
{
    Leveled(LeveledCompactor<R>),
}

#[derive(Debug)]
pub enum CompactTask {
    Freeze,
    Flush(Option<oneshot::Sender<()>>),
}

impl<R> Compactor<R>
where
    R: Record,
{
    pub(crate) async fn check_then_compaction(
        &mut self,
        is_manual: bool,
    ) -> Result<(), CompactionError<R>> {
        match self {
            Compactor::Leveled(leveled) => leveled.check_then_compaction(is_manual).await,
        }
    }

    async fn build_tables<'scan>(
        option: &DbOption,
        version_edits: &mut Vec<VersionEdit<<R::Schema as RecordSchema>::Key>>,
        level: usize,
        streams: Vec<ScanStream<'scan, R>>,
        schema: &R::Schema,
        fs: &Arc<dyn DynFs>,
    ) -> Result<(), CompactionError<R>> {
        let mut stream = MergeStream::<R>::from_vec(streams, u32::MAX.into()).await?;

        // Kould: is the capacity parameter necessary?
        let mut builder =
            <R::Schema as RecordSchema>::Columns::builder(schema.arrow_schema().clone(), 8192);
        let mut min = None;
        let mut max = None;

        while let Some(result) = Pin::new(&mut stream).next().await {
            let entry = result?;
            let key = entry.key();

            if min.is_none() {
                min = Some(key.value.clone().to_key())
            }
            max = Some(key.value.clone().to_key());
            builder.push(key, entry.value());

            if builder.written_size() >= option.max_sst_file_size {
                Self::build_table(
                    option,
                    version_edits,
                    level,
                    &mut builder,
                    &mut min,
                    &mut max,
                    schema,
                    fs,
                )
                .await?;
            }
        }
        if builder.written_size() > 0 {
            Self::build_table(
                option,
                version_edits,
                level,
                &mut builder,
                &mut min,
                &mut max,
                schema,
                fs,
            )
            .await?;
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
        version_edits: &mut Vec<VersionEdit<<R::Schema as RecordSchema>::Key>>,
        level: usize,
        builder: &mut <<R::Schema as RecordSchema>::Columns as ArrowArrays>::Builder,
        min: &mut Option<<R::Schema as RecordSchema>::Key>,
        max: &mut Option<<R::Schema as RecordSchema>::Key>,
        schema: &R::Schema,
        fs: &Arc<dyn DynFs>,
    ) -> Result<(), CompactionError<R>> {
        debug_assert!(min.is_some());
        debug_assert!(max.is_some());

        let gen = generate_file_id();
        let columns = builder.finish(None);
        let mut writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(
                fs.open_options(
                    &option.table_path(gen, level),
                    FileType::Parquet.open_options(false),
                )
                .await?,
            ),
            schema.arrow_schema().clone(),
            Some(option.write_parquet_properties.clone()),
        )?;
        writer.write(columns.as_record_batch()).await?;

        let file_size = writer.bytes_written() as u64;
        writer.close().await?;
        version_edits.push(VersionEdit::Add {
            level: level as u8,
            scope: Scope {
                min: min.take().ok_or(CompactionError::EmptyLevel)?,
                max: max.take().ok_or(CompactionError::EmptyLevel)?,
                gen,
                wal_ids: None,
                file_size,
            },
        });
        Ok(())
    }
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
    ) -> Result<Immutable<<R::Schema as Schema>::Columns>, DbError>
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
    ) -> Result<(), DbError>
    where
        R: Record + Send,
    {
        let immutable = build_immutable::<R>(option, records, schema, fs).await?;
        let mut writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(
                fs.open_options(
                    &option.table_path(gen, level),
                    FileType::Parquet.open_options(false),
                )
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
    ) -> ((FileId, FileId, FileId, FileId, FileId), Version<Test>) {
        let level_0_fs = option
            .level_fs_path(0)
            .map(|path| manager.get_fs(path))
            .unwrap_or(manager.base_fs());
        let level_1_fs = option
            .level_fs_path(1)
            .map(|path| manager.get_fs(path))
            .unwrap_or(manager.base_fs());

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
            file_size: 13,
        });
        version.level_slice[0].push(Scope {
            min: 4.to_string(),
            max: 6.to_string(),
            gen: table_gen_2,
            wal_ids: None,
            file_size: 13,
        });
        version.level_slice[1].push(Scope {
            min: 1.to_string(),
            max: 3.to_string(),
            gen: table_gen_3,
            wal_ids: None,
            file_size: 13,
        });
        version.level_slice[1].push(Scope {
            min: 4.to_string(),
            max: 6.to_string(),
            gen: table_gen_4,
            wal_ids: None,
            file_size: 13,
        });
        version.level_slice[1].push(Scope {
            min: 7.to_string(),
            max: 9.to_string(),
            gen: table_gen_5,
            wal_ids: None,
            file_size: 13,
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
