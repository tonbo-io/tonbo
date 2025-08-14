pub mod error;
pub mod leveled;

use std::sync::Arc;

use async_trait::async_trait;
use fusio::{DynFs, MaybeSend, MaybeSync};
use fusio_parquet::writer::AsyncWriter;
use futures::channel::oneshot;
use futures_util::StreamExt;
use parquet::arrow::AsyncArrowWriter;

use crate::{
    compaction::error::CompactionError,
    fs::{generate_file_id, manager::StoreManager, FileId, FileType},
    inmem::immutable::ImmutableMemTable,
    record::{self, ArrowArrays, ArrowArraysBuilder, KeyRef, Record, Schema as RecordSchema},
    scope::Scope,
    stream::{merge::MergeStream, ScanStream},
    version::edit::VersionEdit,
    DbOption,
};

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait Compactor<R>: MaybeSend + MaybeSync
where
    R: Record,
{
    /// Call minor compaction + major compaction.
    /// This is the only method custom compactors must implement.
    async fn check_then_compaction(
        &self,
        batches: Option<
            &[(
                Option<FileId>,
                ImmutableMemTable<<R::Schema as record::Schema>::Columns>,
            )],
        >,
        recover_wal_ids: Option<Vec<FileId>>,
        is_manual: bool,
    ) -> Result<(), CompactionError<R>>;

    /// Perform minor compaction on immutable memtables to create L0 SST files
    /// Basically the same for all compaction strategies. Think carefully if you want to override
    /// this method.
    async fn minor_compaction(
        option: &DbOption,
        recover_wal_ids: Option<Vec<FileId>>,
        batches: &[(
            Option<FileId>,
            ImmutableMemTable<<R::Schema as record::Schema>::Columns>,
        )],
        schema: &R::Schema,
        manager: &StoreManager,
    ) -> Result<Option<Scope<<R::Schema as record::Schema>::Key>>, CompactionError<R>>
    where
        Self: Sized,
        <<R as record::Record>::Schema as record::Schema>::Columns: MaybeSend + MaybeSync,
    {
        use std::ops::Bound;

        use futures_util::stream;
        use parquet::arrow::ProjectionMask;

        if !batches.is_empty() {
            let level_0_path = option.level_fs_path(0).unwrap_or(&option.base_path);
            let level_0_fs = manager.get_fs(level_0_path);
            let mut wal_ids = Vec::with_capacity(batches.len());

            if let Some(mut recover_wal_ids) = recover_wal_ids {
                wal_ids.append(&mut recover_wal_ids);
            }

            // Create scan streams from all immutable memtables
            let mut streams = Vec::with_capacity(batches.len());
            for (file_id, batch) in batches {
                streams.push(ScanStream::Immutable {
                    inner: stream::iter(batch.scan(
                        (Bound::Unbounded, Bound::Unbounded),
                        u32::MAX.into(),
                        ProjectionMask::all(),
                        None,
                    )),
                });
                if let Some(file_id) = file_id {
                    wal_ids.push(*file_id);
                }
            }

            // Use MergeStream to merge and sort all batches
            let mut stream = MergeStream::<R>::from_vec(streams, u32::MAX.into(), None).await?;

            let mut builder =
                <R::Schema as RecordSchema>::Columns::builder(schema.arrow_schema().clone(), 0);
            let mut min = None;
            let mut max = None;

            // Collect all sorted entries into a single SST
            while let Some(result) = stream.next().await {
                let entry = result?;
                let key = entry.key();

                if min.is_none() {
                    min = Some(key.value.clone().to_key())
                }
                max = Some(key.value.clone().to_key());
                builder.push(key, entry.value());
            }

            // Build the single SST file
            if let (Some(min), Some(max)) = (min, max) {
                let mut version_edits = Vec::new();
                Self::build_table(
                    option,
                    &mut version_edits,
                    0,
                    &mut builder,
                    &mut Some(min),
                    &mut Some(max),
                    schema,
                    level_0_fs,
                )
                .await?;

                // Extract the scope from the version edit and add WAL IDs
                if let Some(VersionEdit::Add { scope, .. }) = version_edits.first() {
                    let mut result_scope = scope.clone();
                    result_scope.wal_ids = if wal_ids.is_empty() {
                        None
                    } else {
                        Some(wal_ids)
                    };
                    return Ok(Some(result_scope));
                }
            }
        }
        Ok(None)
    }

    async fn build_tables(
        option: &DbOption,
        version_edits: &mut Vec<VersionEdit<<R::Schema as RecordSchema>::Key>>,
        level: usize,
        streams: Vec<ScanStream<'_, R>>,
        schema: &R::Schema,
        fs: &Arc<dyn DynFs>,
    ) -> Result<(), CompactionError<R>>
    where
        Self: Sized,
        <<R as record::Record>::Schema as record::Schema>::Columns: MaybeSend + MaybeSync,
    {
        let mut stream = MergeStream::<R>::from_vec(streams, u32::MAX.into(), None).await?;

        let mut builder =
            <R::Schema as RecordSchema>::Columns::builder(schema.arrow_schema().clone(), 0);
        let mut min = None;
        let mut max = None;

        while let Some(result) = stream.next().await {
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
    >
    where
        Self: Sized,
    {
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
    ) -> Result<(), CompactionError<R>>
    where
        Self: Sized,
        <<R as record::Record>::Schema as record::Schema>::Columns: MaybeSend + MaybeSync,
    {
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

#[derive(Debug)]
pub enum CompactTask {
    Freeze,
    Flush(Option<oneshot::Sender<()>>),
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
            immutable::{tests::TestSchema, ImmutableMemTable},
            mutable::MutableMemTable,
        },
        record::{Record, Schema},
        scope::Scope,
        tests::Test,
        trigger::TriggerFactory,
        version::{timestamp::Timestamp, Version},
        wal::log::LogType,
        DbError, DbOption,
    };

    async fn build_immutable<R>(
        option: &DbOption,
        records: Vec<(LogType, R, Timestamp)>,
        schema: &Arc<R::Schema>,
        fs: &Arc<dyn DynFs>,
    ) -> Result<ImmutableMemTable<<R::Schema as Schema>::Columns>, DbError>
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
