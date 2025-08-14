pub mod error;
pub mod leveled;
pub mod tiered;

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

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests_metric {

    use crate::{
        executor::tokio::TokioExecutor, inmem::immutable::tests::TestSchema, tests::Test,
        version::MAX_LEVEL, DbOption, DB,
    };

    pub fn convert_test_ref_to_test(
        entry: crate::transaction::TransactionEntry<'_, Test>,
    ) -> Option<Test> {
        match &entry {
            crate::transaction::TransactionEntry::Stream(stream_entry) => {
                if stream_entry.value().is_some() {
                    let test_ref = entry.get();
                    Some(Test {
                        vstring: test_ref.vstring.to_string(),
                        vu32: test_ref.vu32.unwrap_or(0),
                        vbool: test_ref.vbool,
                    })
                } else {
                    None
                }
            }
            crate::transaction::TransactionEntry::Local(_) => {
                let test_ref = entry.get();
                Some(Test {
                    vstring: test_ref.vstring.to_string(),
                    vu32: test_ref.vu32.unwrap_or(0),
                    vbool: test_ref.vbool,
                })
            }
        }
    }

    pub(crate) async fn read_write_amplification_measurement(option: DbOption) {
        let db: DB<Test, TokioExecutor> =
            DB::new(option.clone(), TokioExecutor::default(), TestSchema)
                .await
                .unwrap();

        // Track metrics for amplification calculation
        let mut total_bytes_written_by_user = 0u64;
        let mut compaction_rounds = 0;

        // Insert initial dataset with more substantial data
        let initial_records = 1000;
        let iter_num = 10;
        for i in 0..initial_records * iter_num {
            let record = Test {
                vstring: format!("this_is_a_longer_key_to_make_files_bigger_{:05}", i),
                vu32: i as u32,
                vbool: Some(i % 2 == 0),
            };

            // More accurate user data size calculation
            let string_bytes = record.vstring.as_bytes().len();
            let u32_bytes = 4;
            let bool_bytes = 1;
            let record_size = string_bytes + u32_bytes + bool_bytes;
            total_bytes_written_by_user += record_size as u64;

            db.insert(record).await.unwrap();

            if i % initial_records == 0 {
                // Force flush and compaction
                db.flush().await.unwrap();
                compaction_rounds += 1;
            }
        }

        // Verify data integrity after all compactions (check a sample of keys)
        for i in 0..initial_records * iter_num {
            let key = format!("this_is_a_longer_key_to_make_files_bigger_{:05}", i);
            let result = db.get(&key, convert_test_ref_to_test).await.unwrap();
            if result.is_some() {
                let record = result.unwrap();
                assert_eq!(
                    record.vu32, i as u32,
                    "Value should be preserved after compaction"
                );
            } else {
                panic!("Key {} should exist after compaction", key);
            }
        }

        // Get final version to measure total file sizes
        let final_version = db.ctx.manifest.current().await;
        let mut files_per_level = vec![0; MAX_LEVEL];

        // Verify that total scope.file_size matches total actual file size on disk
        let manager =
            crate::fs::manager::StoreManager::new(option.base_fs.clone(), vec![]).unwrap();
        let fs = manager.base_fs();
        let mut total_actual_file_size = 0u64;

        for level in 0..MAX_LEVEL {
            files_per_level[level] = final_version.level_slice[level].len();
            for scope in &final_version.level_slice[level] {
                let file = fs
                    .open_options(
                        &option.table_path(scope.gen, level),
                        crate::fs::FileType::Parquet.open_options(true),
                    )
                    .await
                    .unwrap();
                let actual_size = file.size().await.unwrap();
                total_actual_file_size += actual_size;
            }
        }

        // Calculate amplification metrics using actual file sizes
        let write_amplification =
            total_actual_file_size as f64 / total_bytes_written_by_user as f64;

        // Read amplification estimation (simplified)
        // In a real scenario, this would require tracking actual read operations
        let estimated_read_amplification = {
            let mut read_amp = 0.0;
            for level in 0..MAX_LEVEL {
                if files_per_level[level] > 0 {
                    // Level 0 files can overlap, so worst case is reading all files
                    if level == 0 {
                        read_amp += files_per_level[level] as f64;
                    } else {
                        // For other levels, typically 1 file per level for a point lookup
                        read_amp += 1.0;
                    }
                }
            }
            read_amp
        };

        println!("=== Amplification Metrics ===");
        println!("User data written: {} bytes", total_bytes_written_by_user);
        println!("Total file size: {} bytes", total_actual_file_size);
        println!("Write Amplification: {:.2}x", write_amplification);
        println!(
            "Estimated Read Amplification: {:.2}x",
            estimated_read_amplification
        );
        println!("Compaction rounds: {}", compaction_rounds);

        for level in 0..MAX_LEVEL {
            if files_per_level[level] > 0 {
                println!("Level {}: {} files", level, files_per_level[level]);
            }
        }

        // Assertions for reasonable amplification
        // Write amplification can be less than 1.0 in some cases due to compression
        // and the way Parquet stores data efficiently. The important thing is that
        // we can measure it and it's non-zero.
        assert!(
            write_amplification > 0.0,
            "Write amplification should be positive"
        );
        assert!(
            write_amplification < 10.0,
            "Write amplification should be reasonable (< 10x)"
        );
        assert!(
            estimated_read_amplification >= 1.0,
            "Read amplification should be at least 1.0"
        );
        assert!(
            total_actual_file_size > 0,
            "Should have written some data to disk"
        );
    }

    pub(crate) async fn throughput(option: DbOption) {
        use std::time::Instant;

        use futures_util::StreamExt;
        use rand::{seq::SliceRandom, SeedableRng};

        // Create DB with EcoTune compactor using the standard open method
        let db: DB<Test, TokioExecutor> =
            DB::new(option.clone(), TokioExecutor::default(), TestSchema)
                .await
                .unwrap();

        // Test parameters based on EcoTune paper (Section 5.1: 35% Get, 35% Seek, 30% long range
        // scans)
        let total_operations = 100000;
        let insert_ratio = 0.3; // 30% inserts to build up data
        let get_ratio = 0.35; // 35% Get operations (point queries)
        let seek_ratio = 0.35; // 35% Seek operations
        let long_range_ratio = 0.30; // 30% long range scans (paper workload)

        let insert_count = (total_operations as f64 * insert_ratio) as usize;
        let query_count = total_operations - insert_count;
        let get_count = (query_count as f64
            * (get_ratio / (get_ratio + seek_ratio + long_range_ratio)))
            as usize;
        let seek_count = (query_count as f64
            * (seek_ratio / (get_ratio + seek_ratio + long_range_ratio)))
            as usize;
        let long_range_count = query_count - get_count - seek_count;

        println!("EcoTune throughput test with paper proportions:");
        println!("- {} inserts ({:.1}%)", insert_count, insert_ratio * 100.0);
        println!(
            "- {} Get queries ({:.1}%)",
            get_count,
            (get_count as f64 / total_operations as f64) * 100.0
        );
        println!(
            "- {} Seek queries ({:.1}%)",
            seek_count,
            (seek_count as f64 / total_operations as f64) * 100.0
        );
        println!(
            "- {} long-range scans ({:.1}%)",
            long_range_count,
            (long_range_count as f64 / total_operations as f64) * 100.0
        );

        // Create mixed workload operations vector

        let mut operations = Vec::new();

        // Add insert operations
        for i in 0..insert_count {
            operations.push(("insert", i));
        }

        // Add get operations
        for i in 0..get_count {
            operations.push(("get", i));
        }

        // Add seek operations
        for i in 0..seek_count {
            operations.push(("seek", i));
        }

        // Add long-range scan operations
        for i in 0..long_range_count {
            operations.push(("long_range", i));
        }

        // Shuffle operations to create mixed workload
        let mut rng = rand::rngs::StdRng::seed_from_u64(42); // Fixed seed for reproducibility
        operations.shuffle(&mut rng);

        // Execute mixed workload
        let mixed_start = Instant::now();
        let mut insert_ops = 0;
        let mut successful_queries = 0;

        for (op_type, index) in operations {
            match op_type {
                "insert" => {
                    let record = Test {
                        vstring: format!("test_key_{:06}", index),
                        vu32: index as u32,
                        vbool: Some(index % 2 == 0),
                    };
                    db.insert(record).await.unwrap();
                    insert_ops += 1;
                }
                "get" => {
                    // Use modulo to ensure key exists (only query from inserted keys)
                    let key = format!("test_key_{:06}", index % insert_ops.max(1));
                    let found = db
                        .get(&key, |entry| match entry {
                            crate::transaction::TransactionEntry::Stream(stream_entry) => {
                                Some(stream_entry.value().is_some())
                            }
                            crate::transaction::TransactionEntry::Local(_) => Some(true),
                        })
                        .await
                        .unwrap();
                    if found.unwrap_or(false) {
                        successful_queries += 1;
                    }
                }
                "seek" => {
                    let key = format!("test_key_{:06}", index % insert_ops.max(1));
                    let scan = db
                        .scan(
                            (std::ops::Bound::Included(&key), std::ops::Bound::Unbounded),
                            |entry| match entry {
                                crate::transaction::TransactionEntry::Stream(_) => true,
                                crate::transaction::TransactionEntry::Local(_) => true,
                            },
                        )
                        .await
                        .take(1);
                    let mut scan = std::pin::pin!(scan);

                    if let Some(result) = scan.next().await {
                        if result.is_ok() {
                            successful_queries += 1;
                        }
                    }
                }
                "long_range" => {
                    let start_key = format!("test_key_{:06}", index % insert_ops.max(1));
                    let scan = db
                        .scan(
                            (
                                std::ops::Bound::Included(&start_key),
                                std::ops::Bound::Unbounded,
                            ),
                            |entry| match entry {
                                crate::transaction::TransactionEntry::Stream(_) => true,
                                crate::transaction::TransactionEntry::Local(_) => true,
                            },
                        )
                        .await
                        .take(100);
                    let mut scan = std::pin::pin!(scan);

                    let mut count = 0;
                    while let Some(result) = scan.next().await {
                        if result.is_ok() {
                            count += 1;
                            if count >= 100 {
                                break;
                            } // Limit to K=100
                        }
                    }
                    if count > 0 {
                        successful_queries += 1;
                    }
                }
                _ => unreachable!(),
            }
        }

        let mixed_duration = mixed_start.elapsed();
        let mixed_throughput = total_operations as f64 / mixed_duration.as_secs_f64();

        // Calculate mixed workload results
        println!("Mixed Workload Throughput Results:");
        println!("Overall throughput: {:.2} ops/sec", mixed_throughput);
        println!(
            "Total operations: {} (inserts: {}, successful queries: {})",
            total_operations, insert_ops, successful_queries
        );
        println!("Total time: {:.3}s", mixed_duration.as_secs_f64());
    }
}
