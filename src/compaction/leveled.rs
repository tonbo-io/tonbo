use std::cmp;
use std::mem;
use std::ops::Bound;
use std::sync::Arc;

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use fusio_parquet::writer::AsyncWriter;
use parquet::arrow::{AsyncArrowWriter, ProjectionMask};
use ulid::Ulid;

use super::{CompactionError, Compactor};
use crate::fs::manager::StoreManager;
use crate::fs::{generate_file_id, FileId, FileType};
use crate::inmem::immutable::ImmutableMemTable;
use crate::inmem::mutable::MutableMemTable;
use crate::ondisk::sstable::{SsTable, SsTableID};
use crate::scope::Scope;
use crate::stream::level::LevelStream;
use crate::stream::ScanStream;
use crate::version::edit::VersionEdit;
use crate::version::TransactionTs;
use crate::{
    context::Context,
    record::{self, Record, Schema as RecordSchema},
    version::{Version, MAX_LEVEL},
    DbOption, DbStorage,
};

pub struct LeveledTask {
    pub input: Vec<(usize, Vec<Ulid>)>,
}

pub struct LeveledCompactor<R: Record> {
    option: Arc<DbOption>,
    mem_storage: Arc<RwLock<DbStorage<R>>>,
    ctx: Arc<Context<R>>,
    record_schema: Arc<R::Schema>,
}

impl<R> Compactor<R> for LeveledCompactor<R>
where
    R: Record,
    <<R as record::Record>::Schema as record::Schema>::Columns: Send + Sync,
{
    type Task = LeveledTask;

    async fn check_then_compaction(&self, is_manual: bool) -> Result<(), CompactionError<R>> {
        let mut guard = self.mem_storage.write().await;

        guard.trigger.reset();

        // Add the mutable memtable into the immutable memtable
        if !guard.mutable.is_empty() {
            let trigger_clone = guard.trigger.clone();

            // Replace mutable memtable with new memtable
            let mutable = mem::replace(
                &mut guard.mutable,
                MutableMemTable::new(
                    &self.option,
                    trigger_clone,
                    self.ctx.manager.base_fs().clone(),
                    self.record_schema.clone(),
                )
                .await?,
            );
            let (file_id, immutable) = mutable.into_immutable().await?;
            guard.immutables.push((file_id, immutable));
        } else if !is_manual {
            return Ok(());
        }

        if (is_manual && !guard.immutables.is_empty())
            || guard.immutables.len() > self.option.immutable_chunk_max_num
        {
            let recover_wal_ids = guard.recover_wal_ids.take();
            drop(guard);

            let guard = self.mem_storage.upgradable_read().await;
            let chunk_num = if is_manual {
                guard.immutables.len()
            } else {
                self.option.immutable_chunk_num
            };
            let excess = &guard.immutables[0..chunk_num];

            if let Some(scope) = Self::minor_compaction(
                &self.option,
                recover_wal_ids,
                excess,
                &guard.record_schema,
                &self.ctx.manager,
            )
            .await?
            {
                let version_ref = self.ctx.manifest.current().await;
                let mut version_edits = vec![];
                let mut delete_gens = vec![];

                if Self::is_threshold_exceeded_major(&self.option, &version_ref, 0) || is_manual {
                    Self::major_compaction(
                        &version_ref,
                        &self.option,
                        &scope.min,
                        &scope.max,
                        &mut version_edits,
                        &mut delete_gens,
                        &guard.record_schema,
                        &self.ctx,
                        is_manual,
                    )
                    .await?;
                }
                version_edits.insert(0, VersionEdit::Add { level: 0, scope });
                version_edits.push(VersionEdit::LatestTimeStamp {
                    ts: version_ref.increase_ts(),
                });

                self.ctx
                    .manifest
                    .update(version_edits, Some(delete_gens))
                    .await?;
            }
            let mut guard = RwLockUpgradableReadGuard::upgrade(guard).await;
            let sources = guard.immutables.split_off(chunk_num);
            let _ = mem::replace(&mut guard.immutables, sources);
        }
        if is_manual {
            self.ctx.manifest.rewrite().await.unwrap();
        }
        Ok(())
    }
}

impl<R> LeveledCompactor<R>
where
    R: Record,
    <<R as record::Record>::Schema as record::Schema>::Columns: Send + Sync,
{
    pub(crate) fn new(
        mem_storage: Arc<RwLock<DbStorage<R>>>,
        record_schema: Arc<R::Schema>,
        option: Arc<DbOption>,
        ctx: Arc<Context<R>>,
    ) -> Self {
        LeveledCompactor::<R> {
            option,
            mem_storage,
            ctx,
            record_schema,
        }
    }


    /// Major compaction logic that handles both manual and automatic cases
    #[allow(clippy::too_many_arguments)]
    async fn major_compaction(
        version: &Version<R>,
        option: &DbOption,
        mut min: &<R::Schema as RecordSchema>::Key,
        mut max: &<R::Schema as RecordSchema>::Key,
        version_edits: &mut Vec<VersionEdit<<R::Schema as RecordSchema>::Key>>,
        delete_gens: &mut Vec<SsTableID>,
        instance: &R::Schema,
        ctx: &Context<R>,
        is_manual: bool,
    ) -> Result<(), CompactionError<R>> {
        let mut level = 0;

        while level < MAX_LEVEL - 2 {
            let threshold_exceeded = Self::is_threshold_exceeded_major(option, version, level);

            // CONDITION 1: Stop if threshold not exceeded AND not manual, OR level is empty
            if (!threshold_exceeded && !is_manual) || version.level_slice[level].is_empty() {
                break;
            }

            // CONDITION 2: Self compaction case - only when threshold NOT exceeded AND manual AND next level empty
            if !threshold_exceeded && is_manual && version.level_slice[level + 1].is_empty() {
                // Perform self compaction for level 0 if `is_manual`
                if level == 0 {
                    let (meet_scopes_l, _, _) =
                        Self::this_level_scopes(version, min, max, level, threshold_exceeded);

                    // For self compaction if there is only one SST that falls under the range we
                    // can return early This avoids appending it back to the end
                    // of the level
                    if meet_scopes_l.len() <= 1 {
                        return Ok(());
                    }

                    let level_path = option.level_fs_path(level).unwrap_or(&option.base_path);
                    let level_fs = ctx.manager.get_fs(level_path);
                    let mut streams = Vec::with_capacity(meet_scopes_l.len());
                    for scope in meet_scopes_l.iter() {
                        let file = level_fs
                            .open_options(
                                &option.table_path(scope.gen, level),
                                FileType::Parquet.open_options(true),
                            )
                            .await?;

                        streams.push(ScanStream::SsTable {
                            inner: SsTable::open(ctx.parquet_lru.clone(), scope.gen, file)
                                .await?
                                .scan(
                                    (Bound::Unbounded, Bound::Unbounded),
                                    u32::MAX.into(),
                                    None,
                                    ProjectionMask::all(),
                                    None, // Default order for compaction
                                )
                                .await?,
                        });
                    }
                    <LeveledCompactor<R> as Compactor<R>>::build_tables(
                        option,
                        version_edits,
                        level + 1,
                        streams,
                        instance,
                        level_fs,
                    )
                    .await?;

                    for scope in meet_scopes_l {
                        version_edits.push(VersionEdit::Remove {
                            level: level as u8,
                            gen: scope.gen,
                        });
                        delete_gens.push(SsTableID::new(scope.gen, level));
                    }
                }

                return Ok(());
            }

            // CONDITION 3: Normal compaction (threshold exceeded OR manual with next level not empty)
            let (meet_scopes_l, start_l, end_l) =
                Self::this_level_scopes(version, min, max, level, threshold_exceeded);
            if meet_scopes_l.is_empty() {
                return Ok(());
            }

            let (meet_scopes_ll, start_ll, end_ll) =
                Self::next_level_scopes(version, &mut min, &mut max, level, &meet_scopes_l)?;

            let level_path = option.level_fs_path(level).unwrap_or(&option.base_path);
            let level_fs = ctx.manager.get_fs(level_path);
            let mut streams = Vec::with_capacity(meet_scopes_l.len() + meet_scopes_ll.len());

            // Behaviour for level 0 is different as it is unsorted + has overlapping keys
            if level == 0 {
                for scope in meet_scopes_l.iter() {
                    let file = level_fs
                        .open_options(
                            &option.table_path(scope.gen, level),
                            FileType::Parquet.open_options(true),
                        )
                        .await?;

                    streams.push(ScanStream::SsTable {
                        inner: SsTable::open(ctx.parquet_lru.clone(), scope.gen, file)
                            .await?
                            .scan(
                                (Bound::Unbounded, Bound::Unbounded),
                                u32::MAX.into(),
                                None,
                                ProjectionMask::all(),
                                None, // Default order for compaction
                            )
                            .await?,
                    });
                }
            } else {
                let (lower, upper) = <LeveledCompactor<R> as Compactor<R>>::full_scope(&meet_scopes_l)?;
                let level_scan_l = LevelStream::new(
                    version,
                    level,
                    start_l,
                    end_l,
                    (Bound::Included(lower), Bound::Included(upper)),
                    u32::MAX.into(),
                    None,
                    ProjectionMask::all(),
                    level_fs.clone(),
                    ctx.parquet_lru.clone(),
                    None, // Default order for compaction
                )
                .ok_or(CompactionError::EmptyLevel)?;

                streams.push(ScanStream::Level {
                    inner: level_scan_l,
                });
            }

            let level_l_path = option.level_fs_path(level + 1).unwrap_or(&option.base_path);
            let level_l_fs = ctx.manager.get_fs(level_l_path);

            // Pushes next level SSTs that fall in the range
            if !meet_scopes_ll.is_empty() {
                let (lower, upper) =
                    <LeveledCompactor<R> as Compactor<R>>::full_scope(&meet_scopes_ll)?;
                let level_scan_ll = LevelStream::new(
                    version,
                    level + 1,
                    start_ll,
                    end_ll,
                    (Bound::Included(lower), Bound::Included(upper)),
                    u32::MAX.into(),
                    None,
                    ProjectionMask::all(),
                    level_l_fs.clone(),
                    ctx.parquet_lru.clone(),
                    None, // Default order for compaction
                )
                .ok_or(CompactionError::EmptyLevel)?;

                streams.push(ScanStream::Level {
                    inner: level_scan_ll,
                });
            }

            // Build the new SSTs
            <LeveledCompactor<R> as Compactor<R>>::build_tables(
                option,
                version_edits,
                level + 1,
                streams,
                instance,
                level_l_fs,
            )
            .await?;

            // Delete old files on both levels
            for scope in meet_scopes_l {
                version_edits.push(VersionEdit::Remove {
                    level: level as u8,
                    gen: scope.gen,
                });
                delete_gens.push(SsTableID::new(scope.gen, level));
            }
            for scope in meet_scopes_ll {
                version_edits.push(VersionEdit::Remove {
                    level: (level + 1) as u8,
                    gen: scope.gen,
                });
                delete_gens.push(SsTableID::new(scope.gen, level + 1));
            }

            level += 1;
        }

        Ok(())
    }

    // Combine immutable memtables into SST file
    async fn minor_compaction(
        option: &DbOption,
        recover_wal_ids: Option<Vec<FileId>>,
        batches: &[(
            Option<FileId>,
            ImmutableMemTable<<R::Schema as RecordSchema>::Columns>,
        )],
        schema: &R::Schema,
        manager: &StoreManager,
    ) -> Result<Option<Scope<<R::Schema as RecordSchema>::Key>>, CompactionError<R>> {
        if !batches.is_empty() {
            let level_0_path = option.level_fs_path(0).unwrap_or(&option.base_path);
            let level_0_fs = manager.get_fs(level_0_path);

            let mut min = None;
            let mut max = None;

            let gen = generate_file_id();
            let mut wal_ids = Vec::with_capacity(batches.len());

            // Creates writer to write Arrow record batches into parquet
            let mut writer = AsyncArrowWriter::try_new(
                AsyncWriter::new(
                    level_0_fs
                        .open_options(
                            &option.table_path(gen, 0),
                            FileType::Parquet.open_options(false),
                        )
                        .await?,
                ),
                schema.arrow_schema().clone(),
                Some(option.write_parquet_properties.clone()),
            )?;

            // Retrieve WAL ids so recovery is possible if the database crashes before
            // the SST id is written to the `Version`
            if let Some(mut recover_wal_ids) = recover_wal_ids {
                wal_ids.append(&mut recover_wal_ids);
            }
            for (file_id, batch) in batches {
                if let (Some(batch_min), Some(batch_max)) = batch.scope() {
                    if matches!(min.as_ref().map(|min| min > batch_min), Some(true) | None) {
                        min = Some(batch_min.clone())
                    }
                    if matches!(max.as_ref().map(|max| max < batch_max), Some(true) | None) {
                        max = Some(batch_max.clone())
                    }
                }
                writer.write(batch.as_record_batch()).await?;
                if let Some(file_id) = file_id {
                    wal_ids.push(*file_id);
                }
            }
            let file_size = writer.bytes_written() as u64;
            writer.close().await?;
            return Ok(Some(Scope {
                min: min.ok_or(CompactionError::EmptyLevel)?,
                max: max.ok_or(CompactionError::EmptyLevel)?,
                gen,
                wal_ids: Some(wal_ids),
                file_size,
            }));
        }
        Ok(None)
    }

    // Finds all SST files in the next level that overlap the range of the current level
    fn next_level_scopes<'a>(
        version: &'a Version<R>,
        min: &mut &'a <R::Schema as RecordSchema>::Key,
        max: &mut &'a <R::Schema as RecordSchema>::Key,
        level: usize,
        meet_scopes_l: &[&'a Scope<<R::Schema as RecordSchema>::Key>],
    ) -> Result<
        (
            Vec<&'a Scope<<R::Schema as RecordSchema>::Key>>,
            usize,
            usize,
        ),
        CompactionError<R>,
    > {
        let mut meet_scopes_ll = Vec::new();
        let mut start_ll = 0;
        let mut end_ll = 0;

        if !version.level_slice[level + 1].is_empty() {
            *min = meet_scopes_l
                .iter()
                .map(|scope| &scope.min)
                .min()
                .ok_or(CompactionError::EmptyLevel)?;

            *max = meet_scopes_l
                .iter()
                .map(|scope| &scope.max)
                .max()
                .ok_or(CompactionError::EmptyLevel)?;

            start_ll = Version::<R>::scope_search(min, &version.level_slice[level + 1]);
            end_ll = Version::<R>::scope_search(max, &version.level_slice[level + 1]);

            let next_level_len = version.level_slice[level + 1].len();
            for scope in version.level_slice[level + 1]
                [start_ll..cmp::min(end_ll + 1, next_level_len)]
                .iter()
            {
                if scope.contains(min) || scope.contains(max) {
                    meet_scopes_ll.push(scope);
                }
            }
        }
        Ok((meet_scopes_ll, start_ll, end_ll))
    }

    // Finds SST files in the specified level that overlap with the key ranges
    fn this_level_scopes<'a>(
        version: &'a Version<R>,
        min: &<R::Schema as RecordSchema>::Key,
        max: &<R::Schema as RecordSchema>::Key,
        level: usize,
        is_threshold_exceeded: bool,
    ) -> (
        Vec<&'a Scope<<R::Schema as RecordSchema>::Key>>,
        usize,
        usize,
    ) {
        let mut meet_scopes_l = Vec::new();
        let mut start_l = Version::<R>::scope_search(min, &version.level_slice[level]);
        let mut end_l = start_l;
        let option = version.option();

        if level == 0 {
            let add_scopes: Vec<_> = version.level_slice[0]
                .iter()
                .filter(|s| s.contains(min) || s.contains(max))
                .collect();

            // Do not need to update start and end values because level 0 does not open SST tables
            // with a range
            meet_scopes_l.extend(add_scopes);

            // TODO: Not return early here and adjust the logic to not return `end_l - 1`
            // Return early here to avoid underflow subtraction error
            if !meet_scopes_l.is_empty() {
                return (meet_scopes_l, 0, 0);
            }
        } else {
            for scope in version.level_slice[level][start_l..].iter() {
                if (scope.contains(min) || scope.contains(max))
                    && meet_scopes_l.len() <= option.major_l_selection_table_max_num
                {
                    meet_scopes_l.push(scope);
                    end_l += 1;
                } else {
                    break;
                }
            }
        }

        if meet_scopes_l.is_empty() {
            // If meet scopes is empty during manual compaction, compaction can be halted
            if !is_threshold_exceeded {
                return (meet_scopes_l, 0, 0);
            }

            start_l = 0;
            end_l = cmp::min(
                option.major_default_oldest_table_num,
                version.level_slice[level].len(),
            );

            for scope in version.level_slice[level][..end_l].iter() {
                if meet_scopes_l.len() > option.major_l_selection_table_max_num {
                    break;
                }
                meet_scopes_l.push(scope);
            }
        }

        (meet_scopes_l, start_l, end_l - 1)
    }

    /// Checks if the number of SST files in a level exceeds the major compaction threshold
    ///
    /// The threshold is calculated by multiplying the base threshold with a magnification factor
    /// that increases exponentially with the level number.
    ///
    /// Returns true if the number of tables in the level exceeds the threshold.
    pub(crate) fn is_threshold_exceeded_major(
        option: &DbOption,
        version: &Version<R>,
        level: usize,
    ) -> bool {
        Version::<R>::tables_len(version, level)
            >= (option.major_threshold_with_sst_size
                * option.level_sst_magnification.pow(level as u32))
    }
}
#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests {
    use std::sync::{atomic::AtomicU32, Arc};

    use arrow::datatypes::DataType as ArrayDataType;
    use flume::bounded;
    use fusio::{path::Path, DynFs};
    use fusio_dispatch::FsOptions;
    use parquet_lru::NoCache;
    use tempfile::TempDir;

    use crate::{
        compaction::{
            leveled::LeveledCompactor,
            tests::{build_parquet_table, build_version},
        },
        context::Context,
        executor::tokio::TokioExecutor,
        fs::{generate_file_id, manager::StoreManager},
        inmem::{
            immutable::{tests::TestSchema, ImmutableMemTable},
            mutable::MutableMemTable,
        },
        record::{DynRecord, DynSchema, DynamicField, Record, Schema, Value},
        scope::Scope,
        tests::Test,
        trigger::{TriggerFactory, TriggerType},
        version::{
            cleaner::Cleaner, edit::VersionEdit, set::VersionSet, timestamp::Timestamp, Version,
            MAX_LEVEL,
        },
        wal::log::LogType,
        DbError, DbOption, DB,
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

        let mutable = MutableMemTable::new(option, trigger, fs.clone(), schema.clone()).await?;

        for (log_ty, record, ts) in records {
            let _ = mutable.insert(log_ty, record, ts).await?;
        }
        Ok(mutable.into_immutable().await?.1)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn minor_compaction() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_l0 = tempfile::tempdir().unwrap();

        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .level_path(
            0,
            Path::from_filesystem_path(temp_dir_l0.path()).unwrap(),
            FsOptions::Local,
        )
        .unwrap();
        let manager =
            StoreManager::new(option.base_fs.clone(), option.level_paths.clone()).unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let batch_1 = build_immutable::<Test>(
            &option,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: 3.to_string(),
                        vu32: 0,
                        vbool: None,
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 5.to_string(),
                        vu32: 0,
                        vbool: None,
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 6.to_string(),
                        vu32: 0,
                        vbool: None,
                    },
                    0.into(),
                ),
            ],
            &Arc::new(TestSchema),
            manager.base_fs(),
        )
        .await
        .unwrap();

        let batch_2 = build_immutable::<Test>(
            &option,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: 4.to_string(),
                        vu32: 0,
                        vbool: None,
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 2.to_string(),
                        vu32: 0,
                        vbool: None,
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: 1.to_string(),
                        vu32: 0,
                        vbool: None,
                    },
                    0.into(),
                ),
            ],
            &Arc::new(TestSchema),
            manager.base_fs(),
        )
        .await
        .unwrap();

        let scope = LeveledCompactor::<Test>::minor_compaction(
            &option,
            None,
            &vec![
                (Some(generate_file_id()), batch_1),
                (Some(generate_file_id()), batch_2),
            ],
            &TestSchema,
            &manager,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(scope.min, 1.to_string());
        assert_eq!(scope.max, 6.to_string());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dyn_minor_compaction() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = StoreManager::new(FsOptions::Local, vec![]).unwrap();
        let schema = DynSchema::new(
            vec![DynamicField::new(
                "id".to_owned(),
                ArrayDataType::Int32,
                false,
            )],
            0,
        );
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &schema,
        );
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let instance = Arc::new(schema);

        let mut batch1_data = vec![];
        let mut batch2_data = vec![];
        for i in 0..40 {
            let col = Value::Int32(i);
            if i % 4 == 0 {
                continue;
            }
            if i < 35 && (i % 2 == 0 || i % 5 == 0) {
                batch1_data.push((LogType::Full, DynRecord::new(vec![col], 0), 0.into()));
            } else if i >= 7 {
                batch2_data.push((LogType::Full, DynRecord::new(vec![col], 0), 0.into()));
            }
        }

        // data range: [2, 34]
        let batch_1 =
            build_immutable::<DynRecord>(&option, batch1_data, &instance, manager.base_fs())
                .await
                .unwrap();

        // data range: [7, 39]
        let batch_2 =
            build_immutable::<DynRecord>(&option, batch2_data, &instance, manager.base_fs())
                .await
                .unwrap();

        let scope = LeveledCompactor::<DynRecord>::minor_compaction(
            &option,
            None,
            &vec![
                (Some(generate_file_id()), batch_1),
                (Some(generate_file_id()), batch_2),
            ],
            &instance,
            &manager,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(scope.min, Value::Int32(2));
        assert_eq!(scope.max, Value::Int32(39));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn major_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let temp_dir_l0 = TempDir::new().unwrap();
        let temp_dir_l1 = TempDir::new().unwrap();

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .level_path(
            0,
            Path::from_filesystem_path(temp_dir_l0.path()).unwrap(),
            FsOptions::Local,
        )
        .unwrap()
        .level_path(
            1,
            Path::from_filesystem_path(temp_dir_l1.path()).unwrap(),
            FsOptions::Local,
        )
        .unwrap();
        option.major_threshold_with_sst_size = 2;
        let option = Arc::new(option);
        let manager = Arc::new(
            StoreManager::new(option.base_fs.clone(), option.level_paths.clone()).unwrap(),
        );

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

        let ((table_gen_1, table_gen_2, table_gen_3, table_gen_4, _), version) =
            build_version(&option, &manager, &Arc::new(TestSchema)).await;

        let min = 2.to_string();
        let max = 5.to_string();
        let mut version_edits = Vec::new();

        let (_, clean_sender) = Cleaner::new(option.clone(), manager.clone());
        let manifest = Box::new(
            VersionSet::new(clean_sender, option.clone(), manager.clone())
                .await
                .unwrap(),
        );
        let ctx = Context::new(
            manager.clone(),
            Arc::new(NoCache::default()),
            manifest,
            TestSchema.arrow_schema().clone(),
        );

        LeveledCompactor::<Test>::major_compaction(
            &version,
            &option,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
            &TestSchema,
            &ctx,
            false, // test: automatic compaction
        )
        .await
        .unwrap();

        if let VersionEdit::Add { level, scope } = &version_edits[0] {
            assert_eq!(*level, 1);
            assert_eq!(scope.min, 1.to_string());
            assert_eq!(scope.max, 6.to_string());
        }
        assert_eq!(
            version_edits[1..5].to_vec(),
            vec![
                VersionEdit::Remove {
                    level: 0,
                    gen: table_gen_1,
                },
                VersionEdit::Remove {
                    level: 0,
                    gen: table_gen_2,
                },
                VersionEdit::Remove {
                    level: 1,
                    gen: table_gen_3,
                },
                VersionEdit::Remove {
                    level: 1,
                    gen: table_gen_4,
                },
            ]
        );
    }

    // https://github.com/tonbo-io/tonbo/pull/139
    #[tokio::test(flavor = "multi_thread")]
    async fn major_panic() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );
        option.major_threshold_with_sst_size = 1;
        option.level_sst_magnification = 1;
        let manager = Arc::new(
            StoreManager::new(option.base_fs.clone(), option.level_paths.clone()).unwrap(),
        );

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

        let level_0_fs = option
            .level_fs_path(0)
            .map(|path| manager.get_fs(path))
            .unwrap_or(manager.base_fs());
        let level_1_fs = option
            .level_fs_path(1)
            .map(|path| manager.get_fs(path))
            .unwrap_or(manager.base_fs());

        let table_gen0 = generate_file_id();
        let table_gen1 = generate_file_id();
        let mut records0 = vec![];
        let mut records1 = vec![];
        for i in 0..10 {
            let record = (
                LogType::Full,
                Test {
                    vstring: i.to_string(),
                    vu32: i,
                    vbool: Some(true),
                },
                0.into(),
            );
            if i < 5 {
                records0.push(record);
            } else {
                records1.push(record);
            }
        }
        build_parquet_table::<Test>(
            &option,
            table_gen0,
            records0,
            &Arc::new(TestSchema),
            0,
            level_0_fs,
        )
        .await
        .unwrap();
        build_parquet_table::<Test>(
            &option,
            table_gen1,
            records1,
            &Arc::new(TestSchema),
            1,
            level_1_fs,
        )
        .await
        .unwrap();

        let option = Arc::new(option);
        let (sender, _) = bounded(1);
        let mut version =
            Version::<Test>::new(option.clone(), sender, Arc::new(AtomicU32::default()));
        version.level_slice[0].push(Scope {
            min: 0.to_string(),
            max: 4.to_string(),
            gen: table_gen0,
            wal_ids: None,
            file_size: 13,
        });
        version.level_slice[1].push(Scope {
            min: 5.to_string(),
            max: 9.to_string(),
            gen: table_gen1,
            wal_ids: None,
            file_size: 13,
        });

        let mut version_edits = Vec::new();
        let min = 6.to_string();
        let max = 9.to_string();

        let (_, clean_sender) = Cleaner::new(option.clone(), manager.clone());
        let manifest = Box::new(
            VersionSet::new(clean_sender, option.clone(), manager.clone())
                .await
                .unwrap(),
        );
        let ctx = Context::new(
            manager.clone(),
            Arc::new(NoCache::default()),
            manifest,
            TestSchema.arrow_schema().clone(),
        );
        LeveledCompactor::<Test>::major_compaction(
            &version,
            &option,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
            &TestSchema,
            &ctx,
            false, // test: automatic compaction
        )
        .await
        .unwrap();
    }

    // issue: https://github.com/tonbo-io/tonbo/issues/152
    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_major_level_sort() {
        let temp_dir = TempDir::new().unwrap();
        eprintln!("test");
        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 0;
        option.major_threshold_with_sst_size = 2;
        option.level_sst_magnification = 1;

        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), TestSchema)
            .await
            .unwrap();

        for i in 5..9 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }

        db.flush().await.unwrap();
        for i in 0..4 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        db.insert(Test {
            vstring: "6".to_owned(),
            vu32: 22,
            vbool: Some(false),
        })
        .await
        .unwrap();
        db.insert(Test {
            vstring: "8".to_owned(),
            vu32: 77,
            vbool: Some(false),
        })
        .await
        .unwrap();
        db.flush().await.unwrap();
        db.insert(Test {
            vstring: "1".to_owned(),
            vu32: 22,
            vbool: Some(false),
        })
        .await
        .unwrap();
        db.insert(Test {
            vstring: "5".to_owned(),
            vu32: 77,
            vbool: Some(false),
        })
        .await
        .unwrap();
        db.flush().await.unwrap();

        db.insert(Test {
            vstring: "2".to_owned(),
            vu32: 22,
            vbool: Some(false),
        })
        .await
        .unwrap();
        db.insert(Test {
            vstring: "7".to_owned(),
            vu32: 77,
            vbool: Some(false),
        })
        .await
        .unwrap();
        db.flush().await.unwrap();

        let version = db.ctx.manifest().current().await;

        for level in 0..MAX_LEVEL {
            let sort_runs = &version.level_slice[level];

            if sort_runs.is_empty() {
                continue;
            }
            for pos in 0..sort_runs.len() - 1 {
                let current = &sort_runs[pos];
                let next = &sort_runs[pos + 1];

                assert!(current.min < current.max);
                assert!(next.min < next.max);

                if level == 0 {
                    continue;
                }
                assert!(current.max < next.min);
            }
        }
        dbg!(version);
    }

    // Self compaction is when on level 0 there is no files in the next level
    #[tokio::test(flavor = "multi_thread")]
    async fn test_manual_self_compaction() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 5;
        option.level_sst_magnification = 1;

        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), TestSchema)
            .await
            .unwrap();

        // Flush once with SST of min: 5 and max: 9
        for i in 5..10 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }

        db.flush().await.unwrap();

        // Flush again with SST of min: 2 and max: 6
        for i in 2..7 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }

        db.flush().await.unwrap();

        // Insert SST of min: 3 and max: 7
        // This should trigger compaction for the first two SSTs because
        // their key ranges fall under 3-7
        for i in 3..8 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        let version = db.ctx.manifest.current().await;
        let sort_runs_zero = &version.level_slice[0];
        let sort_runs_one = &version.level_slice[1];

        assert_eq!(sort_runs_zero.len(), 1);
        assert_eq!(sort_runs_one.len(), 1);

        assert_eq!(sort_runs_zero[0].min, "3");
        assert_eq!(sort_runs_zero[0].max, "7");

        assert_eq!(sort_runs_one[0].min, "2");
        assert_eq!(sort_runs_one[0].max, "9");
    }

    // Test manual self compaction when no key ranges are met
    #[tokio::test(flavor = "multi_thread")]
    async fn test_manual_self_no_compaction() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 5;
        option.level_sst_magnification = 1;

        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), TestSchema)
            .await
            .unwrap();

        // Flush once with SST of min: 5 and max: 9
        for i in 5..10 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }

        db.flush().await.unwrap();

        // Flush again with SST of min: 2 and max: 6
        for i in 2..7 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }

        db.flush().await.unwrap();

        // Insert SST of min: 10 and max: 15
        // Should not trigger compaction as the first two SST's
        // key ranges do not fall in udner 10-15
        for i in 10..15 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        let version = db.ctx.manifest.current().await;
        let sort_runs = &version.level_slice[0];

        assert_eq!(sort_runs.len(), 3);

        assert_eq!(sort_runs[0].min, "5");
        assert_eq!(sort_runs[0].max, "9");

        assert_eq!(sort_runs[1].min, "2");
        assert_eq!(sort_runs[1].max, "6");

        assert_eq!(sort_runs[2].min, "10");
        assert_eq!(sort_runs[2].max, "14");
    }

    // This use to fail because SSTs on level 0 would be returned as a range;
    // this logic was in [`LeveledCompactor::this_level_scopes`].
    // In the case of SST(1-5), SST(20-25), SST(5-10), SST(4-6); the fourth
    // SST manual flush should trigger a compaction between the first and third
    // SST however because it was in a range it would drop after the first SST.
    //
    // This test makes sure that it will find all SSTs in that range.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_self_manual_compaction_fix_range() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 5;
        option.level_sst_magnification = 1;

        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), TestSchema)
            .await
            .unwrap();

        // Flush once with SST of min: 5 and max: 9
        for i in 5..10 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }

        db.flush().await.unwrap();

        // Flush again with SST of min: 20 and max: 24
        for i in 20..25 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }

        db.flush().await.unwrap();

        for i in 2..5 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 3..8 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        let version = db.ctx.manifest.current().await;
        let sort_runs_l0 = &version.level_slice[0];
        let sort_runs_l1 = &version.level_slice[1];

        assert_eq!(sort_runs_l0.len(), 2);
        assert_eq!(sort_runs_l1.len(), 1);

        assert_eq!(sort_runs_l0[0].min, "20");
        assert_eq!(sort_runs_l0[0].max, "24");

        assert_eq!(sort_runs_l0[1].min, "3");
        assert_eq!(sort_runs_l0[1].max, "7");

        assert_eq!(sort_runs_l1[0].min, "2");
        assert_eq!(sort_runs_l1[0].max, "9");
    }

    // This is to check that it doesnt self compact if the threshold is exceeded
    // It also checks that if the threshold is reached a second time that the manual
    // compaction will flush to the next level even if threshold isnt reached.
    // issue: https://github.com/tonbo-io/tonbo/issues/158
    #[tokio::test(flavor = "multi_thread")]
    async fn test_self_manual_compaction_level_1() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 5;
        option.level_sst_magnification = 1;

        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), TestSchema)
            .await
            .unwrap();

        for i in 0..5 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 5..10 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 10..15 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 15..20 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 20..25 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 4..7 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        let version = db.ctx.manifest.current().await;
        let sort_runs_level_0 = &version.level_slice[0];
        let sort_runs_level_1 = &version.level_slice[1];
        let sort_runs_level_2 = &version.level_slice[2];

        // Six SSTs are inserted
        // The logic here is as follows:
        //  1. Inserts 5 non overlapping SSTs to not trigger self compaction
        //  2. The sixth SST is inserted which overlaps with two SSTs and pushes the length over the
        //     threshold. This compacts the two SSTs into the next level and adds the new SST into
        //     level 0.
        assert_eq!(sort_runs_level_0.len(), 4);
        assert_eq!(sort_runs_level_1.len(), 1);
        assert!(sort_runs_level_2.is_empty());

        for i in 25..30 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        let version = db.ctx.manifest.current().await;
        let sort_runs_level_0 = &version.level_slice[0];
        assert_eq!(sort_runs_level_0.len(), 5);

        for i in 4..7 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        let version = db.ctx.manifest.current().await;
        let sort_runs_level_0 = &version.level_slice[0];
        let sort_runs_level_1 = &version.level_slice[1];
        let sort_runs_level_2 = &version.level_slice[2];

        // Two SSTs are inserted.
        // The logic here is as follow:
        //  1. Add one non overlapping SST -> there is no compaction
        //  2. Add an which overlaps with both one SST in level 0 and level 1. These combine to form
        //     a new SST on level 1.
        //  4. Compaction does not continue into the next level for level 1 because non level 0 does
        //     not self compact if threshold isn't exceeded.
        assert_eq!(sort_runs_level_0.len(), 5);
        assert_eq!(sort_runs_level_1.len(), 1);
        assert_eq!(sort_runs_level_2.len(), 0);
    }

    // Issue: https://github.com/tonbo-io/tonbo/issues/151
    // TODO: Remove the write amplification
    #[tokio::test(flavor = "multi_thread")]
    async fn write_amplification_test() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        );
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 1;
        option.major_threshold_with_sst_size = 2;
        option.level_sst_magnification = 1;

        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(100);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), TestSchema)
            .await
            .unwrap();

        for i in 100..130 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 200..300 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 7..100 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 5..8 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 0..3 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        for i in 2..7 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }
        db.flush().await.unwrap();

        let version = db.ctx.manifest.current().await;
        let sort_runs_level_0 = &version.level_slice[0];
        let sort_runs_level_1 = &version.level_slice[1];
        let sort_runs_level_2 = &version.level_slice[2];

        assert_eq!(sort_runs_level_0.len(), 1);
        assert_eq!(sort_runs_level_1.len(), 2);
        assert_eq!(sort_runs_level_2.len(), 1);
    }
}
