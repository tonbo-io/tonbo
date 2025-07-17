use std::{cmp, collections::Bound, mem, sync::Arc};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use common::{
    util::{value_gt, value_lt},
    Keys, Value,
};
use fusio_parquet::writer::AsyncWriter;
use parquet::arrow::{AsyncArrowWriter, ProjectionMask};

use super::Compactor;
use crate::{
    compaction::CompactionError,
    context::Context,
    fs::{generate_file_id, manager::StoreManager, FileId, FileType},
    inmem::{immutable::Immutable, mutable::MutableMemTable},
    ondisk::sstable::SsTable,
    record::{Record, Schema},
    scope::Scope,
    stream::{level::LevelStream, ScanStream},
    version::{edit::VersionEdit, TransactionTs, Version, MAX_LEVEL},
    DbOption, DbStorage,
};

pub(crate) struct LeveledCompactor<R>
where
    R: Record,
{
    option: Arc<DbOption>,
    schema: Arc<RwLock<DbStorage<R>>>,
    ctx: Arc<Context<R>>,
    record_schema: Arc<Schema>,
}

impl<R> LeveledCompactor<R>
where
    R: Record,
{
    pub(crate) fn new(
        schema: Arc<RwLock<DbStorage<R>>>,
        record_schema: Arc<Schema>,
        option: Arc<DbOption>,
        ctx: Arc<Context<R>>,
    ) -> Self {
        LeveledCompactor::<R> {
            option,
            schema,
            ctx,
            record_schema,
        }
    }

    pub(crate) async fn check_then_compaction(
        &mut self,
        is_manual: bool,
    ) -> Result<(), CompactionError> {
        let mut guard = self.schema.write().await;

        guard.trigger.reset();

        if !guard.mutable.is_empty() {
            let trigger_clone = guard.trigger.clone();

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

            let guard = self.schema.upgradable_read().await;
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
                let version_ref = self.ctx.version_set.current().await;
                let mut version_edits = vec![];
                let mut delete_gens = vec![];

                if Self::is_threshold_exceeded_major(&self.option, &version_ref, 0) {
                    Self::major_compaction(
                        &version_ref,
                        &self.option,
                        scope.min.as_ref(),
                        scope.max.as_ref(),
                        &mut version_edits,
                        &mut delete_gens,
                        &guard.record_schema,
                        &self.ctx,
                    )
                    .await?;
                }
                version_edits.insert(0, VersionEdit::Add { level: 0, scope });
                version_edits.push(VersionEdit::LatestTimeStamp {
                    ts: version_ref.increase_ts(),
                });

                self.ctx
                    .version_set
                    .apply_edits(version_edits, Some(delete_gens), false)
                    .await?;
            }
            let mut guard = RwLockUpgradableReadGuard::upgrade(guard).await;
            let sources = guard.immutables.split_off(chunk_num);
            let _ = mem::replace(&mut guard.immutables, sources);
        }
        if is_manual {
            self.ctx.version_set.rewrite().await.unwrap();
        }
        Ok(())
    }

    async fn minor_compaction(
        option: &DbOption,
        recover_wal_ids: Option<Vec<FileId>>,
        batches: &[(Option<FileId>, Immutable<R::Columns>)],
        schema: &Schema,
        manager: &StoreManager,
    ) -> Result<Option<Scope>, CompactionError> {
        if !batches.is_empty() {
            let level_0_path = option.level_fs_path(0).unwrap_or(&option.base_path);
            let level_0_fs = manager.get_fs(level_0_path);

            let mut min = None;
            let mut max = None;

            let gen = generate_file_id();
            let mut wal_ids = Vec::with_capacity(batches.len());

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
                option.write_parquet_properties.clone(),
            )?;

            if let Some(mut recover_wal_ids) = recover_wal_ids {
                wal_ids.append(&mut recover_wal_ids);
            }
            for (file_id, batch) in batches {
                if let (Some(batch_min), Some(batch_max)) = batch.scope() {
                    if matches!(
                        min.as_ref().map(|min| value_gt(min, &batch_min)),
                        Some(true) | None
                    ) {
                        min = Some(batch_min.clone())
                    }
                    if matches!(
                        max.as_ref().map(|max| value_lt(max, &batch_max)),
                        Some(true) | None
                    ) {
                        max = Some(batch_max.clone())
                    }
                }
                writer.write(batch.as_record_batch()).await?;
                if let Some(file_id) = file_id {
                    wal_ids.push(*file_id);
                }
            }
            writer.close().await?;
            return Ok(Some(Scope {
                min: min.ok_or(CompactionError::EmptyLevel)?,
                max: max.ok_or(CompactionError::EmptyLevel)?,
                gen,
                wal_ids: Some(wal_ids),
            }));
        }
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    async fn major_compaction(
        version: &Version<R>,
        option: &DbOption,
        mut min: &Keys,
        mut max: &Keys,
        version_edits: &mut Vec<VersionEdit>,
        delete_gens: &mut Vec<(FileId, usize)>,
        instance: &Schema,
        ctx: &Context<R>,
    ) -> Result<(), CompactionError> {
        let mut level = 0;

        while level < MAX_LEVEL - 2 {
            if !Self::is_threshold_exceeded_major(option, version, level) {
                break;
            }
            let (meet_scopes_l, start_l, end_l) = Self::this_level_scopes(version, min, max, level);
            let (meet_scopes_ll, start_ll, end_ll) =
                Self::next_level_scopes(version, &mut min, &mut max, level, &meet_scopes_l)?;

            let level_path = option.level_fs_path(level).unwrap_or(&option.base_path);
            let level_fs = ctx.manager.get_fs(level_path);
            let mut streams = Vec::with_capacity(meet_scopes_l.len() + meet_scopes_ll.len());
            // This Level
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
                            )
                            .await?,
                    });
                }
            } else {
                let (lower, upper) = Compactor::<R>::full_scope(&meet_scopes_l)?;
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
                )
                .ok_or(CompactionError::EmptyLevel)?;

                streams.push(ScanStream::Level {
                    inner: level_scan_l,
                });
            }

            let level_l_path = option.level_fs_path(level + 1).unwrap_or(&option.base_path);
            let level_l_fs = ctx.manager.get_fs(level_l_path);
            if !meet_scopes_ll.is_empty() {
                // Next Level
                let (lower, upper) = Compactor::<R>::full_scope(&meet_scopes_ll)?;
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
                )
                .ok_or(CompactionError::EmptyLevel)?;

                streams.push(ScanStream::Level {
                    inner: level_scan_ll,
                });
            }

            Compactor::<R>::build_tables(
                option,
                version_edits,
                level + 1,
                streams,
                instance,
                level_l_fs,
            )
            .await?;

            for scope in meet_scopes_l {
                version_edits.push(VersionEdit::Remove {
                    level: level as u8,
                    gen: scope.gen,
                });
                delete_gens.push((scope.gen, level));
            }
            for scope in meet_scopes_ll {
                version_edits.push(VersionEdit::Remove {
                    level: (level + 1) as u8,
                    gen: scope.gen,
                });
                delete_gens.push((scope.gen, level + 1));
            }
            level += 1;
        }

        Ok(())
    }

    fn next_level_scopes<'a>(
        version: &'a Version<R>,
        min: &mut &'a Keys,
        max: &mut &'a Keys,
        level: usize,
        meet_scopes_l: &[&'a Scope],
    ) -> Result<(Vec<&'a Scope>, usize, usize), CompactionError> {
        let mut meet_scopes_ll = Vec::new();
        let mut start_ll = 0;
        let mut end_ll = 0;

        if !version.level_slice[level + 1].is_empty() {
            *min = meet_scopes_l
                .iter()
                .map(|scope| scope.min.as_ref())
                .min()
                .ok_or(CompactionError::EmptyLevel)?;

            *max = meet_scopes_l
                .iter()
                .map(|scope| scope.max.as_ref())
                .max()
                .ok_or(CompactionError::EmptyLevel)?;

            start_ll = Version::<R>::scope_search(*min, &version.level_slice[level + 1]);
            end_ll = Version::<R>::scope_search(*max, &version.level_slice[level + 1]);

            let next_level_len = version.level_slice[level + 1].len();
            for scope in version.level_slice[level + 1]
                [start_ll..cmp::min(end_ll + 1, next_level_len)]
                .iter()
            {
                if scope.contains(*min) || scope.contains(*max) {
                    meet_scopes_ll.push(scope);
                }
            }
        }
        Ok((meet_scopes_ll, start_ll, end_ll))
    }

    fn this_level_scopes<'a>(
        version: &'a Version<R>,
        min: &Keys,
        max: &Keys,
        level: usize,
    ) -> (Vec<&'a Scope>, usize, usize) {
        let mut meet_scopes_l = Vec::new();
        let mut start_l = Version::<R>::scope_search(min, &version.level_slice[level]);
        let mut end_l = start_l;
        let option = version.option();

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
        if meet_scopes_l.is_empty() {
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

    use arrow::datatypes::{DataType as ArrowDataType, Field};
    use common::{datatype::DataType, util::value_eq, AsValue, Key, Keys, PrimaryKey, Value};
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
        inmem::{immutable::Immutable, mutable::MutableMemTable},
        record::{
            // DynRecord,
            DynRecord,
            Record,
            Schema, // ValueDesc
        },
        scope::Scope,
        tests::Test,
        timestamp::Timestamp,
        trigger::{TriggerFactory, TriggerType},
        version::{cleaner::Cleaner, edit::VersionEdit, set::VersionSet, Version, MAX_LEVEL},
        wal::log::LogType,
        DbError, DbOption, DB,
    };

    async fn build_immutable<R>(
        option: &DbOption,
        records: Vec<(LogType, R, Timestamp)>,
        schema: &Arc<Schema>,
        fs: &Arc<dyn DynFs>,
    ) -> Result<Immutable<R::Columns>, DbError>
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

        let option = DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap())
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
            &Arc::new(Test::schema()),
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
            &Arc::new(Test::schema()),
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
            &Test::schema(),
            &manager,
        )
        .await
        .unwrap()
        .unwrap();
        dbg!(&scope);

        assert!(value_eq(scope.min.as_ref(), &vec![Arc::new(1.to_string())]));
        assert!(value_eq(scope.max.as_ref(), &vec![Arc::new(6.to_string())]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dyn_minor_compaction() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = StoreManager::new(FsOptions::Local, vec![]).unwrap();
        let schema = Schema::new(vec![Field::new("id", ArrowDataType::Int32, false)], 0);
        let option = DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap());
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let instance = Arc::new(schema);

        let mut batch1_data = vec![];
        let mut batch2_data = vec![];
        for i in 0..40 {
            let col = Arc::new(i);
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
        assert_eq!(&scope.min, &vec![Arc::new(2) as Arc<dyn Value>]);
        assert_eq!(&scope.max, &vec![Arc::new(39) as Arc<dyn Value>]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn major_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let temp_dir_l0 = TempDir::new().unwrap();
        let temp_dir_l1 = TempDir::new().unwrap();

        let mut option = DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap())
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
            build_version(&option, &manager, &Arc::new(Test::schema())).await;

        let min: Keys = vec![Arc::new(2.to_string())];
        let max: Keys = vec![Arc::new(5.to_string())];
        let mut version_edits = Vec::new();

        let (_, clean_sender) = Cleaner::new(option.clone(), manager.clone());
        let version_set = VersionSet::new(clean_sender, option.clone(), manager.clone())
            .await
            .unwrap();
        let ctx = Context::new(
            manager.clone(),
            Arc::new(NoCache::default()),
            version_set,
            Arc::new(Test::schema()),
        );

        LeveledCompactor::<Test>::major_compaction(
            &version,
            &option,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
            &Test::schema(),
            &ctx,
        )
        .await
        .unwrap();

        if let VersionEdit::Add { level, scope } = &version_edits[0] {
            assert_eq!(*level, 1);
            assert_eq!(scope.min, vec![Arc::new(1.to_string()) as Arc<dyn Value>]);
            assert_eq!(scope.max, vec![Arc::new(6.to_string()) as Arc<dyn Value>]);
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

        let mut option = DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap());
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
            &Arc::new(Test::schema()),
            0,
            level_0_fs,
        )
        .await
        .unwrap();
        build_parquet_table::<Test>(
            &option,
            table_gen1,
            records1,
            &Arc::new(Test::schema()),
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
            min: vec![Arc::new(0.to_string())],
            max: vec![Arc::new(4.to_string())],
            gen: table_gen0,
            wal_ids: None,
        });
        version.level_slice[1].push(Scope {
            min: vec![Arc::new(5.to_string())],
            max: vec![Arc::new(9.to_string())],
            gen: table_gen1,
            wal_ids: None,
        });

        let mut version_edits = Vec::new();
        let min: Keys = vec![Arc::new(6.to_string())];
        let max: Keys = vec![Arc::new(9.to_string())];

        let (_, clean_sender) = Cleaner::new(option.clone(), manager.clone());
        let version_set = VersionSet::new(clean_sender, option.clone(), manager.clone())
            .await
            .unwrap();
        let ctx = Context::new(
            manager.clone(),
            Arc::new(NoCache::default()),
            version_set,
            Arc::new(Test::schema()),
        );
        LeveledCompactor::<Test>::major_compaction(
            &version,
            &option,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
            &Test::schema(),
            &ctx,
        )
        .await
        .unwrap();
    }

    // issue: https://github.com/tonbo-io/tonbo/issues/152
    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_major_level_sort() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::new(Path::from_filesystem_path(temp_dir.path()).unwrap());
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 0;
        option.major_threshold_with_sst_size = 2;
        option.level_sst_magnification = 1;

        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), Test::schema())
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

        let version = db.ctx.version_set.current().await;

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
}
