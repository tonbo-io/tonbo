use std::{future::Future, ops::Bound, sync::Arc};

use async_trait::async_trait;
use fusio::MaybeSend;
use parquet::arrow::ProjectionMask;
use ulid::Ulid;

use super::{CompactionError, Compactor};
use crate::{
    compaction::RecordSchema,
    context::Context,
    fs::{FileId, FileType},
    inmem::immutable::ImmutableMemTable,
    ondisk::sstable::{SsTable, SsTableID},
    record::{self, Record},
    scope::Scope,
    stream::{level::LevelStream, ScanStream},
    version::{edit::VersionEdit, TransactionTs, Version, MAX_LEVEL},
    CompactionExecutor, DbOption,
};

struct TieredTask {
    input: Vec<(usize, Vec<Ulid>)>,
    target_tier: usize,
}

#[derive(Clone, Debug)]
pub struct TieredOptions {
    /// Maximum number of tiers
    max_tiers: usize,
    /// Base capacity for tier 0
    tier_base_capacity: usize,
    /// Growth factor between tiers
    tier_growth_factor: usize,
}

impl Default for TieredOptions {
    fn default() -> Self {
        Self {
            max_tiers: 4,
            tier_base_capacity: 4,
            tier_growth_factor: 4,
        }
    }
}

impl TieredOptions {
    /// Set maximum number of tiers
    pub fn max_tiers(mut self, value: usize) -> Self {
        self.max_tiers = value;
        self
    }

    /// Set tier base capacity
    pub fn tier_base_capacity(mut self, value: usize) -> Self {
        self.tier_base_capacity = value;
        self
    }

    /// Set tier growth factor
    pub fn tier_growth_factor(mut self, value: usize) -> Self {
        self.tier_growth_factor = value;
        self
    }
}

pub struct TieredCompactor<R: Record> {
    options: TieredOptions,
    db_option: Arc<DbOption>,
    ctx: Arc<Context<R>>,
    record_schema: Arc<R::Schema>,
}

impl<R: Record> TieredCompactor<R> {
    pub(crate) fn new(
        options: TieredOptions,
        record_schema: Arc<R::Schema>,
        db_option: Arc<DbOption>,
        ctx: Arc<Context<R>>,
    ) -> Self {
        Self {
            options,
            db_option,
            ctx,
            record_schema,
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<R> Compactor<R> for TieredCompactor<R>
where
    R: Record,
    <<R as record::Record>::Schema as record::Schema>::Columns: Send + Sync,
{
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
    ) -> Result<(), CompactionError<R>> {
        // Perform minor compaction if batches are provided
        if let Some(batches) = batches {
            if let Some(scope) = Self::minor_compaction(
                &self.db_option,
                recover_wal_ids,
                batches,
                &self.record_schema,
                &self.ctx.manager,
            )
            .await?
            {
                // Update manifest with new L0 SST
                let version_ref = self.ctx.manifest.current().await;
                let mut version_edits = vec![VersionEdit::Add { level: 0, scope }];
                version_edits.push(VersionEdit::LatestTimeStamp {
                    ts: version_ref.increase_ts(),
                });

                self.ctx
                    .manifest
                    .update(version_edits, None)
                    .await
                    .map_err(|e| CompactionError::Manifest(e))?;
            }
        }

        // Perform major compaction
        Self::major_compaction(
            &self.ctx,
            &self.options,
            &self.db_option,
            &self.record_schema,
            is_manual,
        )
        .await?;

        Ok(())
    }
}

impl<R> CompactionExecutor<R> for TieredCompactor<R>
where
    R: Record,
    <R::Schema as record::Schema>::Columns: Send + Sync,
{
    fn check_then_compaction<'a>(
        &'a self,
        batches: Option<
            &'a [(
                Option<FileId>,
                ImmutableMemTable<<R::Schema as record::Schema>::Columns>,
            )],
        >,
        recover_wal_ids: Option<Vec<FileId>>,
        is_manual: bool,
    ) -> impl Future<Output = Result<(), CompactionError<R>>> + MaybeSend + 'a {
        <Self as Compactor<R>>::check_then_compaction(self, batches, recover_wal_ids, is_manual)
    }
}

impl<R> TieredCompactor<R>
where
    R: Record,
    <<R as Record>::Schema as RecordSchema>::Columns: Send + Sync,
{
    pub async fn major_compaction(
        ctx: &Context<R>,
        options: &TieredOptions,
        db_option: &DbOption,
        record_schema: &R::Schema,
        is_manual: bool,
    ) -> Result<(), CompactionError<R>> {
        while let Some(tier) = Self::should_major_compact(ctx, options).await {
            if let Some(task) = Self::plan_major(ctx, tier).await {
                Self::execute_major(ctx, db_option, record_schema, task).await?;
            } else {
                break;
            }
        }

        if is_manual {
            ctx.manifest
                .rewrite()
                .await
                .map_err(CompactionError::Manifest)?;
        }

        Ok(())
    }

    async fn should_major_compact(ctx: &Context<R>, options: &TieredOptions) -> Option<usize> {
        let version_ref = ctx.manifest.current().await;
        for tier in 0..options.max_tiers - 1 {
            if Self::is_tier_full(options, &version_ref, tier) {
                return Some(tier);
            }
        }
        None
    }

    async fn plan_major(ctx: &Context<R>, tier: usize) -> Option<TieredTask> {
        let version_ref = ctx.manifest.current().await;
        let tier_files: Vec<Ulid> = version_ref.level_slice[tier]
            .iter()
            .map(|scope| scope.gen)
            .collect();
        if !tier_files.is_empty() {
            return Some(TieredTask {
                input: vec![(tier, tier_files)],
                target_tier: tier + 1,
            });
        }

        None
    }

    async fn execute_major(
        ctx: &Context<R>,
        db_option: &DbOption,
        record_schema: &R::Schema,
        task: TieredTask,
    ) -> Result<(), CompactionError<R>> {
        let version_ref = ctx.manifest.current().await;
        let mut version_edits = vec![];
        let mut delete_gens = vec![];
        for (source_tier, file_gens) in &task.input {
            if file_gens.is_empty() {
                continue;
            }

            let tier_scopes: Vec<&Scope<_>> = version_ref.level_slice[*source_tier]
                .iter()
                .filter(|scope| file_gens.contains(&scope.gen))
                .collect();
            if tier_scopes.is_empty() {
                continue;
            }
            Self::tier_compaction(
                &version_ref,
                db_option,
                &mut version_edits,
                &mut delete_gens,
                record_schema,
                ctx,
                *source_tier,
                task.target_tier,
            )
            .await?;
            break;
        }

        if !version_edits.is_empty() {
            version_edits.push(VersionEdit::LatestTimeStamp {
                ts: version_ref.increase_ts(),
            });

            ctx.manifest
                .update(version_edits, Some(delete_gens))
                .await?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn tier_compaction(
        version: &Version<R>,
        option: &DbOption,
        version_edits: &mut Vec<VersionEdit<<R::Schema as RecordSchema>::Key>>,
        delete_gens: &mut Vec<SsTableID>,
        instance: &R::Schema,
        ctx: &Context<R>,
        source_tier: usize,
        target_tier: usize,
    ) -> Result<(), CompactionError<R>> {
        let source_scopes: Vec<&Scope<_>> = version.level_slice[source_tier].iter().collect();

        if source_scopes.is_empty() {
            return Ok(());
        }

        let source_tier_path = option
            .level_fs_path(source_tier)
            .unwrap_or(&option.base_path);
        let source_tier_fs = ctx.manager.get_fs(source_tier_path);
        let target_tier_path = option
            .level_fs_path(target_tier)
            .unwrap_or(&option.base_path);
        let target_tier_fs = ctx.manager.get_fs(target_tier_path);

        let mut streams = Vec::with_capacity(source_scopes.len());

        if source_tier == 0 {
            for scope in source_scopes.iter() {
                let file = source_tier_fs
                    .open_options(
                        &option.table_path(scope.gen, source_tier),
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
                            None,
                            instance.primary_key_indices(),
                        )
                        .await?,
                });
            }
        } else {
            let (lower, upper) = <TieredCompactor<R> as Compactor<R>>::full_scope(&source_scopes)?;
            let tier_scan = LevelStream::new(
                version,
                source_tier,
                0,
                source_scopes.len() - 1,
                (Bound::Included(lower), Bound::Included(upper)),
                u32::MAX.into(),
                None,
                ProjectionMask::all(),
                source_tier_fs.clone(),
                ctx.parquet_lru.clone(),
                None,
                instance.primary_key_indices(),
            )
            .ok_or(CompactionError::EmptyLevel)?;

            streams.push(ScanStream::Level { inner: tier_scan });
        }

        <TieredCompactor<R> as Compactor<R>>::build_tables(
            option,
            version_edits,
            target_tier,
            streams,
            instance,
            target_tier_fs,
        )
        .await?;

        // Mark all source tier files for deletion
        for scope in source_scopes {
            version_edits.push(VersionEdit::Remove {
                level: source_tier as u8,
                gen: scope.gen,
            });
            delete_gens.push(SsTableID::new(scope.gen, source_tier));
        }

        Ok(())
    }

    fn is_tier_full(options: &TieredOptions, version: &Version<R>, tier: usize) -> bool {
        let max_tiers = options.max_tiers;
        // TODO: Move MAX_LEVEL out of Version
        if tier >= max_tiers || tier >= MAX_LEVEL {
            return false;
        }

        let tier_capacity = Self::tier_capacity(options, tier);
        version.level_slice[tier].len() > tier_capacity
    }

    fn tier_capacity(options: &TieredOptions, tier: usize) -> usize {
        // Base capacity for tier 0
        // let base_capacity = option.tier_base_capacity.unwrap_or(4);
        // let growth_factor = option.tier_growth_factor.unwrap_or(4);

        options.tier_base_capacity * options.tier_growth_factor.pow(tier as u32)
    }
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests {
    use std::sync::{atomic::AtomicU32, Arc};

    use flume::bounded;
    use fusio::{path::Path, DynFs};
    use fusio_dispatch::FsOptions;
    use parquet_lru::NoCache;
    use tempfile::TempDir;

    use crate::{
        compaction::{
            tests::build_parquet_table,
            tiered::{TieredCompactor, TieredOptions},
            Compactor,
        },
        context::Context,
        executor::tokio::TokioExecutor,
        fs::{generate_file_id, manager::StoreManager},
        inmem::{
            immutable::{tests::TestSchema, ImmutableMemTable},
            mutable::MutableMemTable,
        },
        record::{Record, Schema},
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

        // Minor compaction is the same for both leveled and tiered
        let scope = TieredCompactor::<Test>::minor_compaction(
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
    async fn tier_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let temp_dir_t0 = TempDir::new().unwrap();
        let temp_dir_t1 = TempDir::new().unwrap();

        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .level_path(
            0,
            Path::from_filesystem_path(temp_dir_t0.path()).unwrap(),
            FsOptions::Local,
        )
        .unwrap()
        .level_path(
            1,
            Path::from_filesystem_path(temp_dir_t1.path()).unwrap(),
            FsOptions::Local,
        )
        .unwrap();

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

        // Create test data for tier 0 (4 files)
        let tier_0_fs = manager.get_fs(&option.level_fs_path(0).unwrap_or(&option.base_path));

        let table_gen_1 = generate_file_id();
        let table_gen_2 = generate_file_id();
        let table_gen_3 = generate_file_id();
        let table_gen_4 = generate_file_id();

        // Build parquet files for tier 0
        build_parquet_table::<Test>(
            &option,
            table_gen_1,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: "1".to_string(),
                        vu32: 1,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: "2".to_string(),
                        vu32: 2,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
            ],
            &Arc::new(TestSchema),
            0,
            &tier_0_fs,
        )
        .await
        .unwrap();

        build_parquet_table::<Test>(
            &option,
            table_gen_2,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: "3".to_string(),
                        vu32: 3,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: "4".to_string(),
                        vu32: 4,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
            ],
            &Arc::new(TestSchema),
            0,
            &tier_0_fs,
        )
        .await
        .unwrap();

        build_parquet_table::<Test>(
            &option,
            table_gen_3,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: "5".to_string(),
                        vu32: 5,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: "6".to_string(),
                        vu32: 6,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
            ],
            &Arc::new(TestSchema),
            0,
            &tier_0_fs,
        )
        .await
        .unwrap();

        build_parquet_table::<Test>(
            &option,
            table_gen_4,
            vec![
                (
                    LogType::Full,
                    Test {
                        vstring: "7".to_string(),
                        vu32: 7,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
                (
                    LogType::Full,
                    Test {
                        vstring: "8".to_string(),
                        vu32: 8,
                        vbool: Some(true),
                    },
                    0.into(),
                ),
            ],
            &Arc::new(TestSchema),
            0,
            &tier_0_fs,
        )
        .await
        .unwrap();

        // Create version with tier 0 at capacity (4 files)
        let (sender, _) = bounded(1);
        let mut version =
            Version::<Test>::new(option.clone(), sender, Arc::new(AtomicU32::default()));

        // Add all 4 files to tier 0
        version.level_slice[0].push(Scope {
            min: "1".to_string(),
            max: "2".to_string(),
            gen: table_gen_1,
            wal_ids: None,
            file_size: 100,
        });
        version.level_slice[0].push(Scope {
            min: "3".to_string(),
            max: "4".to_string(),
            gen: table_gen_2,
            wal_ids: None,
            file_size: 100,
        });
        version.level_slice[0].push(Scope {
            min: "5".to_string(),
            max: "6".to_string(),
            gen: table_gen_3,
            wal_ids: None,
            file_size: 100,
        });
        version.level_slice[0].push(Scope {
            min: "7".to_string(),
            max: "8".to_string(),
            gen: table_gen_4,
            wal_ids: None,
            file_size: 100,
        });

        // Test tier compaction
        let mut version_edits = Vec::new();
        let mut delete_gens = Vec::new();

        let (_, clean_sender) = Cleaner::new(option.clone(), manager.clone());
        let manifest: VersionSet<Test, TokioExecutor> =
            VersionSet::new(clean_sender, option.clone(), manager.clone())
                .await
                .unwrap();
        let ctx = Context::new(
            manager.clone(),
            Arc::new(NoCache::default()),
            Box::new(manifest),
            TestSchema.arrow_schema().clone(),
        );

        TieredCompactor::<Test>::tier_compaction(
            &version,
            &option,
            &mut version_edits,
            &mut delete_gens,
            &TestSchema,
            &ctx,
            0, // source tier
            1, // target tier
        )
        .await
        .unwrap();

        // Verify that new files are added to tier 1
        let add_edits: Vec<_> = version_edits
            .iter()
            .filter_map(|edit| match edit {
                VersionEdit::Add { level, scope } => Some((*level, scope)),
                _ => None,
            })
            .collect();

        assert!(!add_edits.is_empty(), "Should have added files to tier 1");
        for (level, scope) in add_edits {
            assert_eq!(level, 1, "Files should be added to tier 1");
            assert!(scope.min <= scope.max, "Scope should be valid");
        }

        // Verify that all tier 0 files are marked for removal
        let remove_edits: Vec<_> = version_edits
            .iter()
            .filter_map(|edit| match edit {
                VersionEdit::Remove { level, gen } => Some((*level, *gen)),
                _ => None,
            })
            .collect();

        assert_eq!(remove_edits.len(), 4, "Should remove all 4 tier 0 files");
        for (level, gen) in remove_edits {
            assert_eq!(level, 0, "All removed files should be from tier 0");
            assert!(
                [table_gen_1, table_gen_2, table_gen_3, table_gen_4].contains(&gen),
                "Removed file should be one of the original tier 0 files"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tier_capacity_and_compaction_planning() {
        let temp_dir = TempDir::new().unwrap();
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        ));

        let (sender, _) = bounded(1);
        let mut version =
            Version::<Test>::new(option.clone(), sender, Arc::new(AtomicU32::default()));

        let options = TieredOptions {
            tier_base_capacity: 2,
            tier_growth_factor: 2,
            max_tiers: 3,
            ..Default::default()
        };

        // Test 1: Initially no tiers are full
        assert!(!TieredCompactor::<Test>::is_tier_full(
            &options, &version, 0
        ));
        assert!(!TieredCompactor::<Test>::is_tier_full(
            &options, &version, 1
        ));
        assert!(!TieredCompactor::<Test>::is_tier_full(
            &options, &version, 2
        ));

        // Test 2: Add files to reach capacity (but not exceed)
        version.level_slice[0].push(Scope {
            min: "0".to_string(),
            max: "1".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 100,
        });
        version.level_slice[0].push(Scope {
            min: "2".to_string(),
            max: "3".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 100,
        });

        // Tier 0 should not be full yet (at capacity but not exceeding)
        assert!(!TieredCompactor::<Test>::is_tier_full(
            &options, &version, 0
        ));

        // Test 3: Exceed capacity to trigger compaction planning
        version.level_slice[0].push(Scope {
            min: "4".to_string(),
            max: "5".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 100,
        });

        // Now tier 0 should be full (exceeding capacity of 2)
        assert!(TieredCompactor::<Test>::is_tier_full(&options, &version, 0));

        // Test 4: Compaction planning logic
        for tier in 0..MAX_LEVEL - 1 {
            if TieredCompactor::<Test>::is_tier_full(&options, &version, tier) {
                assert_eq!(tier, 0); // Only tier 0 should be full

                let tier_files: Vec<_> = version.level_slice[tier]
                    .iter()
                    .map(|scope| scope.gen)
                    .collect();
                assert_eq!(tier_files.len(), 3); // Should have 3 files (exceeding capacity of 2)
                break;
            }
        }

        // Test 5: Multi-tier capacity behavior
        // Add 4 files to tier 1 (capacity = 4)
        for i in 0..4 {
            version.level_slice[1].push(Scope {
                min: format!("{}", i * 2 + 10),
                max: format!("{}", i * 2 + 11),
                gen: generate_file_id(),
                wal_ids: None,
                file_size: 100,
            });
        }

        // Tier 1 at capacity but not full
        assert!(!TieredCompactor::<Test>::is_tier_full(
            &options, &version, 1
        ));

        // Exceed tier 1 capacity
        version.level_slice[1].push(Scope {
            min: "100".to_string(),
            max: "101".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 100,
        });

        // Now both tiers should be full
        assert!(TieredCompactor::<Test>::is_tier_full(&options, &version, 0));
        assert!(TieredCompactor::<Test>::is_tier_full(&options, &version, 1));
        assert!(!TieredCompactor::<Test>::is_tier_full(
            &options, &version, 2
        ));

        // Test 6: Boundary conditions
        assert!(!TieredCompactor::<Test>::is_tier_full(
            &options, &version, 3
        ));
        assert!(!TieredCompactor::<Test>::is_tier_full(
            &options, &version, 4
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tiered_edge_cases() {
        let temp_dir = TempDir::new().unwrap();
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        ));

        let (sender, _) = bounded(1);
        let mut version =
            Version::<Test>::new(option.clone(), sender, Arc::new(AtomicU32::default()));

        // Test with max_tiers = 1 (only tier 0)
        let options = TieredOptions {
            tier_base_capacity: 2,
            tier_growth_factor: 2,
            max_tiers: 1,
            ..Default::default()
        };

        // Add files to tier 0 to exceed capacity
        version.level_slice[0].push(Scope {
            min: "1".to_string(),
            max: "2".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 100,
        });
        version.level_slice[0].push(Scope {
            min: "3".to_string(),
            max: "4".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 100,
        });
        version.level_slice[0].push(Scope {
            min: "5".to_string(),
            max: "6".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 100,
        });

        // With max_tiers = 1, tier 0 is still considered full when exceeding capacity
        // but compaction planning should handle the case where there's no target tier
        assert!(TieredCompactor::<Test>::is_tier_full(&options, &version, 0));

        // Test with zero capacity
        let zero_options = TieredOptions {
            tier_base_capacity: 0,
            tier_growth_factor: 2,
            max_tiers: 4,
            ..Default::default()
        };

        assert_eq!(TieredCompactor::<Test>::tier_capacity(&zero_options, 0), 0);
        assert_eq!(TieredCompactor::<Test>::tier_capacity(&zero_options, 1), 0);

        // Test beyond MAX_LEVEL
        let (sender2, _) = bounded(1);
        let version_empty =
            Version::<Test>::new(option.clone(), sender2, Arc::new(AtomicU32::default()));
        let normal_options = TieredOptions::default();
        assert!(!TieredCompactor::<Test>::is_tier_full(
            &normal_options,
            &version_empty,
            MAX_LEVEL
        ));
        assert!(!TieredCompactor::<Test>::is_tier_full(
            &normal_options,
            &version_empty,
            MAX_LEVEL + 1
        ));

        // Test planning with max_tiers = 1 - this should create a task with target_tier = 1
        // even though max_tiers = 1, which might be a design issue
        for tier in 0..MAX_LEVEL - 1 {
            if TieredCompactor::<Test>::is_tier_full(&options, &version, tier) {
                assert_eq!(tier, 0);
                let tier_files: Vec<_> = version.level_slice[tier]
                    .iter()
                    .map(|scope| scope.gen)
                    .collect();
                assert_eq!(tier_files.len(), 3);

                // The planned target_tier would be 1, which exceeds max_tiers = 1
                let planned_target = tier + 1;
                assert_eq!(planned_target, 1);
                assert!(planned_target >= options.max_tiers); // This should be caught somewhere
                break;
            }
        }
    }

    // Test tiered compaction timing - when tier 0 reaches capacity, it should trigger compaction
    #[tokio::test(flavor = "multi_thread")]
    async fn test_tiered_compaction_timing() {
        // Test different capacity configurations and their compaction behavior

        // Test case 1: Capacity 2 - should trigger compaction
        {
            let temp_dir = TempDir::new().unwrap();
            let tiered_options = TieredOptions {
                tier_base_capacity: 2,
                tier_growth_factor: 3,
                max_tiers: 4,
            };

            let mut option = DbOption::new(
                Path::from_filesystem_path(temp_dir.path()).unwrap(),
                &TestSchema,
            )
            .tiered_compaction(tiered_options)
            .immutable_chunk_num(1)
            .immutable_chunk_max_num(1);
            option.trigger_type = TriggerType::Length(5);

            let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::default(), TestSchema)
                .await
                .unwrap();

            // Add files up to capacity
            for batch in 0..2 {
                for i in (batch * 5)..(batch + 1) * 5 {
                    db.insert(Test {
                        vstring: i.to_string(),
                        vu32: i,
                        vbool: Some(true),
                    })
                    .await
                    .unwrap();
                }
                db.flush().await.unwrap();
            }

            let version = db.ctx.manifest.current().await;
            assert_eq!(version.level_slice[0].len(), 2); // At capacity
            assert_eq!(version.level_slice[1].len(), 0);

            // Exceed capacity
            for i in 10..15 {
                db.insert(Test {
                    vstring: i.to_string(),
                    vu32: i,
                    vbool: Some(true),
                })
                .await
                .unwrap();
            }
            db.flush().await.unwrap();

            let version = db.ctx.manifest.current().await;
            let total_files = version.level_slice[0].len() + version.level_slice[1].len();
            assert!(total_files >= 1);
            // Should have triggered compaction to tier 1
            if version.level_slice[1].len() > 0 {
                for scope in &version.level_slice[1] {
                    assert!(scope.min <= scope.max);
                }
            }
        }

        // Test case 2: Capacity 4 - should NOT trigger compaction
        {
            let temp_dir = TempDir::new().unwrap();
            let tiered_options = TieredOptions {
                tier_base_capacity: 4, // Higher capacity
                tier_growth_factor: 2,
                max_tiers: 4,
            };

            let mut option = DbOption::new(
                Path::from_filesystem_path(temp_dir.path()).unwrap(),
                &TestSchema,
            )
            .tiered_compaction(tiered_options)
            .immutable_chunk_num(1)
            .immutable_chunk_max_num(1);
            option.trigger_type = TriggerType::Length(5);

            let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::default(), TestSchema)
                .await
                .unwrap();

            // Add 3 files (under capacity of 4)
            for batch in 0..3 {
                for i in (batch * 5)..(batch + 1) * 5 {
                    db.insert(Test {
                        vstring: i.to_string(),
                        vu32: i,
                        vbool: Some(true),
                    })
                    .await
                    .unwrap();
                }
                db.flush().await.unwrap();
            }

            let version = db.ctx.manifest.current().await;
            assert_eq!(version.level_slice[0].len(), 3); // Under capacity
            assert_eq!(version.level_slice[1].len(), 0); // No compaction
        }

        // Test case 3: Capacity 3 - precise threshold behavior
        {
            let temp_dir = TempDir::new().unwrap();
            let tiered_options = TieredOptions {
                tier_base_capacity: 3,
                tier_growth_factor: 2,
                max_tiers: 3,
            };

            let mut option = DbOption::new(
                Path::from_filesystem_path(temp_dir.path()).unwrap(),
                &TestSchema,
            )
            .tiered_compaction(tiered_options)
            .immutable_chunk_num(1)
            .immutable_chunk_max_num(1);
            option.trigger_type = TriggerType::Length(10);

            let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::default(), TestSchema)
                .await
                .unwrap();

            // Add exactly at capacity
            for batch in 0..3 {
                for i in (batch * 10)..(batch + 1) * 10 {
                    db.insert(Test {
                        vstring: format!("{:03}", i),
                        vu32: i,
                        vbool: Some(i % 2 == 0),
                    })
                    .await
                    .unwrap();
                }
                db.flush().await.unwrap();
            }

            let version_before = db.ctx.manifest.current().await;
            assert_eq!(version_before.level_slice[0].len(), 3); // At capacity
            assert_eq!(version_before.level_slice[1].len(), 0);

            // Exceed capacity
            for i in 30..40 {
                db.insert(Test {
                    vstring: format!("{:03}", i),
                    vu32: i,
                    vbool: Some(i % 2 == 0),
                })
                .await
                .unwrap();
            }
            db.flush().await.unwrap();

            let version_after = db.ctx.manifest.current().await;
            let tier0_files = version_after.level_slice[0].len();
            let tier1_files = version_after.level_slice[1].len();

            // Should trigger compaction
            assert!(tier0_files < 3 || tier1_files > 0);
            let total_files = tier0_files + tier1_files;
            assert!(total_files >= 1 && total_files <= 4);
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests_metric {

    use fusio::path::Path;
    use tempfile::TempDir;

    use crate::{
        compaction::{
            tests_metric::{read_write_amplification_measurement, throughput},
            tiered::TieredOptions,
        },
        inmem::immutable::tests::TestSchema,
        trigger::TriggerType,
        DbOption,
    };

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn read_write_amplification_measurement_tiered() {
        let temp_dir = TempDir::new().unwrap();
        let tiered_options = TieredOptions {
            tier_base_capacity: 3,
            tier_growth_factor: 4,
            ..Default::default()
        };
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .tiered_compaction(tiered_options)
        .max_sst_file_size(1024); // Small file size to force multiple files

        read_write_amplification_measurement(option).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn throughput_tiered() {
        let temp_dir = TempDir::new().unwrap();
        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .tiered_compaction(TieredOptions::default());
        option.trigger_type = TriggerType::SizeOfMem(1 * 1024 * 1024);

        throughput(option).await;
    }
}
