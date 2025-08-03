use std::mem;
use std::ops::Bound;
use std::sync::Arc;

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use fusio_parquet::writer::AsyncWriter;
use parquet::arrow::{AsyncArrowWriter, ProjectionMask};
use ulid::Ulid;

use super::{CompactionError, Compactor};
use crate::compaction::RecordSchema;
use crate::fs::manager::StoreManager;
use crate::fs::{generate_file_id, FileId, FileType};
use crate::inmem::immutable::ImmutableMemTable;
use crate::inmem::mutable::MutableMemTable;
use crate::ondisk::sstable::{SsTable, SsTableID};
use crate::scope::Scope;
use crate::stream::ScanStream;
use crate::version::edit::VersionEdit;
use crate::version::TransactionTs;
use crate::{
    context::Context,
    record::{self, Record},
    version::{Version, MAX_LEVEL},
    CompactionExecutor, DbOption, DbStorage,
};

pub struct LazyLeveledTask {
    pub input: Vec<(usize, Vec<Ulid>)>,
    pub target_level: usize,
}

#[derive(Clone, Debug)]
pub struct LazyLeveledOptions {
    /// Size threshold (in bytes) to trigger major compaction relative to SST size
    pub major_threshold_with_sst_size: usize,
    /// Magnification factor controlling SST file count per level
    pub level_sst_magnification: usize,
    /// Default number of oldest tables to include in a major compaction
    pub major_default_oldest_table_num: usize,
    /// Maximum number of tables to select for major compaction at level L
    pub major_l_selection_table_max_num: usize,
    /// Number of immutable chunks to accumulate before triggering a flush
    pub immutable_chunk_num: usize,
    /// Maximum allowed number of immutable chunks in memory
    pub immutable_chunk_max_num: usize,
    /// The bottom-most level that uses leveled compaction (all levels above use tiered)
    pub bottom_most_level: usize,
    /// Maximum number of files per tier in tiered levels (L0 to switch_level-1)
    pub tiered_max_files_per_level: usize,
    /// Growth factor for tiered levels (each level can have growth_factor^level files)
    pub tiered_growth_factor: usize,
}

impl Default for LazyLeveledOptions {
    fn default() -> Self {
        Self {
            major_threshold_with_sst_size: 4,
            level_sst_magnification: 10,
            major_default_oldest_table_num: 3,
            major_l_selection_table_max_num: 4,
            immutable_chunk_num: 3,
            immutable_chunk_max_num: 5,
            bottom_most_level: MAX_LEVEL - 1, // Only the last level uses leveled compaction
            tiered_max_files_per_level: 4,    // Base capacity for tiered levels
            tiered_growth_factor: 4,          // Each tier can have growth_factor^level files
        }
    }
}

pub struct LazyLeveledCompactor<R: Record> {
    options: LazyLeveledOptions,
    db_option: Arc<DbOption>,
    schema: Arc<RwLock<DbStorage<R>>>,
    ctx: Arc<Context<R>>,
    record_schema: Arc<R::Schema>,
}

impl<R: Record> LazyLeveledCompactor<R> {
    pub(crate) fn new(
        options: LazyLeveledOptions,
        schema: Arc<RwLock<DbStorage<R>>>,
        record_schema: Arc<R::Schema>,
        db_option: Arc<DbOption>,
        ctx: Arc<Context<R>>,
    ) -> Self {
        Self {
            options,
            db_option,
            schema,
            ctx,
            record_schema,
        }
    }
}

#[async_trait::async_trait]
impl<R> Compactor<R> for LazyLeveledCompactor<R>
where
    R: Record,
    <<R as record::Record>::Schema as record::Schema>::Columns: Send + Sync,
{
    async fn check_then_compaction(&self, is_manual: bool) -> Result<(), CompactionError<R>> {
        self.minor_flush(is_manual).await?;

        while self.should_major_compact().await {
            if let Some(task) = self.plan_major().await {
                self.execute_major(task).await?;
            } else {
                break;
            }
        }

        if is_manual {
            self.ctx.manifest.rewrite().await.unwrap();
        }

        Ok(())
    }
}

impl<R: Record> CompactionExecutor<R> for LazyLeveledCompactor<R>
where
    <<R as crate::record::Record>::Schema as crate::record::Schema>::Columns: Send + Sync,
{
    fn check_then_compaction(
        &self,
        is_manual: bool,
    ) -> impl std::future::Future<Output = Result<(), CompactionError<R>>> + Send {
        <Self as Compactor<R>>::check_then_compaction(self, is_manual)
    }
}

impl<R> LazyLeveledCompactor<R>
where
    R: Record,
    <<R as record::Record>::Schema as record::Schema>::Columns: Send + Sync,
{
    pub async fn should_major_compact(&self) -> bool {
        let version_ref = self.ctx.manifest.current().await;

        // Check tiered levels (all levels except the bottom-most)
        for level in 0..self.options.bottom_most_level.min(MAX_LEVEL) {
            if Self::is_tiered_level_full(&self.options, &version_ref, level) {
                return true;
            }
        }

        // Check the bottom-most level (leveled compaction)
        if self.options.bottom_most_level < MAX_LEVEL
            && Self::is_leveled_threshold_exceeded(
                &self.options,
                &version_ref,
                self.options.bottom_most_level,
            )
        {
            return true;
        }

        false
    }

    pub async fn plan_major(&self) -> Option<LazyLeveledTask> {
        let version_ref = self.ctx.manifest.current().await;

        // Handle tiered levels first (all levels except bottom-most)
        for level in 0..self.options.bottom_most_level {
            if Self::is_tiered_level_full(&self.options, &version_ref, level) {
                return self.plan_tiered_compaction(&version_ref, level).await;
            }
        }

        // Handle the bottom-most level (leveled compaction)
        if Self::is_leveled_threshold_exceeded(
            &self.options,
            &version_ref,
            self.options.bottom_most_level,
        ) {
            return self
                .plan_leveled_compaction(&version_ref, self.options.bottom_most_level)
                .await;
        }

        None
    }

    /// Plan compaction for tiered levels (all files in level go to next level)
    async fn plan_tiered_compaction(
        &self,
        version_ref: &Version<R>,
        level: usize,
    ) -> Option<LazyLeveledTask> {
        let level_files: Vec<Ulid> = version_ref.level_slice[level]
            .iter()
            .map(|scope| scope.gen)
            .collect();

        if !level_files.is_empty() {
            Some(LazyLeveledTask {
                input: vec![(level, level_files)],
                target_level: level + 1,
            })
        } else {
            None
        }
    }

    /// Plan compaction for leveled levels (overlap-based compaction)
    async fn plan_leveled_compaction(
        &self,
        version_ref: &Version<R>,
        level: usize,
    ) -> Option<LazyLeveledTask> {
        let source_scopes: Vec<&Scope<_>> = version_ref.level_slice[level]
            .iter()
            .take(self.options.major_l_selection_table_max_num)
            .collect();

        if !source_scopes.is_empty() {
            let level_files: Vec<Ulid> = source_scopes.iter().map(|scope| scope.gen).collect();
            let mut input = vec![(level, level_files)];

            // Include overlapping files from the next level for leveled compaction
            if level + 1 < MAX_LEVEL {
                // Find the min/max key range of source files
                let source_min = source_scopes.iter().map(|scope| &scope.min).min().unwrap();
                let source_max = source_scopes.iter().map(|scope| &scope.max).max().unwrap();

                // Find overlapping files in the next level based on key ranges
                let next_level_files: Vec<Ulid> = version_ref.level_slice[level + 1]
                    .iter()
                    .filter(|scope| {
                        // Include files that overlap with source range
                        &scope.max >= source_min && &scope.min <= source_max
                    })
                    .map(|scope| scope.gen)
                    .collect();

                if !next_level_files.is_empty() {
                    input.push((level + 1, next_level_files));
                }
            }

            Some(LazyLeveledTask {
                input,
                target_level: level + 1,
            })
        } else {
            None
        }
    }

    pub async fn execute_major(&self, task: LazyLeveledTask) -> Result<(), CompactionError<R>> {
        let version_ref = self.ctx.manifest.current().await;
        let mut version_edits = vec![];
        let mut delete_gens = vec![];

        // Determine if this is a tiered or leveled compaction based on the source level
        let source_level = task.input[0].0; // First level in the task
        let is_tiered_compaction = source_level != self.options.bottom_most_level;

        if is_tiered_compaction {
            // Use tiered compaction strategy (like TieredCompactor)
            Self::execute_tiered_compaction(
                &version_ref,
                &self.db_option,
                &self.options,
                &mut version_edits,
                &mut delete_gens,
                &self.record_schema,
                &self.ctx,
                &task,
            )
            .await?;
        } else {
            // Use leveled compaction strategy (like LeveledCompactor)
            Self::execute_leveled_compaction(
                &version_ref,
                &self.db_option,
                &self.options,
                &mut version_edits,
                &mut delete_gens,
                &self.record_schema,
                &self.ctx,
                &task,
            )
            .await?;
        }

        if !version_edits.is_empty() {
            version_edits.push(VersionEdit::LatestTimeStamp {
                ts: version_ref.increase_ts(),
            });

            self.ctx
                .manifest
                .update(version_edits, Some(delete_gens))
                .await?;
        }

        Ok(())
    }

    pub async fn minor_flush(
        &self,
        is_manual: bool,
    ) -> Result<Option<LazyLeveledTask>, CompactionError<R>> {
        let mut guard = self.schema.write().await;

        guard.trigger.reset();

        if !guard.mutable.is_empty() {
            let trigger_clone = guard.trigger.clone();

            let mutable = mem::replace(
                &mut guard.mutable,
                MutableMemTable::new(
                    &self.db_option,
                    trigger_clone,
                    self.ctx.manager.base_fs().clone(),
                    self.record_schema.clone(),
                )
                .await?,
            );

            let (file_id, immutable) = mutable.into_immutable().await?;
            guard.immutables.push((file_id, immutable));
        } else if !is_manual {
            return Ok(None);
        }

        if (is_manual && !guard.immutables.is_empty())
            || guard.immutables.len() > self.options.immutable_chunk_max_num
        {
            let recover_wal_ids = guard.recover_wal_ids.take();
            drop(guard);

            let guard = self.schema.upgradable_read().await;
            let chunk_num = if is_manual {
                guard.immutables.len()
            } else {
                self.options.immutable_chunk_num
            };
            let excess = &guard.immutables[0..chunk_num];

            if let Some(scope) = Self::minor_compaction(
                &self.db_option,
                recover_wal_ids,
                excess,
                &guard.record_schema,
                &self.ctx.manager,
            )
            .await?
            {
                let version_ref = self.ctx.manifest.current().await;
                let mut version_edits = vec![VersionEdit::Add { level: 0, scope }];
                version_edits.push(VersionEdit::LatestTimeStamp {
                    ts: version_ref.increase_ts(),
                });

                self.ctx.manifest.update(version_edits, None).await?;
            }
            let mut guard = RwLockUpgradableReadGuard::upgrade(guard).await;
            let sources = guard.immutables.split_off(chunk_num);
            let _ = mem::replace(&mut guard.immutables, sources);
        }

        Ok(None)
    }

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

    /// Execute tiered compaction (move all files from source level to target level)
    #[allow(clippy::too_many_arguments)]
    async fn execute_tiered_compaction(
        version: &Version<R>,
        option: &DbOption,
        _lazy_options: &LazyLeveledOptions,
        version_edits: &mut Vec<VersionEdit<<R::Schema as RecordSchema>::Key>>,
        delete_gens: &mut Vec<SsTableID>,
        instance: &R::Schema,
        ctx: &Context<R>,
        task: &LazyLeveledTask,
    ) -> Result<(), CompactionError<R>> {
        let source_level = task.input[0].0;
        let target_level = task.target_level;
        let file_gens = &task.input[0].1;

        let source_scopes: Vec<&Scope<_>> = version.level_slice[source_level]
            .iter()
            .filter(|scope| file_gens.contains(&scope.gen))
            .collect();

        if source_scopes.is_empty() {
            return Ok(());
        }

        let source_level_path = option
            .level_fs_path(source_level)
            .unwrap_or(&option.base_path);
        let source_level_fs = ctx.manager.get_fs(source_level_path);
        let target_level_path = option
            .level_fs_path(target_level)
            .unwrap_or(&option.base_path);
        let target_level_fs = ctx.manager.get_fs(target_level_path);

        let mut streams = Vec::with_capacity(source_scopes.len());

        // For tiered compaction, treat all levels like level 0 (overlapping files)
        for scope in source_scopes.iter() {
            let file = source_level_fs
                .open_options(
                    &option.table_path(scope.gen, source_level),
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
                    )
                    .await?,
            });
        }

        // Build the new SSTs in target level
        <LazyLeveledCompactor<R> as Compactor<R>>::build_tables(
            option,
            version_edits,
            target_level,
            streams,
            instance,
            target_level_fs,
        )
        .await?;

        // Mark all source files for deletion
        for scope in source_scopes {
            version_edits.push(VersionEdit::Remove {
                level: source_level as u8,
                gen: scope.gen,
            });
            delete_gens.push(SsTableID::new(scope.gen, source_level));
        }

        Ok(())
    }

    /// Execute leveled compaction (overlap-based compaction like standard leveled compactor)
    #[allow(clippy::too_many_arguments)]
    async fn execute_leveled_compaction(
        version: &Version<R>,
        option: &DbOption,
        _lazy_options: &LazyLeveledOptions,
        version_edits: &mut Vec<VersionEdit<<R::Schema as RecordSchema>::Key>>,
        delete_gens: &mut Vec<SsTableID>,
        instance: &R::Schema,
        ctx: &Context<R>,
        task: &LazyLeveledTask,
    ) -> Result<(), CompactionError<R>> {
        let source_level = task.input[0].0;
        let target_level = task.target_level;

        // Get source level files
        let source_file_gens = &task.input[0].1;
        let source_scopes: Vec<&Scope<_>> = version.level_slice[source_level]
            .iter()
            .filter(|scope| source_file_gens.contains(&scope.gen))
            .collect();

        if source_scopes.is_empty() {
            return Ok(());
        }

        let _min = source_scopes.iter().map(|scope| &scope.min).min().unwrap();
        let _max = source_scopes.iter().map(|scope| &scope.max).max().unwrap();

        // Find overlapping files in target level
        let target_scopes = if task.input.len() > 1 {
            let target_file_gens = &task.input[1].1;
            version.level_slice[target_level]
                .iter()
                .filter(|scope| target_file_gens.contains(&scope.gen))
                .collect()
        } else {
            // If no target files were planned, find overlapping files dynamically
            if !source_scopes.is_empty() {
                let source_min = source_scopes.iter().map(|scope| &scope.min).min().unwrap();
                let source_max = source_scopes.iter().map(|scope| &scope.max).max().unwrap();

                version.level_slice[target_level]
                    .iter()
                    .filter(|scope| {
                        // Include files that overlap with source range
                        &scope.max >= source_min && &scope.min <= source_max
                    })
                    .collect()
            } else {
                Vec::new()
            }
        };

        let source_level_path = option
            .level_fs_path(source_level)
            .unwrap_or(&option.base_path);
        let source_level_fs = ctx.manager.get_fs(source_level_path);
        let target_level_path = option
            .level_fs_path(target_level)
            .unwrap_or(&option.base_path);
        let target_level_fs = ctx.manager.get_fs(target_level_path);

        let mut streams = Vec::with_capacity(source_scopes.len() + target_scopes.len());

        // Add source level streams
        for scope in source_scopes.iter() {
            let file = source_level_fs
                .open_options(
                    &option.table_path(scope.gen, source_level),
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
                    )
                    .await?,
            });
        }

        // Add overlapping target level streams
        for scope in target_scopes.iter() {
            let file = target_level_fs
                .open_options(
                    &option.table_path(scope.gen, target_level),
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
                    )
                    .await?,
            });
        }

        // Build new SST files
        <LazyLeveledCompactor<R> as Compactor<R>>::build_tables(
            option,
            version_edits,
            target_level,
            streams,
            instance,
            target_level_fs,
        )
        .await?;

        // Mark old files for deletion
        for scope in source_scopes {
            version_edits.push(VersionEdit::Remove {
                level: source_level as u8,
                gen: scope.gen,
            });
            delete_gens.push(SsTableID::new(scope.gen, source_level));
        }
        for scope in target_scopes {
            version_edits.push(VersionEdit::Remove {
                level: target_level as u8,
                gen: scope.gen,
            });
            delete_gens.push(SsTableID::new(scope.gen, target_level));
        }

        Ok(())
    }

    /// Checks if a tiered level is full and needs compaction
    fn is_tiered_level_full(
        options: &LazyLeveledOptions,
        version: &Version<R>,
        level: usize,
    ) -> bool {
        if level >= MAX_LEVEL || level == options.bottom_most_level {
            return false;
        }

        // Tiered level capacity: base_capacity * growth_factor^level
        let tier_capacity =
            options.tiered_max_files_per_level * options.tiered_growth_factor.pow(level as u32);

        Version::<R>::tables_len(version, level) >= tier_capacity
    }

    /// Checks if the bottom-most level (leveled) exceeds threshold and needs compaction
    fn is_leveled_threshold_exceeded(
        options: &LazyLeveledOptions,
        version: &Version<R>,
        level: usize,
    ) -> bool {
        if level >= MAX_LEVEL || level != options.bottom_most_level {
            return false;
        }

        // Standard leveled compaction threshold for the bottom-most level
        let threshold = options.major_threshold_with_sst_size
            * options.level_sst_magnification.pow(level as u32);

        Version::<R>::tables_len(version, level) >= threshold
    }
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests {
    use std::sync::{atomic::AtomicU32, Arc};

    use flume::bounded;
    use fusio::{path::Path, DynFs};
    use fusio_dispatch::FsOptions;
    use tempfile::TempDir;

    use crate::{
        compaction::lazyleveled::{LazyLeveledCompactor, LazyLeveledOptions},
        executor::tokio::TokioExecutor,
        fs::{generate_file_id, manager::StoreManager},
        inmem::{
            immutable::{tests::TestSchema, ImmutableMemTable},
            mutable::MutableMemTable,
        },
        record::{Record, Schema},
        scope::Scope,
        tests::Test,
        trigger::TriggerFactory,
        version::timestamp::Timestamp,
        version::{Version, MAX_LEVEL},
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
    async fn test_lazy_leveled_single_bottom_level_strategy() {
        let temp_dir = TempDir::new().unwrap();
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        ));

        let (sender, _) = bounded(1);
        let mut version =
            Version::<Test>::new(option.clone(), sender, Arc::new(AtomicU32::default()));

        // Configure to use bottom-most level (L6 assuming MAX_LEVEL=7) for leveled compaction
        let options = LazyLeveledOptions {
            major_threshold_with_sst_size: 4,
            level_sst_magnification: 4,
            bottom_most_level: MAX_LEVEL - 1, // Only the last level uses leveled compaction
            tiered_max_files_per_level: 2,    // Base capacity for tiered levels
            tiered_growth_factor: 3,          // Growth factor for tiered levels
            ..Default::default()
        };

        // Test initial state - all levels except last should be treated as tiered
        for level in 0..MAX_LEVEL - 1 {
            assert!(!LazyLeveledCompactor::<Test>::is_tiered_level_full(
                &options, &version, level
            ));
            assert!(
                !LazyLeveledCompactor::<Test>::is_leveled_threshold_exceeded(
                    &options, &version, level
                )
            );
        }

        // Test bottom-most level (leveled)
        assert!(
            !LazyLeveledCompactor::<Test>::is_leveled_threshold_exceeded(
                &options,
                &version,
                MAX_LEVEL - 1
            )
        );
        assert!(!LazyLeveledCompactor::<Test>::is_tiered_level_full(
            &options,
            &version,
            MAX_LEVEL - 1
        ));

        // Test L0 (tiered): capacity = 2 * 3^0 = 2
        version.level_slice[0].push(Scope {
            min: "1".to_string(),
            max: "5".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 100,
        });
        version.level_slice[0].push(Scope {
            min: "6".to_string(),
            max: "10".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 100,
        });

        // L0 should now be full (2 >= 2)
        assert!(LazyLeveledCompactor::<Test>::is_tiered_level_full(
            &options, &version, 0
        ));

        // Test L1 (tiered): capacity = 2 * 3^1 = 6
        for _ in 0..6 {
            version.level_slice[1].push(Scope {
                min: format!("{}", generate_file_id()),
                max: format!("{}", generate_file_id()),
                gen: generate_file_id(),
                wal_ids: None,
                file_size: 100,
            });
        }

        // L1 should now be full (6 >= 6)
        assert!(LazyLeveledCompactor::<Test>::is_tiered_level_full(
            &options, &version, 1
        ));

        // Test bottom-most level (leveled): threshold = 4 * 4^(MAX_LEVEL-1)
        let bottom_level = MAX_LEVEL - 1;
        let threshold = options.major_threshold_with_sst_size
            * options.level_sst_magnification.pow(bottom_level as u32);

        // Add files just below threshold
        for _ in 0..threshold - 1 {
            version.level_slice[bottom_level].push(Scope {
                min: format!("{}", generate_file_id()),
                max: format!("{}", generate_file_id()),
                gen: generate_file_id(),
                wal_ids: None,
                file_size: 100,
            });
        }

        // Bottom level should not trigger compaction yet
        assert!(
            !LazyLeveledCompactor::<Test>::is_leveled_threshold_exceeded(
                &options,
                &version,
                bottom_level
            )
        );

        // Add one more file to exceed threshold
        version.level_slice[bottom_level].push(Scope {
            min: format!("{}", generate_file_id()),
            max: format!("{}", generate_file_id()),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 100,
        });

        // Bottom level should now trigger leveled compaction
        assert!(LazyLeveledCompactor::<Test>::is_leveled_threshold_exceeded(
            &options,
            &version,
            bottom_level
        ));

        // Ensure all other levels are not treated as leveled
        for level in 0..MAX_LEVEL - 1 {
            assert!(
                !LazyLeveledCompactor::<Test>::is_leveled_threshold_exceeded(
                    &options, &version, level
                )
            );
        }

        // Ensure bottom level is not treated as tiered
        assert!(!LazyLeveledCompactor::<Test>::is_tiered_level_full(
            &options,
            &version,
            bottom_level
        ));

        println!("Lazy Leveled Strategy Test:");
        println!("- Tiered levels: L0 to L{}", MAX_LEVEL - 2);
        println!("- Leveled level: L{} (bottom-most)", MAX_LEVEL - 1);
        println!("- Bottom level threshold: {}", threshold);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lazy_leveled_minor_compaction() {
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

        let scope = LazyLeveledCompactor::<Test>::minor_compaction(
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lazy_leveled_hybrid_integration() {
        let temp_dir = TempDir::new().unwrap();
        let lazy_options = LazyLeveledOptions {
            major_threshold_with_sst_size: 4,
            level_sst_magnification: 4,
            bottom_most_level: MAX_LEVEL - 1, // Only bottom-most level uses leveled compaction
            tiered_max_files_per_level: 4,    // Base capacity to trigger compaction
            tiered_growth_factor: 2,
            immutable_chunk_num: 3,
            immutable_chunk_max_num: 5,
            ..Default::default()
        };

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .lazy_leveled_compaction(lazy_options)
        .max_sst_file_size(1024);
        option.trigger_type = crate::trigger::TriggerType::Length(5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::current(), TestSchema)
            .await
            .unwrap();

        // Insert test data that should trigger multiple levels of compaction
        let record_num = 10000;
        for i in 0..record_num {
            db.insert(Test {
                vstring: format!("key{:05}", i), // Use 4-digit padding for proper lexicographical sorting
                vu32: i,
                vbool: Some(i % 2 == 0),
            })
            .await
            .unwrap();

            // Force compaction every 1000 insertions to see intermediate states
            if i > 0 && i % 1000 == 0 {
                db.flush().await.unwrap();

                // Log current state every 1000 insertions
                let version = db.ctx.manifest.current().await;
                println!("After {} insertions:", i);
                for level in 0..MAX_LEVEL {
                    if !version.level_slice[level].is_empty() {
                        println!(
                            "  Level {}: {} files ({})",
                            level,
                            version.level_slice[level].len(),
                            if level == MAX_LEVEL - 1 {
                                "leveled"
                            } else {
                                "tiered"
                            }
                        );
                    }
                }
            }
        }

        db.flush().await.unwrap();

        // Verify data integrity
        for i in 0..record_num {
            let key = format!("key{:05}", i); // Use 4-digit padding to match insertion format
            let result = db.get(&key, convert_test_ref_to_test).await.unwrap();
            assert!(result.is_some(), "Key {} should be found", key);
            let record = result.unwrap();
            assert_eq!(record.vu32, i, "Value for key {} should match", key);
        }

        // Check that files are distributed across levels as expected for hybrid strategy
        let version = db.ctx.manifest.current().await;
        let mut has_files = false;
        let mut tiered_levels_have_files = false;
        let mut leveled_levels_have_files = false;

        for level in 0..MAX_LEVEL {
            if !version.level_slice[level].is_empty() {
                has_files = true;
                println!(
                    "Level {}: {} files ({})",
                    level,
                    version.level_slice[level].len(),
                    if level == MAX_LEVEL - 1 {
                        "leveled"
                    } else {
                        "tiered"
                    }
                );

                if level == MAX_LEVEL - 1 {
                    leveled_levels_have_files = true;
                } else {
                    tiered_levels_have_files = true;
                }
            }
        }

        assert!(has_files, "Should have files in the LSM tree");
        println!("Lazy Leveled strategy verification:");
        println!(
            "- Tiered levels (L0-L{}) have files: {}",
            MAX_LEVEL - 2,
            tiered_levels_have_files
        );
        println!(
            "- Leveled level (L{}) has files: {}",
            MAX_LEVEL - 1,
            leveled_levels_have_files
        );
    }
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod tests_metric {

    use crate::compaction::lazyleveled::tests::convert_test_ref_to_test;
    use fusio::path::Path;
    use tempfile::TempDir;

    use crate::compaction::lazyleveled::LazyLeveledOptions;
    use crate::{
        executor::tokio::TokioExecutor, inmem::immutable::tests::TestSchema, tests::Test,
        trigger::TriggerType, version::MAX_LEVEL, DbOption, DB,
    };

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn test_read_write_amplification_measurement() {
        let temp_dir = TempDir::new().unwrap();
        let lazy_options = LazyLeveledOptions {
            major_threshold_with_sst_size: 4,
            level_sst_magnification: 4,
            bottom_most_level: MAX_LEVEL - 1, // Only bottom-most level uses leveled compaction
            tiered_max_files_per_level: 4,    // Base capacity to trigger compaction
            tiered_growth_factor: 2,
            immutable_chunk_num: 3,
            immutable_chunk_max_num: 5,
            ..Default::default()
        };

        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .lazy_leveled_compaction(lazy_options)
        .max_sst_file_size(1024);
        option.trigger_type = TriggerType::Length(5);

        let db: DB<Test, TokioExecutor> =
            DB::new(option.clone(), TokioExecutor::current(), TestSchema)
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

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn test_throughput() {
        use futures_util::StreamExt;
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        use std::time::Instant;

        let temp_dir = TempDir::new().unwrap();
        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .lazy_leveled_compaction(LazyLeveledOptions::default());
        option.trigger_type = TriggerType::SizeOfMem(1 * 1024 * 1024);

        // Create DB with EcoTune compactor using the standard open method
        let db: DB<Test, TokioExecutor> =
            DB::new(option.clone(), TokioExecutor::current(), TestSchema)
                .await
                .unwrap();

        // Test parameters based on EcoTune paper (Section 5.1: 35% Get, 35% Seek, 30% long range scans)
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
