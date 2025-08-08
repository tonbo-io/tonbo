use std::cmp;
use std::mem;
use std::ops::Bound;
use std::sync::Arc;

use crate::executor::{RwLock, Executor};
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
use crate::stream::level::LevelStream;
use crate::stream::ScanStream;
use crate::version::edit::VersionEdit;
use crate::version::TransactionTs;
use crate::{
    context::Context,
    record::{self, Record},
    version::{Version, MAX_LEVEL},
    CompactionExecutor, DbOption, DbStorage,
};

pub struct LeveledTask {
    pub input: Vec<(usize, Vec<Ulid>)>,
}

pub struct LeveledCompactor<R, E> 
where 
    R: Record,
    <R::Schema as record::Schema>::Columns: Send + Sync,
    E: Executor,
{
    options: LeveledOptions,
    db_option: Arc<DbOption>,
    mem_storage: Arc<E::RwLock<DbStorage<R>>>,
    ctx: Arc<Context<R>>,
    record_schema: Arc<R::Schema>,
}

#[derive(Clone, Debug)]
pub struct LeveledOptions {
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
}

impl Default for LeveledOptions {
    fn default() -> Self {
        Self {
            major_threshold_with_sst_size: 4,
            level_sst_magnification: 10,
            major_default_oldest_table_num: 3,
            major_l_selection_table_max_num: 4,
            immutable_chunk_num: 3,
            immutable_chunk_max_num: 5,
        }
    }
}

impl LeveledOptions {
    /// Set major threshold with SST size
    pub fn major_threshold_with_sst_size(mut self, value: usize) -> Self {
        self.major_threshold_with_sst_size = value;
        self
    }

    /// Set level SST magnification
    pub fn level_sst_magnification(mut self, value: usize) -> Self {
        self.level_sst_magnification = value;
        self
    }

    /// Set major default oldest table number
    pub fn major_default_oldest_table_num(mut self, value: usize) -> Self {
        self.major_default_oldest_table_num = value;
        self
    }
}

impl<R, E> LeveledCompactor<R, E> 
where
    R: Record,
    <R::Schema as record::Schema>::Columns: Send + Sync,
    E: Executor,
{
    pub(crate) fn new(
        options: LeveledOptions,
        mem_storage: Arc<E::RwLock<DbStorage<R>>>,
        record_schema: Arc<R::Schema>,
        db_option: Arc<DbOption>,
        db_option: Arc<DbOption>,
        ctx: Arc<Context<R>>,
    ) -> Self {
        Self {
            options,
            db_option,
            mem_storage,
            ctx,
            record_schema,
        }
    }
}

#[async_trait::async_trait]
impl<R, E> Compactor<R> for LeveledCompactor<R, E>
where
    R: Record,
    E: Executor + Send + Sync,
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

impl<R, E> CompactionExecutor<R> for LeveledCompactor<R, E>
where
    R: Record,
    <R::Schema as record::Schema>::Columns: Send + Sync,
    E: Executor + Send + Sync,
{
    fn check_then_compaction(
        &self,
        is_manual: bool,
    ) -> impl std::future::Future<Output = Result<(), CompactionError<R>>> + Send {
        <Self as Compactor<R>>::check_then_compaction(self, is_manual)
    }
}

impl<R, E> LeveledCompactor<R, E>
where
    R: Record,
    <R::Schema as record::Schema>::Columns: Send + Sync,
    E: Executor + Send + Sync,
{
    pub async fn should_major_compact(&self) -> bool {
        // Check if any level needs major compaction
        let version_ref = self.ctx.manifest.current().await;
        for level in 0..MAX_LEVEL - 2 {
            if Self::is_threshold_exceeded_major(&self.options, &version_ref, level) {
                return true;
            }
        }
        false
    }

    pub async fn plan_major(&self) -> Option<LeveledTask> {
        let version_ref = self.ctx.manifest.current().await;

        // Find the first level that needs compaction
        for level in 0..MAX_LEVEL - 2 {
            if Self::is_threshold_exceeded_major(&self.options, &version_ref, level) {
                // Collect file IDs from the level that needs compaction
                let level_files: Vec<Ulid> = version_ref.level_slice[level]
                    .iter()
                    .map(|scope| scope.gen)
                    .collect();

                if !level_files.is_empty() {
                    let mut input = vec![(level, level_files)];
                    if level + 1 < MAX_LEVEL {
                        let next_level_files: Vec<Ulid> = version_ref.level_slice[level + 1]
                            .iter()
                            .map(|scope| scope.gen)
                            .collect();

                        if !next_level_files.is_empty() {
                            input.push((level + 1, next_level_files));
                        }
                    }
                    return Some(LeveledTask { input });
                }
            }
        }
        None
    }

    pub async fn execute_major(
        &self,
        task: LeveledTask,
    ) -> Result<(), CompactionError<R>> {
        let version_ref = self.ctx.manifest.current().await;
        let mut version_edits = vec![];
        let mut delete_gens = vec![];

        // Extract the level from the task
        for (level, file_gens) in &task.input {
            if file_gens.is_empty() {
                continue;
            }

            // Get the scopes for the files to be compacted
            let level_scopes: Vec<&Scope<_>> = version_ref.level_slice[*level]
                .iter()
                .filter(|scope| file_gens.contains(&scope.gen))
                .collect();

            if level_scopes.is_empty() {
                continue;
            }

            // Determine min/max range for compaction
            let min = level_scopes.iter().map(|scope| &scope.min).min().unwrap();
            let max = level_scopes.iter().map(|scope| &scope.max).max().unwrap();
            // Execute the actual compaction logic
            Self::major_compaction(
                &version_ref,
                &self.db_option,
                &self.options,
                &min,
                &max,
                &mut version_edits,
                &mut delete_gens,
                &self.record_schema,
                &self.ctx,
                task.input[0].0,
            )
            .await?;

            break; // Process one level at a time
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
    ) -> Result<Option<LeveledTask>, CompactionError<R>> {
        let mut guard = self.mem_storage.write().await;

        guard.trigger.reset();

        // Add the mutable memtable into the immutable memtable
        if !guard.mutable.is_empty() {
            let trigger_clone = guard.trigger.clone();

            // Replace mutable memtable with new memtable
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

            let guard = self.mem_storage.read().await;
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

                self.ctx
                    .manifest
                    .update(version_edits, None)
                    .await?;
            }
            drop(guard);
            let mut guard = self.mem_storage.write().await;
            let sources = guard.immutables.split_off(chunk_num);
            let _ = mem::replace(&mut guard.immutables, sources);
        }
        Ok(None)
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

    // Accumulate all SST files in a stream that fall within the min/max range in `level` and `level
    // + 1`. Then use those files to build the new SST files and delete the olds ones
    #[allow(clippy::too_many_arguments)]
    async fn major_compaction_impl(
        version: &Version<R>,
        option: &DbOption,
        leveled_options: &LeveledOptions,
        leveled_options: &LeveledOptions,
        mut min: &<R::Schema as RecordSchema>::Key,
        mut max: &<R::Schema as RecordSchema>::Key,
        version_edits: &mut Vec<VersionEdit<<R::Schema as RecordSchema>::Key>>,
        delete_gens: &mut Vec<SsTableID>,
        instance: &R::Schema,
        ctx: &Context<R>,
        target_level: usize,
        target_level: usize,
    ) -> Result<(), CompactionError<R>> {
        let level = target_level;

        let (meet_scopes_l, start_l, end_l) = Self::this_level_scopes(version, min, max, level, leveled_options);
        let (meet_scopes_ll, start_ll, end_ll) =
            Self::next_level_scopes(version, &mut min, &mut max, level, &meet_scopes_l)?;

        let level_path = option.level_fs_path(level).unwrap_or(&option.base_path);
        let level_fs = ctx.manager.get_fs(level_path);
        let mut streams = Vec::with_capacity(meet_scopes_l.len() + meet_scopes_ll.len());
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
                            None
                        )
                        .await?,
                });
            }
        } else {
            let (lower, upper) = <LeveledCompactor<R, E> as Compactor<R>>::full_scope(&meet_scopes_l)?;
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
                None
            )
            .ok_or(CompactionError::EmptyLevel)?;

            streams.push(ScanStream::Level {
                inner: level_scan_l,
            });
        }
            streams.push(ScanStream::Level {
                inner: level_scan_l,
            });
        }

        let level_l_path = option.level_fs_path(level + 1).unwrap_or(&option.base_path);
        let level_l_fs = ctx.manager.get_fs(level_l_path);
        let level_l_path = option.level_fs_path(level + 1).unwrap_or(&option.base_path);
        let level_l_fs = ctx.manager.get_fs(level_l_path);

        // Pushes next level SSTs that fall in the range
        if !meet_scopes_ll.is_empty() {
            let (lower, upper) =
                <LeveledCompactor<R, E> as Compactor<R>>::full_scope(&meet_scopes_ll)?;
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
                None
            )
            .ok_or(CompactionError::EmptyLevel)?;

            streams.push(ScanStream::Level {
                inner: level_scan_ll,
            });
        }
            streams.push(ScanStream::Level {
                inner: level_scan_ll,
            });
        }

        // Build the new SSTs
        <LeveledCompactor<R, E> as Compactor<R>>::build_tables(
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

        Ok(())
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
        options: &LeveledOptions,
        options: &LeveledOptions,
    ) -> (
        Vec<&'a Scope<<R::Schema as RecordSchema>::Key>>,
        usize,
        usize,
    ) {
        let mut meet_scopes_l = Vec::new();
        let mut start_l = Version::<R>::scope_search(min, &version.level_slice[level]);
        let mut end_l = start_l;

        for scope in version.level_slice[level][start_l..].iter() {
            if (scope.contains(min) || scope.contains(max))
                && meet_scopes_l.len() <= options.major_l_selection_table_max_num
            {
                meet_scopes_l.push(scope);
                end_l += 1;
            } else {
                break;
            }
        }

        for scope in version.level_slice[level][start_l..].iter() {
            if (scope.contains(min) || scope.contains(max))
                && meet_scopes_l.len() <= options.major_l_selection_table_max_num
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
                options.major_default_oldest_table_num,
                options.major_default_oldest_table_num,
                version.level_slice[level].len(),
            );

            for scope in version.level_slice[level][..end_l].iter() {
                if meet_scopes_l.len() > options.major_l_selection_table_max_num {
                if meet_scopes_l.len() > options.major_l_selection_table_max_num {
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
        options: &LeveledOptions,
        options: &LeveledOptions,
        version: &Version<R>,
        level: usize,
    ) -> bool {
        Version::<R>::tables_len(version, level)
            >= (options.major_threshold_with_sst_size
                * options.level_sst_magnification.pow(level as u32))
            >= (options.major_threshold_with_sst_size
                * options.level_sst_magnification.pow(level as u32))
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
    use arrow::datatypes::DataType as ArrayDataType;

    use crate::{
        compaction::{
            error::CompactionError, leveled::{LeveledCompactor, LeveledOptions}, tests::{build_parquet_table, build_version}
        }, context::Context, executor::tokio::TokioExecutor, fs::{generate_file_id, manager::StoreManager}, inmem::{
            immutable::{tests::TestSchema, ImmutableMemTable},
            mutable::MutableMemTable,
        }, record::{self, DataType, DynRecord, DynSchema, DynamicField, Record, Schema, Value, ValueRef}, scope::Scope, tests::Test, trigger::{TriggerFactory, TriggerType}, version::{cleaner::Cleaner, edit::VersionEdit, set::VersionSet, timestamp::Timestamp, Version, MAX_LEVEL}, wal::log::LogType, CompactionExecutor, Compactor, DbError, DbOption, DB
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

        let scope = LeveledCompactor::<Test, TokioExecutor>::minor_compaction(
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

        let scope = LeveledCompactor::<DynRecord, TokioExecutor>::minor_compaction(
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
        option = option.leveled_compaction(LeveledOptions {
            major_threshold_with_sst_size: 2,
            ..Default::default()
        });
        option = option.leveled_compaction(LeveledOptions {
            major_threshold_with_sst_size: 2,
            ..Default::default()
        });
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
            VersionSet::<Test,  crate::executor::tokio::TokioExecutor>::new(clean_sender, option.clone(), manager.clone())
                .await
                .unwrap(),
        );
        let ctx = Context::new(
            manager.clone(),
            Arc::new(NoCache::default()),
            manifest,
            TestSchema.arrow_schema().clone(),
        );

        let leveled_options = LeveledOptions {
            major_threshold_with_sst_size: 2,
            ..Default::default()
        };
        LeveledCompactor::<Test, TokioExecutor>::major_compaction(
            &version,
            &option,
            &leveled_options,
            &leveled_options,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
            &TestSchema,
            &ctx,
            0,
            0,
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

        let option = DbOption::new(
        let option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .leveled_compaction(LeveledOptions {
            major_threshold_with_sst_size: 1,
            level_sst_magnification: 1,
            ..Default::default()
        });
        )
        .leveled_compaction(LeveledOptions {
            major_threshold_with_sst_size: 1,
            level_sst_magnification: 1,
            ..Default::default()
        });
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
            VersionSet::<Test, crate::executor::tokio::TokioExecutor>::new(clean_sender, option.clone(), manager.clone())
                .await
                .unwrap(),
        );
        let ctx = Context::new(
            manager.clone(),
            Arc::new(NoCache::default()),
            manifest,
            TestSchema.arrow_schema().clone(),
        );
        let leveled_options = LeveledOptions {
            major_threshold_with_sst_size: 1,
            level_sst_magnification: 1,
            ..Default::default()
        };
        LeveledCompactor::<Test, TokioExecutor>::major_compaction(
            &version,
            &option,
            &leveled_options,
            &leveled_options,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
            &TestSchema,
            &ctx,
            0,
            0,
        )
        .await
        .unwrap();
    }

    // issue: https://github.com/tonbo-io/tonbo/issues/152
    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_major_level_sort() {
        let temp_dir = TempDir::new().unwrap();
        eprintln!("test");
        eprintln!("test");
        let mut option = DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &TestSchema,
        )
        .immutable_chunk_num(1)
        .immutable_chunk_max_num(0)
        .leveled_compaction(LeveledOptions {
            major_threshold_with_sst_size: 2,
            level_sst_magnification: 1,
            major_default_oldest_table_num: 1,
            ..Default::default()
        })
        .max_sst_file_size(2 * 1024 * 1024);
        )
        .immutable_chunk_num(1)
        .immutable_chunk_max_num(0)
        .leveled_compaction(LeveledOptions {
            major_threshold_with_sst_size: 2,
            level_sst_magnification: 1,
            major_default_oldest_table_num: 1,
            ..Default::default()
        })
        .max_sst_file_size(2 * 1024 * 1024);
        option.trigger_type = TriggerType::Length(5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::default(), TestSchema)
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
}
