use std::{cmp, collections::Bound, mem, pin::Pin, sync::Arc};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use fusio::DynFs;
use fusio_parquet::writer::AsyncWriter;
use futures_util::StreamExt;
use parquet::arrow::{AsyncArrowWriter, ProjectionMask};
use thiserror::Error;
use tokio::sync::oneshot;

use crate::{
    fs::{generate_file_id, manager::StoreManager, FileId, FileType},
    inmem::{
        immutable::{ArrowArrays, Builder, Immutable},
        mutable::Mutable,
    },
    ondisk::sstable::SsTable,
    record::{KeyRef, Record, RecordInstance},
    scope::Scope,
    stream::{level::LevelStream, merge::MergeStream, ScanStream},
    transaction::CommitError,
    version::{
        edit::VersionEdit, set::VersionSet, TransactionTs, Version, VersionError, MAX_LEVEL,
    },
    DbOption, ParquetLru, Schema,
};

#[derive(Debug)]
pub enum CompactTask {
    Freeze,
    Flush(Option<oneshot::Sender<()>>),
}

pub(crate) struct Compactor<R>
where
    R: Record,
{
    pub(crate) option: Arc<DbOption<R>>,
    pub(crate) schema: Arc<RwLock<Schema<R>>>,
    pub(crate) version_set: VersionSet<R>,
    pub(crate) manager: Arc<StoreManager>,
    pub(crate) instance: Arc<RecordInstance>,
}

impl<R> Compactor<R>
where
    R: Record,
{
    pub(crate) fn new(
        schema: Arc<RwLock<Schema<R>>>,
        option: Arc<DbOption<R>>,
        version_set: VersionSet<R>,
        manager: Arc<StoreManager>,
        instance: Arc<RecordInstance>
    ) -> Self {
        Compactor::<R> {
            option,
            schema,
            version_set,
            manager,
            instance,
        }
    }

    pub(crate) async fn check_then_compaction(
        &mut self,
        parquet_lru: ParquetLru,
        is_manual: bool,
    ) -> Result<(), CompactionError<R>> {
        let mut guard = self.schema.write().await;

        guard.trigger.reset();

        if !guard.mutable.is_empty() {
            let trigger_clone = guard.trigger.clone();

            let mutable = mem::replace(
                &mut guard.mutable,
                Mutable::new(&self.option, trigger_clone, self.manager.base_fs()).await?,
            );
            let (file_id, immutable) = mutable.into_immutable(&guard.record_instance).await?;
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
                &self.instance,
                &self.manager,
            )
            .await?
            {
                let version_ref = self.version_set.current().await;
                let mut version_edits = vec![];
                let mut delete_gens = vec![];

                if self.option.is_threshold_exceeded_major(&version_ref, 0) {
                    Self::major_compaction(
                        &version_ref,
                        &self.option,
                        &scope.min,
                        &scope.max,
                        &mut version_edits,
                        &mut delete_gens,
                        &self.instance,
                        &self.manager,
                        parquet_lru,
                    )
                    .await?;
                }
                version_edits.insert(0, VersionEdit::Add { level: 0, scope });
                version_edits.push(VersionEdit::LatestTimeStamp {
                    ts: version_ref.increase_ts(),
                });

                self.version_set
                    .apply_edits(version_edits, Some(delete_gens), false)
                    .await?;
            }
            let mut guard = RwLockUpgradableReadGuard::upgrade(guard).await;
            let sources = guard.immutables.split_off(chunk_num);
            let _ = mem::replace(&mut guard.immutables, sources);
        }
        Ok(())
    }

    pub(crate) async fn minor_compaction(
        option: &DbOption<R>,
        recover_wal_ids: Option<Vec<FileId>>,
        batches: &[(Option<FileId>, Immutable<R::Columns>)],
        instance: &RecordInstance,
        manager: &StoreManager,
    ) -> Result<Option<Scope<R::Key>>, CompactionError<R>> {
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
                instance.arrow_schema::<R>().clone(),
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
    pub(crate) async fn major_compaction(
        version: &Version<R>,
        option: &DbOption<R>,
        mut min: &R::Key,
        mut max: &R::Key,
        version_edits: &mut Vec<VersionEdit<R::Key>>,
        delete_gens: &mut Vec<(FileId, usize)>,
        instance: &RecordInstance,
        manager: &StoreManager,
        parquet_lru: ParquetLru,
    ) -> Result<(), CompactionError<R>> {
        let mut level = 0;

        while level < MAX_LEVEL - 2 {
            if !option.is_threshold_exceeded_major(version, level) {
                break;
            }
            let (meet_scopes_l, start_l, end_l) = Self::this_level_scopes(version, min, max, level);
            let (meet_scopes_ll, start_ll, end_ll) =
                Self::next_level_scopes(version, &mut min, &mut max, level, &meet_scopes_l)?;

            let level_path = option.level_fs_path(level).unwrap_or(&option.base_path);
            let level_fs = manager.get_fs(level_path);
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
                        inner: SsTable::open(parquet_lru.clone(), scope.gen, file)
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
                let (lower, upper) = Self::full_scope(&meet_scopes_l)?;
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
                    parquet_lru.clone(),
                )
                .ok_or(CompactionError::EmptyLevel)?;

                streams.push(ScanStream::Level {
                    inner: level_scan_l,
                });
            }
            if !meet_scopes_ll.is_empty() {
                // Next Level
                let (lower, upper) = Self::full_scope(&meet_scopes_ll)?;
                let level_scan_ll = LevelStream::new(
                    version,
                    level + 1,
                    start_ll,
                    end_ll,
                    (Bound::Included(lower), Bound::Included(upper)),
                    u32::MAX.into(),
                    None,
                    ProjectionMask::all(),
                    level_fs.clone(),
                    parquet_lru.clone(),
                )
                .ok_or(CompactionError::EmptyLevel)?;

                streams.push(ScanStream::Level {
                    inner: level_scan_ll,
                });
            }

            let level_l_path = option.level_fs_path(level + 1).unwrap_or(&option.base_path);
            let level_l_fs = manager.get_fs(level_l_path);
            Self::build_tables(
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
        min: &mut &'a <R as Record>::Key,
        max: &mut &'a <R as Record>::Key,
        level: usize,
        meet_scopes_l: &[&'a Scope<<R as Record>::Key>],
    ) -> Result<(Vec<&'a Scope<<R as Record>::Key>>, usize, usize), CompactionError<R>> {
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

    fn this_level_scopes<'a>(
        version: &'a Version<R>,
        min: &<R as Record>::Key,
        max: &<R as Record>::Key,
        level: usize,
    ) -> (Vec<&'a Scope<<R as Record>::Key>>, usize, usize) {
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

    async fn build_tables<'scan>(
        option: &DbOption<R>,
        version_edits: &mut Vec<VersionEdit<<R as Record>::Key>>,
        level: usize,
        streams: Vec<ScanStream<'scan, R>>,
        instance: &RecordInstance,
        fs: &Arc<dyn DynFs>,
    ) -> Result<(), CompactionError<R>> {
        let mut stream = MergeStream::<R>::from_vec(streams, u32::MAX.into()).await?;

        // Kould: is the capacity parameter necessary?
        let mut builder = R::Columns::builder(&instance.arrow_schema::<R>(), 8192);
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
                    instance,
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
                instance,
                fs,
            )
            .await?;
        }
        Ok(())
    }

    fn full_scope<'a>(
        meet_scopes: &[&'a Scope<<R as Record>::Key>],
    ) -> Result<(&'a <R as Record>::Key, &'a <R as Record>::Key), CompactionError<R>> {
        let lower = &meet_scopes.first().ok_or(CompactionError::EmptyLevel)?.min;
        let upper = &meet_scopes.last().ok_or(CompactionError::EmptyLevel)?.max;
        Ok((lower, upper))
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_table(
        option: &DbOption<R>,
        version_edits: &mut Vec<VersionEdit<R::Key>>,
        level: usize,
        builder: &mut <R::Columns as ArrowArrays>::Builder,
        min: &mut Option<R::Key>,
        max: &mut Option<R::Key>,
        instance: &RecordInstance,
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
            instance.arrow_schema::<R>().clone(),
            Some(option.write_parquet_properties.clone()),
        )?;
        writer.write(columns.as_record_batch()).await?;
        writer.close().await?;
        version_edits.push(VersionEdit::Add {
            level: level as u8,
            scope: Scope {
                min: min.take().ok_or(CompactionError::EmptyLevel)?,
                max: max.take().ok_or(CompactionError::EmptyLevel)?,
                gen,
                wal_ids: None,
            },
        });
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
    use fusio::{path::Path, DynFs};
    use fusio_dispatch::FsOptions;
    use fusio_parquet::writer::AsyncWriter;
    use parquet::arrow::AsyncArrowWriter;
    use parquet_lru::NoCache;
    use tempfile::TempDir;

    use crate::{
        compaction::Compactor,
        executor::tokio::TokioExecutor,
        fs::{generate_file_id, manager::StoreManager, FileId, FileType},
        inmem::{immutable::Immutable, mutable::Mutable},
        record::{Column, ColumnDesc, Datatype, DynRecord, Record, RecordInstance},
        scope::Scope,
        tests::Test,
        timestamp::Timestamp,
        trigger::{TriggerFactory, TriggerType},
        version::{edit::VersionEdit, Version, MAX_LEVEL},
        wal::log::LogType,
        DbError, DbOption, DB,
    };

    async fn build_immutable<R>(
        option: &DbOption<R>,
        records: Vec<(LogType, R, Timestamp)>,
        instance: &RecordInstance,
        fs: &Arc<dyn DynFs>,
    ) -> Result<Immutable<R::Columns>, DbError<R>>
    where
        R: Record + Send,
    {
        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let mutable: Mutable<R> = Mutable::new(option, trigger, fs).await?;

        for (log_ty, record, ts) in records {
            let _ = mutable.insert(log_ty, record, ts).await?;
        }
        Ok(Immutable::from((mutable.data, instance)))
    }

    pub(crate) async fn build_parquet_table<R>(
        option: &DbOption<R>,
        gen: FileId,
        records: Vec<(LogType, R, Timestamp)>,
        instance: &RecordInstance,
        level: usize,
        fs: &Arc<dyn DynFs>,
    ) -> Result<(), DbError<R>>
    where
        R: Record + Send,
    {
        let immutable = build_immutable::<R>(option, records, instance, fs).await?;
        let mut writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(
                fs.open_options(
                    &option.table_path(gen, level),
                    FileType::Parquet.open_options(false),
                )
                .await?,
            ),
            R::arrow_schema().clone(),
            None,
        )?;
        writer.write(immutable.as_record_batch()).await?;
        writer.close().await?;

        Ok(())
    }

    #[tokio::test]
    async fn minor_compaction() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_dir_l0 = tempfile::tempdir().unwrap();

        let option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap())
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
            &RecordInstance::Normal,
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
            &RecordInstance::Normal,
            manager.base_fs(),
        )
        .await
        .unwrap();

        let scope = Compactor::<Test>::minor_compaction(
            &option,
            None,
            &vec![
                (Some(generate_file_id()), batch_1),
                (Some(generate_file_id()), batch_2),
            ],
            &RecordInstance::Normal,
            &manager,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(scope.min, 1.to_string());
        assert_eq!(scope.max, 6.to_string());
    }

    #[tokio::test]
    async fn dyn_minor_compaction() {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = StoreManager::new(FsOptions::Local, vec![]).unwrap();
        let option = DbOption::with_path(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            "id".to_string(),
            0,
        );
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let empty_record = DynRecord::empty_record(
            vec![ColumnDesc::new("id".to_owned(), Datatype::Int32, false)],
            0,
        );
        let instance = RecordInstance::Runtime(empty_record);

        let mut batch1_data = vec![];
        let mut batch2_data = vec![];
        for i in 0..40 {
            let col = Column::new(Datatype::Int32, "id".to_owned(), Arc::new(i), false);
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

        let scope = Compactor::<DynRecord>::minor_compaction(
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
        assert_eq!(
            scope.min,
            Column::new(Datatype::Int32, "id".to_owned(), Arc::new(2), false)
        );
        assert_eq!(
            scope.max,
            Column::new(Datatype::Int32, "id".to_owned(), Arc::new(39), false)
        );
    }

    #[tokio::test]
    async fn major_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let temp_dir_l0 = TempDir::new().unwrap();
        let temp_dir_l1 = TempDir::new().unwrap();

        let mut option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap())
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
        let manager =
            StoreManager::new(option.base_fs.clone(), option.level_paths.clone()).unwrap();

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
            build_version(&option, &manager).await;

        let min = 2.to_string();
        let max = 5.to_string();
        let mut version_edits = Vec::new();

        Compactor::<Test>::major_compaction(
            &version,
            &option,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
            &RecordInstance::Normal,
            &manager,
            Arc::new(NoCache::default()),
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

    pub(crate) async fn build_version(
        option: &Arc<DbOption<Test>>,
        manager: &StoreManager,
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
            &RecordInstance::Normal,
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
            &RecordInstance::Normal,
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
            &RecordInstance::Normal,
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
            &RecordInstance::Normal,
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
            &RecordInstance::Normal,
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

    // https://github.com/tonbo-io/tonbo/pull/139
    #[tokio::test]
    pub(crate) async fn major_panic() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap());
        option.major_threshold_with_sst_size = 1;
        option.level_sst_magnification = 1;
        let manager =
            StoreManager::new(option.base_fs.clone(), option.level_paths.clone()).unwrap();

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
            &RecordInstance::Normal,
            0,
            level_0_fs,
        )
        .await
        .unwrap();
        build_parquet_table::<Test>(
            &option,
            table_gen1,
            records1,
            &RecordInstance::Normal,
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
        });
        version.level_slice[1].push(Scope {
            min: 5.to_string(),
            max: 9.to_string(),
            gen: table_gen1,
            wal_ids: None,
        });

        let mut version_edits = Vec::new();
        let min = 6.to_string();
        let max = 9.to_string();

        Compactor::<Test>::major_compaction(
            &version,
            &option,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
            &RecordInstance::Normal,
            &manager,
            Arc::new(NoCache::default()),
        )
        .await
        .unwrap();
    }

    // issue: https://github.com/tonbo-io/tonbo/issues/152
    #[tokio::test]
    async fn test_flush_major_level_sort() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::from(Path::from_filesystem_path(temp_dir.path()).unwrap());
        option.immutable_chunk_num = 1;
        option.immutable_chunk_max_num = 0;
        option.major_threshold_with_sst_size = 2;
        option.level_sst_magnification = 1;

        option.max_sst_file_size = 2 * 1024 * 1024;
        option.major_default_oldest_table_num = 1;
        option.trigger_type = TriggerType::Length(5);

        let db: DB<Test, TokioExecutor> = DB::new(option, TokioExecutor::new()).await.unwrap();

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

        let version = db.version_set.current().await;

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
