use std::{cmp, collections::Bound, mem, pin::Pin, sync::Arc};

use async_lock::RwLock;
use futures_util::StreamExt;
use object_store::{buffered::BufWriter, ObjectStore};
use parquet::arrow::{AsyncArrowWriter, ProjectionMask};
use thiserror::Error;
use ulid::Ulid;

use crate::{
    fs::{
        build_reader,
        store_manager::{StoreManager, StoreManagerError},
        FileId, FileProvider,
    },
    inmem::{
        immutable::{ArrowArrays, Builder, Immutable},
        mutable::Mutable,
    },
    ondisk::sstable::SsTable,
    record::{KeyRef, Record},
    scope::Scope,
    stream::{level::LevelStream, merge::MergeStream, ScanStream},
    version::{edit::VersionEdit, set::VersionSet, Version, VersionError, MAX_LEVEL},
    DbOption, Schema,
};

#[derive(Debug)]
pub(crate) enum CompactTask {
    Freeze,
}

pub(crate) struct Compactor<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) option: Arc<DbOption<R>>,
    pub(crate) schema: Arc<RwLock<Schema<R, FP>>>,
    pub(crate) store_manager: Arc<StoreManager>,
    pub(crate) version_set: VersionSet<R, FP>,
}

impl<R, FP> Compactor<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) fn new(
        schema: Arc<RwLock<Schema<R, FP>>>,
        option: Arc<DbOption<R>>,
        store_manager: Arc<StoreManager>,
        version_set: VersionSet<R, FP>,
    ) -> Self {
        Compactor::<R, FP> {
            option,
            schema,
            store_manager,
            version_set,
        }
    }

    pub(crate) async fn check_then_compaction(
        &mut self,
        // TODO
        // option_tx: Option<oneshot::Sender<()>>,
    ) -> Result<(), CompactionError<R>> {
        let mut guard = self.schema.write().await;

        guard.trigger.reset();

        if guard.mutable.is_empty() {
            return Ok(());
        }

        let trigger_clone = guard.trigger.clone();
        let mutable = mem::replace(
            &mut guard.mutable,
            Mutable::new(&self.option, trigger_clone).await?,
        );
        let (file_id, immutable) = mutable.into_immutable().await?;

        guard.immutables.push((file_id, immutable));
        if guard.immutables.len() > self.option.immutable_chunk_max_num {
            let recover_wal_ids = guard.recover_wal_ids.take();
            let chunk_num = self.option.immutable_chunk_num;
            let excess = &guard.immutables[0..chunk_num];

            if let Some(scope) =
                Self::minor_compaction(&self.option, &self.store_manager, recover_wal_ids, excess)
                    .await?
            {
                let version_ref = self.version_set.current().await;
                let mut version_edits = vec![];
                let mut delete_gens = vec![];

                if self.option.is_threshold_exceeded_major(&version_ref, 0) {
                    Self::major_compaction(
                        &version_ref,
                        &self.option,
                        &self.store_manager,
                        &scope.min,
                        &scope.max,
                        &mut version_edits,
                        &mut delete_gens,
                    )
                    .await?;
                }
                version_edits.insert(0, VersionEdit::Add { level: 0, scope });
                version_edits.push(VersionEdit::LatestTimeStamp {
                    ts: version_ref.transaction_ts(),
                });

                self.version_set
                    .apply_edits(version_edits, Some(delete_gens), false)
                    .await?;
            }
            let sources = guard.immutables.split_off(chunk_num);
            let _ = mem::replace(&mut guard.immutables, sources);
        }
        // TODO
        // if let Some(tx) = option_tx {
        //     let _ = tx.send(());
        // }
        Ok(())
    }

    pub(crate) async fn minor_compaction(
        option: &DbOption<R>,
        store_manager: &Arc<StoreManager>,
        recover_wal_ids: Option<Vec<FileId>>,
        batches: &[(Option<FileId>, Immutable<R::Columns>)],
    ) -> Result<Option<Scope<R::Key>>, CompactionError<R>> {
        if !batches.is_empty() {
            let mut min = None;
            let mut max = None;

            let gen = FileId::new();
            let mut wal_ids = Vec::with_capacity(batches.len());

            let level_0_store = option.level_store(0, store_manager)?.clone();
            let mut writer = AsyncArrowWriter::try_new(
                BufWriter::new(level_0_store, option.table_path(&gen)),
                R::arrow_schema().clone(),
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

    pub(crate) async fn major_compaction(
        version: &Version<R, FP>,
        option: &DbOption<R>,
        store_manager: &Arc<StoreManager>,
        mut min: &R::Key,
        mut max: &R::Key,
        version_edits: &mut Vec<VersionEdit<R::Key>>,
        delete_gens: &mut Vec<(FileId, usize)>,
    ) -> Result<(), CompactionError<R>> {
        let mut level = 0;

        while level < MAX_LEVEL - 2 {
            if !option.is_threshold_exceeded_major(version, level) {
                break;
            }
            let (meet_scopes_l, start_l, end_l) = Self::this_level_scopes(version, min, max, level);
            let (meet_scopes_ll, start_ll, end_ll) =
                Self::next_level_scopes(version, &mut min, &mut max, level, &meet_scopes_l)?;

            let mut streams = Vec::with_capacity(meet_scopes_l.len() + meet_scopes_ll.len());
            // This Level
            if level == 0 {
                for scope in meet_scopes_l.iter() {
                    let store = option.level_store(level, store_manager)?;
                    let reader = build_reader(store.clone(), option.table_path(&scope.gen)).await?;

                    streams.push(ScanStream::SsTable {
                        inner: SsTable::open(reader)
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
                let store = option.level_store(level, store_manager)?;
                let level_scan_l = LevelStream::new(
                    version,
                    store.clone(),
                    level,
                    start_l,
                    end_l,
                    (Bound::Included(lower), Bound::Included(upper)),
                    u32::MAX.into(),
                    None,
                    ProjectionMask::all(),
                )
                .ok_or(CompactionError::EmptyLevel)?;

                streams.push(ScanStream::Level {
                    inner: level_scan_l,
                });
            }
            let store = option.level_store(level + 1, store_manager)?;

            if !meet_scopes_ll.is_empty() {
                // Next Level
                let (lower, upper) = Self::full_scope(&meet_scopes_ll)?;
                let level_scan_ll = LevelStream::new(
                    version,
                    store.clone(),
                    level + 1,
                    start_ll,
                    end_ll,
                    (Bound::Included(lower), Bound::Included(upper)),
                    u32::MAX.into(),
                    None,
                    ProjectionMask::all(),
                )
                .ok_or(CompactionError::EmptyLevel)?;

                streams.push(ScanStream::Level {
                    inner: level_scan_ll,
                });
            }
            Self::build_tables(option, store, version_edits, level, streams).await?;

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
        version: &'a Version<R, FP>,
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

            start_ll = Version::<R, FP>::scope_search(min, &version.level_slice[level + 1]);
            end_ll = Version::<R, FP>::scope_search(max, &version.level_slice[level + 1]);

            let next_level_len = version.level_slice[level + 1].len();
            for scope in version.level_slice[level + 1]
                [start_ll..cmp::min(end_ll + 1, next_level_len - 1)]
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
        version: &'a Version<R, FP>,
        min: &<R as Record>::Key,
        max: &<R as Record>::Key,
        level: usize,
    ) -> (Vec<&'a Scope<<R as Record>::Key>>, usize, usize) {
        let mut meet_scopes_l = Vec::new();
        let mut start_l = Version::<R, FP>::scope_search(min, &version.level_slice[level]);
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
                version.level_slice.len(),
            );

            for scope in version.level_slice[level][..end_l].iter() {
                if meet_scopes_l.len() > option.major_l_selection_table_max_num {
                    break;
                }
                meet_scopes_l.push(scope);
            }
        }
        (meet_scopes_l, start_l, end_l)
    }

    async fn build_tables<'scan>(
        option: &DbOption<R>,
        store: &Arc<dyn ObjectStore>,
        version_edits: &mut Vec<VersionEdit<<R as Record>::Key>>,
        level: usize,
        streams: Vec<ScanStream<'scan, R>>,
    ) -> Result<(), CompactionError<R>> {
        let mut stream = MergeStream::<R>::from_vec(streams, u32::MAX.into()).await?;

        // Kould: is the capacity parameter necessary?
        let mut builder = R::Columns::builder(8192);
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
                    store.clone(),
                    version_edits,
                    level,
                    &mut builder,
                    &mut min,
                    &mut max,
                )
                .await?;
            }
        }
        if builder.written_size() > 0 {
            Self::build_table(
                option,
                store.clone(),
                version_edits,
                level,
                &mut builder,
                &mut min,
                &mut max,
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

    async fn build_table(
        option: &DbOption<R>,
        store: Arc<dyn ObjectStore>,
        version_edits: &mut Vec<VersionEdit<R::Key>>,
        level: usize,
        builder: &mut <R::Columns as ArrowArrays>::Builder,
        min: &mut Option<R::Key>,
        max: &mut Option<R::Key>,
    ) -> Result<(), CompactionError<R>> {
        debug_assert!(min.is_some());
        debug_assert!(max.is_some());

        let gen = Ulid::new();
        let columns = builder.finish(None);
        let mut writer = AsyncArrowWriter::try_new(
            BufWriter::new(store, option.table_path(&gen)),
            R::arrow_schema().clone(),
            Some(option.write_parquet_properties.clone()),
        )?;
        writer.write(columns.as_record_batch()).await?;
        writer.close().await?;
        version_edits.push(VersionEdit::Add {
            level: (level + 1) as u8,
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
    #[error("compaction object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("compaction store manager error: {0}")]
    StoreManagerError(#[from] StoreManagerError),
    #[error("compaction version error: {0}")]
    Version(#[from] VersionError<R>),
    #[error("the level being compacted does not have a table")]
    EmptyLevel,
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        str::FromStr,
        sync::{atomic::AtomicU32, Arc},
    };

    use flume::bounded;
    use object_store::{buffered::BufWriter, ObjectStore};
    use parquet::arrow::AsyncArrowWriter;
    use tempfile::TempDir;
    use url::Url;

    use crate::{
        compaction::Compactor,
        executor::{tokio::TokioExecutor, Executor},
        fs::{store_manager::StoreManager, FileId, FileProvider},
        inmem::{immutable::Immutable, mutable::Mutable},
        record::Record,
        scope::Scope,
        tests::Test,
        timestamp::Timestamp,
        trigger::TriggerFactory,
        version::{edit::VersionEdit, Version, MAX_LEVEL},
        wal::log::LogType,
        DbError, DbOption,
    };

    async fn build_immutable<R, FP>(
        option: &DbOption<R>,
        records: Vec<(LogType, R, Timestamp)>,
    ) -> Result<Immutable<R::Columns>, DbError<R>>
    where
        R: Record + Send,
        FP: FileProvider,
    {
        let trigger = Arc::new(TriggerFactory::create(option.trigger_type));

        let mutable: Mutable<R, FP> = Mutable::new(option, trigger).await?;

        for (log_ty, record, ts) in records {
            let _ = mutable.insert(log_ty, record, ts).await?;
        }
        Ok(Immutable::from(mutable.data))
    }

    pub(crate) async fn build_parquet_table<R, FP>(
        option: &DbOption<R>,
        store: Arc<dyn ObjectStore>,
        gen: FileId,
        records: Vec<(LogType, R, Timestamp)>,
    ) -> Result<(), DbError<R>>
    where
        R: Record + Send,
        FP: Executor,
    {
        let immutable = build_immutable::<R, FP>(option, records).await?;
        let mut writer = AsyncArrowWriter::try_new(
            BufWriter::new(store, option.table_path(&gen)),
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
        let path_buf = temp_dir.path().to_path_buf();
        let table_root_url = Url::from_str("memory:").unwrap();
        let option = DbOption::build(path_buf, table_root_url.clone());
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();
        let store_manager = Arc::new(StoreManager::new(&vec![table_root_url; MAX_LEVEL]).unwrap());

        let batch_1 = build_immutable::<Test, TokioExecutor>(
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
        )
        .await
        .unwrap();

        let batch_2 = build_immutable::<Test, TokioExecutor>(
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
        )
        .await
        .unwrap();

        let scope = Compactor::<Test, TokioExecutor>::minor_compaction(
            &option,
            &store_manager,
            None,
            &vec![
                (Some(FileId::new()), batch_1),
                (Some(FileId::new()), batch_2),
            ],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(scope.min, 1.to_string());
        assert_eq!(scope.max, 6.to_string());
    }

    #[tokio::test]
    async fn major_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let table_root_url = Url::from_str("memory:").unwrap();

        let mut option = DbOption::build(&temp_dir.path(), table_root_url);
        option.major_threshold_with_sst_size = 2;
        let option = Arc::new(option);
        let store_manager = Arc::new(StoreManager::new(&option.table_urls).unwrap());

        let ((table_gen_1, table_gen_2, table_gen_3, table_gen_4, _), version) =
            build_version(&option, &store_manager).await;

        let min = 2.to_string();
        let max = 5.to_string();
        let mut version_edits = Vec::new();

        Compactor::<Test, TokioExecutor>::major_compaction(
            &version,
            &option,
            &store_manager,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
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
        store_manager: &Arc<StoreManager>,
    ) -> (
        (FileId, FileId, FileId, FileId, FileId),
        Version<Test, TokioExecutor>,
    ) {
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();
        let level_0_store = option.level_store(0, store_manager).unwrap();

        // level 0
        let table_gen_1 = FileId::new();
        let table_gen_2 = FileId::new();
        build_parquet_table::<Test, TokioExecutor>(
            option,
            level_0_store.clone(),
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
        )
        .await
        .unwrap();
        build_parquet_table::<Test, TokioExecutor>(
            option,
            level_0_store.clone(),
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
        )
        .await
        .unwrap();

        let level_1_store = option.level_store(1, store_manager).unwrap();

        // level 1
        let table_gen_3 = FileId::new();
        let table_gen_4 = FileId::new();
        let table_gen_5 = FileId::new();
        build_parquet_table::<Test, TokioExecutor>(
            option,
            level_1_store.clone(),
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
        )
        .await
        .unwrap();
        build_parquet_table::<Test, TokioExecutor>(
            option,
            level_1_store.clone(),
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
        )
        .await
        .unwrap();
        build_parquet_table::<Test, TokioExecutor>(
            option,
            level_1_store.clone(),
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
        )
        .await
        .unwrap();

        let (sender, _) = bounded(1);
        let mut version = Version::<Test, TokioExecutor>::new(
            option.clone(),
            sender,
            Arc::new(AtomicU32::default()),
        );
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
