use std::{cmp, mem, pin::Pin, sync::Arc};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use futures_util::StreamExt;
use parquet::arrow::AsyncArrowWriter;
use states::CompactionStates;
use task::CompactionTask;
use thiserror::Error;
use tokio::sync::oneshot;
use ulid::Ulid;

use crate::{
    fs::{FileId, FileProvider},
    inmem::{
        immutable::{ArrowArrays, Builder, Immutable},
        mutable::Mutable,
    },
    record::{KeyRef, Record},
    scope::Scope,
    stream::{merge::MergeStream, ScanStream},
    transaction::CommitError,
    version::{edit::VersionEdit, set::VersionSet, TransactionTs, Version, VersionError},
    DbOption, Schema,
};

mod states;
mod task;

#[derive(Debug)]
pub enum CompactTask {
    Freeze,
    Flush(Option<oneshot::Sender<()>>),
}

pub(crate) struct Compactor<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) option: Arc<DbOption<R>>,
    pub(crate) schema: Arc<RwLock<Schema<R, FP>>>,
    pub(crate) version_set: VersionSet<R, FP>,
    pub(crate) states: Arc<CompactionStates<R>>,
}

impl<R, FP> Compactor<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) fn new(
        schema: Arc<RwLock<Schema<R, FP>>>,
        option: Arc<DbOption<R>>,
        version_set: VersionSet<R, FP>,
    ) -> Self {
        Compactor::<R, FP> {
            states: Arc::new(CompactionStates::new(option.clone())),
            option,
            schema,
            version_set,
        }
    }

    pub(crate) async fn check_then_compaction(
        &mut self,
        option_tx: Option<oneshot::Sender<()>>,
    ) -> Result<(), CompactionError<R>> {
        let mut guard = self.schema.write().await;

        guard.trigger.reset();
        let mut immutables_len = guard.immutables.len();
        let mut recover_wal_ids = None;

        if !guard.mutable.is_empty() {
            let trigger_clone = guard.trigger.clone();
            let mutable = mem::replace(
                &mut guard.mutable,
                Mutable::new(&self.option, trigger_clone).await?,
            );
            let (file_id, immutable) = mutable.into_immutable().await?;
            guard.immutables.push((file_id, immutable));
            immutables_len += 1;
            if immutables_len > self.option.immutable_chunk_max_num {
                recover_wal_ids = guard.recover_wal_ids.take();
            }
        }

        drop(guard);

        if immutables_len > self.option.immutable_chunk_max_num {
            let guard = self.schema.upgradable_read().await;
            let chunk_num = self.option.immutable_chunk_num;
            let excess = &guard.immutables[0..chunk_num];

            if let Some(scope) =
                Self::minor_compaction(&self.option, recover_wal_ids, excess).await?
            {
                let version_ref = self.version_set.current().await;
                let mut version_edits = vec![];

                if self.option.is_threshold_exceeded_major(&version_ref, 0) {
                    let tasks = CompactionTask::generate_major_tasks(
                        &version_ref,
                        &self.option,
                        &scope.min,
                        &scope.max,
                    )
                    .await?;
                    self.states.add_task_batch(tasks).await;
                }
                version_edits.insert(0, VersionEdit::Add { level: 0, scope });
                version_edits.push(VersionEdit::LatestTimeStamp {
                    ts: version_ref.increase_ts(),
                });

                self.version_set
                    .apply_edits(version_edits, None, false)
                    .await?;
            }
            let mut guard = RwLockUpgradableReadGuard::upgrade(guard).await;
            let sources = guard.immutables.split_off(chunk_num);
            let _ = mem::replace(&mut guard.immutables, sources);
        }

        if let Some(task) = self.states.consume_task().await {
            let mut compaction_status = task.compact::<R, FP>(&self.states).await?;
            let version_ref = self.version_set.current().await;
            compaction_status
                .version_edits
                .push(VersionEdit::LatestTimeStamp {
                    ts: version_ref.increase_ts(),
                });
            self.version_set
                .apply_edits(
                    compaction_status.version_edits,
                    compaction_status.delete_gens,
                    false,
                )
                .await?;
            task.finish(&self.states).await;
        }

        if let Some(tx) = option_tx {
            tx.send(()).map_err(|_| CommitError::ChannelClose)?
        }
        Ok(())
    }

    pub(crate) async fn minor_compaction(
        option: &DbOption<R>,
        recover_wal_ids: Option<Vec<FileId>>,
        batches: &[(Option<FileId>, Immutable<R::Columns>)],
    ) -> Result<Option<Scope<R::Key>>, CompactionError<R>> {
        if !batches.is_empty() {
            let mut min = None;
            let mut max = None;

            let gen = FileId::new();
            let mut wal_ids = Vec::with_capacity(batches.len());

            let mut writer = AsyncArrowWriter::try_new(
                FP::open(option.table_path(&gen)).await?,
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

    /// for test
    #[allow(unused)]
    pub(crate) async fn major_compaction(
        states: Arc<CompactionStates<R>>,
        version: &Version<R, FP>,
        option: &DbOption<R>,
        min: &R::Key,
        max: &R::Key,
        version_edits: &mut Vec<VersionEdit<R::Key>>,
        delete_gens: &mut Vec<FileId>,
    ) -> Result<(), CompactionError<R>> {
        let tasks = CompactionTask::generate_major_tasks(version, option, min, max).await?;
        for task in tasks {
            let mut status = task.compact::<R, FP>(&states).await?;
            version_edits.append(&mut status.version_edits);
            if let Some(mut gens) = status.delete_gens {
                delete_gens.append(&mut gens);
            }
        }
        Ok(())
    }

    fn next_level_scopes<'a>(
        version: &'a Version<R, FP>,
        min: &mut &'a <R as Record>::Key,
        max: &mut &'a <R as Record>::Key,
        level: usize,
        meet_scopes_l: &[&'a Scope<<R as Record>::Key>],
    ) -> Result<Vec<&'a Scope<<R as Record>::Key>>, CompactionError<R>> {
        let mut meet_scopes_ll = Vec::new();

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

            let start_ll = Version::<R, FP>::scope_search(min, &version.level_slice[level + 1]);
            let end_ll = Version::<R, FP>::scope_search(max, &version.level_slice[level + 1]);

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
        Ok(meet_scopes_ll)
    }

    fn this_level_scopes<'a>(
        version: &'a Version<R, FP>,
        min: &<R as Record>::Key,
        max: &<R as Record>::Key,
        level: usize,
    ) -> Vec<&'a Scope<<R as Record>::Key>> {
        let mut meet_scopes_l = Vec::new();
        let mut start_l = 0;
        let option = version.option();
        if level > 0 {
            start_l = Version::<R, FP>::scope_search(min, &version.level_slice[level]);
        }

        for scope in version.level_slice[level][start_l..].iter() {
            if (scope.contains(min) || scope.contains(max))
                && meet_scopes_l.len() <= option.major_l_selection_table_max_num
            {
                meet_scopes_l.push(scope);
            } else {
                break;
            }
        }
        if meet_scopes_l.is_empty() {
            let end_l = cmp::min(
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
        meet_scopes_l
    }

    async fn build_tables<'scan>(
        option: &DbOption<R>,
        version_edits: &mut Vec<VersionEdit<<R as Record>::Key>>,
        level: usize,
        streams: Vec<ScanStream<'scan, R, FP>>,
    ) -> Result<(), CompactionError<R>>
    where
        FP: 'scan,
    {
        let mut stream = MergeStream::<R, FP>::from_vec(streams, u32::MAX.into()).await?;

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

    #[allow(unused)]
    fn full_scope<'a>(
        meet_scopes: &[&'a Scope<<R as Record>::Key>],
    ) -> Result<(&'a <R as Record>::Key, &'a <R as Record>::Key), CompactionError<R>> {
        let lower = &meet_scopes.first().ok_or(CompactionError::EmptyLevel)?.min;
        let upper = &meet_scopes.last().ok_or(CompactionError::EmptyLevel)?.max;
        Ok((lower, upper))
    }

    async fn build_table(
        option: &DbOption<R>,
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
            FP::open(option.table_path(&gen)).await?,
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
    #[error("compaction version error: {0}")]
    Version(#[from] VersionError<R>),
    #[error("database error: {0}")]
    Commit(#[from] CommitError<R>),
    #[error("the level being compacted does not have a table")]
    EmptyLevel,
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::{atomic::AtomicU32, Arc};

    use flume::bounded;
    use parquet::{arrow::AsyncArrowWriter, errors::ParquetError};
    use tempfile::TempDir;

    use crate::{
        compaction::{states::CompactionStates, Compactor},
        executor::{tokio::TokioExecutor, Executor},
        fs::{FileId, FileProvider},
        inmem::{immutable::Immutable, mutable::Mutable},
        record::Record,
        scope::Scope,
        tests::Test,
        timestamp::Timestamp,
        trigger::{TriggerFactory, TriggerType},
        version::{edit::VersionEdit, Version, MAX_LEVEL},
        wal::log::LogType,
        DbError, DbOption, DB,
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
        gen: FileId,
        records: Vec<(LogType, R, Timestamp)>,
    ) -> Result<(), DbError<R>>
    where
        R: Record + Send,
        FP: Executor,
    {
        let immutable = build_immutable::<R, FP>(option, records).await?;
        let mut writer = AsyncArrowWriter::try_new(
            FP::open(option.table_path(&gen))
                .await
                .map_err(ParquetError::from)?,
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
        let option = DbOption::from(temp_dir.path());
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

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
            &DbOption::from(temp_dir.path()),
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

        let mut option = DbOption::from(temp_dir.path());
        option.major_threshold_with_sst_size = 2;
        let option = Arc::new(option);

        let ((table_gen_1, table_gen_2, table_gen_3, table_gen_4, _), version) =
            build_version(&option).await;

        let min = 2.to_string();
        let max = 5.to_string();
        let mut version_edits = Vec::new();

        Compactor::<Test, TokioExecutor>::major_compaction(
            Arc::new(CompactionStates::new(option.clone())),
            &version,
            &option,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
        )
        .await
        .unwrap();
        dbg!(version_edits.clone());
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
    ) -> (
        (FileId, FileId, FileId, FileId, FileId),
        Version<Test, TokioExecutor>,
    ) {
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        // level 0
        let table_gen_1 = FileId::new();
        let table_gen_2 = FileId::new();
        build_parquet_table::<Test, TokioExecutor>(
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
        )
        .await
        .unwrap();
        build_parquet_table::<Test, TokioExecutor>(
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
        )
        .await
        .unwrap();

        // level 1
        let table_gen_3 = FileId::new();
        let table_gen_4 = FileId::new();
        let table_gen_5 = FileId::new();
        build_parquet_table::<Test, TokioExecutor>(
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
        )
        .await
        .unwrap();
        build_parquet_table::<Test, TokioExecutor>(
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
        )
        .await
        .unwrap();
        build_parquet_table::<Test, TokioExecutor>(
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

    // https://github.com/tonbo-io/tonbo/pull/139
    #[tokio::test]
    pub(crate) async fn major_panic() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::from(temp_dir.path());
        option.major_threshold_with_sst_size = 1;
        option.level_sst_magnification = 1;
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let table_gen0 = FileId::new();
        let table_gen1 = FileId::new();
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
        build_parquet_table::<Test, TokioExecutor>(&option, table_gen0, records0)
            .await
            .unwrap();
        build_parquet_table::<Test, TokioExecutor>(&option, table_gen1, records1)
            .await
            .unwrap();

        let option = Arc::new(option);
        let (sender, _) = bounded(1);
        let mut version = Version::<Test, TokioExecutor>::new(
            option.clone(),
            sender,
            Arc::new(AtomicU32::default()),
        );
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

        Compactor::<Test, TokioExecutor>::major_compaction(
            Arc::new(CompactionStates::new(option.clone())),
            &version,
            &option,
            &min,
            &max,
            &mut version_edits,
            &mut vec![],
        )
        .await
        .unwrap();
    }

    // issue: https://github.com/tonbo-io/tonbo/issues/152
    #[tokio::test]
    async fn test_flush_major_level_sort() {
        let temp_dir = TempDir::new().unwrap();

        let mut option = DbOption::from(temp_dir.path());
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
        let version = db.version_set.current().await;

        dbg!(version.clone());
        for i in 0..4 {
            let item = Test {
                vstring: i.to_string(),
                vu32: i,
                vbool: Some(true),
            };
            db.insert(item).await.unwrap();
        }

        db.flush().await.unwrap();
        let version = db.version_set.current().await;

        dbg!(version.clone());

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
        let version = db.version_set.current().await;

        dbg!(version.clone());
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
        let version = db.version_set.current().await;

        dbg!(version.clone());

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

        dbg!(version.clone());
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
                dbg!(current.max.clone(), next.min.clone());
                assert!(current.max < next.min);
            }
        }
        dbg!(version);
    }
}
