use std::{ops::Bound, sync::Arc};

use parquet::arrow::ProjectionMask;

use super::{states::CompactionStates, CompactionError, Compactor};
use crate::{
    fs::{FileId, FileProvider},
    ondisk::sstable::SsTable,
    record::Record,
    stream::{level::LevelStream, ScanStream},
    version::{edit::VersionEdit, Version, MAX_LEVEL},
    DbOption,
};

/// `CompactionStatus` is the result of a compaction.
pub(crate) struct CompactionStatus<R>
where
    R: Record,
{
    pub(crate) delete_gens: Option<Vec<FileId>>,
    pub(crate) version_edits: Vec<VersionEdit<R::Key>>,
}

pub(crate) enum CompactionTask {
    #[allow(unused)]
    Minor,
    Major(MajorTask),
}

pub(crate) struct MajorTask {
    base: Vec<FileId>,
    target: Vec<FileId>,
    base_level: usize,
}

impl MajorTask {
    pub(crate) fn new(base_level: usize, base: Vec<FileId>, target: Vec<FileId>) -> Self {
        Self {
            base,
            target,
            base_level,
        }
    }

    pub(crate) fn base(&self) -> &Vec<FileId> {
        &self.base
    }

    pub(crate) fn target(&self) -> &Vec<FileId> {
        &self.target
    }

    #[allow(unused)]
    pub(crate) fn level(&self) -> u8 {
        self.base_level as u8
    }

    pub(crate) async fn compact<R, FP>(
        &self,
        option: Arc<DbOption<R>>,
        version_edits: &mut Vec<VersionEdit<<R as Record>::Key>>,
    ) -> Result<(), CompactionError<R>>
    where
        R: Record,
        FP: FileProvider,
    {
        let mut streams = Vec::with_capacity(self.base.len() + self.target.len());
        if self.base_level == 0 {
            for gen in self.base().iter() {
                let file = FP::open(option.table_path(gen)).await?;

                streams.push(ScanStream::SsTable {
                    inner: SsTable::<R, FP>::open(file)
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
            let gens = self.base.iter().copied().collect();

            let level_scan_l = LevelStream::new(
                option.clone(),
                gens,
                (Bound::Unbounded, Bound::Unbounded),
                u32::MAX.into(),
                None,
                ProjectionMask::all(),
            )
            .ok_or(CompactionError::EmptyLevel)?;

            streams.push(ScanStream::Level {
                inner: level_scan_l,
            });
        }
        if !self.target.is_empty() {
            // Next Level
            let gens = self.target.iter().copied().collect();
            let level_scan_ll = LevelStream::new(
                option.clone(),
                gens,
                (Bound::Unbounded, Bound::Unbounded),
                u32::MAX.into(),
                None,
                ProjectionMask::all(),
            )
            .ok_or(CompactionError::EmptyLevel)?;

            streams.push(ScanStream::Level {
                inner: level_scan_ll,
            });
        }
        Compactor::build_tables(option.as_ref(), version_edits, self.base_level, streams).await?;

        for gen in self.base.iter() {
            version_edits.push(VersionEdit::Remove {
                level: self.base_level as u8,
                gen: *gen,
            });
        }
        for gen in self.target.iter() {
            version_edits.push(VersionEdit::Remove {
                level: self.base_level as u8 + 1,
                gen: *gen,
            });
        }
        Ok(())
    }
}

impl CompactionTask {
    pub(crate) async fn generate_major_tasks<R, FP>(
        version: &Version<R, FP>,
        option: &DbOption<R>,
        mut min: &R::Key,
        mut max: &R::Key,
    ) -> Result<Vec<CompactionTask>, CompactionError<R>>
    where
        R: Record,
        FP: FileProvider,
    {
        let mut level = 0;
        let mut tasks = vec![];

        while level < MAX_LEVEL - 2 {
            if !option.is_threshold_exceeded_major(version, level) {
                break;
            }
            let meet_scopes_l = Compactor::this_level_scopes(version, min, max, level);
            let meet_scopes_ll =
                Compactor::next_level_scopes(version, &mut min, &mut max, level, &meet_scopes_l)?;

            let base = meet_scopes_l.iter().map(|scope| scope.gen).collect();
            let target = meet_scopes_ll.iter().map(|scope| scope.gen).collect();

            tasks.push(CompactionTask::Major(MajorTask::new(level, base, target)));
            level += 1;
        }

        Ok(tasks)
    }

    pub(crate) async fn compact<R, FP>(
        &self,
        states: &Arc<CompactionStates<R>>,
    ) -> Result<CompactionStatus<R>, CompactionError<R>>
    where
        R: Record,
        FP: FileProvider,
    {
        match self {
            CompactionTask::Minor => todo!(),
            CompactionTask::Major(task) => {
                let mut delete_gens = vec![];
                task.base().iter().for_each(|gen| delete_gens.push(*gen));
                task.target().iter().for_each(|gen| delete_gens.push(*gen));

                let mut version_edits = vec![];
                task.compact::<R, FP>(states.option.clone(), &mut version_edits)
                    .await?;

                Ok(CompactionStatus {
                    delete_gens: Some(delete_gens),
                    version_edits,
                })
            }
        }
    }

    pub(crate) async fn finish<R>(&self, states: &Arc<CompactionStates<R>>)
    where
        R: Record,
    {
        states.finish_task(self).await;
    }
}
