use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use async_lock::RwLock;

use super::task::CompactionTask;
use crate::{fs::FileId, record::Record, DbOption};

/// `CompactionStates` is a global state of compaction, containing the compaction tasks that will be
/// compacted
#[allow(unused)]
pub(crate) struct CompactionStates<R>
where
    R: Record,
{
    inner: Arc<RwLock<CompactionStatesInner>>,
    pub(crate) option: Arc<DbOption<R>>,
}

impl<R> CompactionStates<R>
where
    R: Record,
{
    pub(crate) fn new(option: Arc<DbOption<R>>) -> Self {
        Self {
            option,
            inner: Arc::new(RwLock::new(CompactionStatesInner::new())),
        }
    }

    #[allow(unused)]
    pub(crate) async fn in_process(&self, gen: &FileId) -> bool {
        self.inner.read().await.status.contains(gen)
    }

    #[allow(unused)]
    pub(crate) async fn add_task(&self, task: CompactionTask) {
        self.inner.write().await.add_task(task)
    }

    pub(crate) async fn add_task_batch(&self, task_batch: Vec<CompactionTask>) {
        let mut guard = self.inner.write().await;
        for task in task_batch {
            guard.add_task(task)
        }
    }

    pub(crate) async fn consume_task(&self) -> Option<CompactionTask> {
        self.inner.write().await.consume_task()
    }

    pub(crate) async fn finish_task(&self, task: &CompactionTask) {
        self.inner.write().await.finish_task(task)
    }
}

struct CompactionStatesInner {
    tasks: VecDeque<CompactionTask>,
    status: HashSet<FileId>,
}

impl CompactionStatesInner {
    fn new() -> Self {
        Self {
            tasks: VecDeque::default(),
            status: HashSet::default(),
        }
    }

    fn add_task(&mut self, task: CompactionTask) {
        if let CompactionTask::Major(task) = &task {
            for gen in task.base().iter().chain(task.target().iter()) {
                self.status.insert(*gen);
            }
        }
        self.tasks.push_back(task);
    }

    fn consume_task(&mut self) -> Option<CompactionTask> {
        self.tasks.pop_front()
    }

    fn finish_task(&mut self, task: &CompactionTask) {
        if let CompactionTask::Major(task) = &task {
            for gen in task.base().iter().chain(task.target().iter()) {
                self.status.remove(gen);
            }
        }
    }
}
