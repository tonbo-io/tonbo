//! Leveled compaction planner utilities used to schedule background merges.

use std::cmp::Ordering;

use crate::{
    key::KeyOwned,
    manifest::VersionState,
    ondisk::sstable::{SsTableId, SsTableStats},
};

/// Snapshot of SST layout across levels used for leveled compaction planning.
#[derive(Clone, Debug, Default)]
pub struct CompactionSnapshot {
    levels: Vec<LevelInfo>,
}

impl CompactionSnapshot {
    /// Build a snapshot from the provided levels.
    pub fn new(levels: Vec<LevelInfo>) -> Self {
        Self { levels }
    }

    /// Access the recorded levels.
    pub fn levels(&self) -> &[LevelInfo] {
        &self.levels
    }

    /// Access a specific level by its numeric identifier.
    pub fn level(&self, level: usize) -> Option<&LevelInfo> {
        self.levels.iter().find(|info| info.level == level)
    }

    /// Returns true when no levels/files have been recorded.
    pub fn is_empty(&self) -> bool {
        self.levels.iter().all(LevelInfo::is_empty)
    }
}

/// Metadata about a single compaction level.
#[derive(Clone, Debug)]
pub struct LevelInfo {
    level: usize,
    files: Vec<LevelFile>,
}

impl LevelInfo {
    /// Create a new level with the provided files.
    pub fn new(level: usize, files: Vec<LevelFile>) -> Self {
        Self { level, files }
    }

    /// Level number (0 == L0).
    pub fn level(&self) -> usize {
        self.level
    }

    /// Files recorded for this level.
    pub fn files(&self) -> &[LevelFile] {
        &self.files
    }

    /// Mutable access to files (useful for tests/builders).
    pub fn files_mut(&mut self) -> &mut Vec<LevelFile> {
        &mut self.files
    }

    /// Number of files stored in this level.
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Returns `true` if the level has no files.
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

impl From<&VersionState> for CompactionSnapshot {
    fn from(state: &VersionState) -> Self {
        let mut levels = Vec::new();
        for (level, entries) in state.ssts().iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let files = entries
                .iter()
                .map(|entry| LevelFile::new(entry.sst_id().clone(), entry.stats().cloned()))
                .collect();
            levels.push(LevelInfo::new(level, files));
        }
        Self { levels }
    }
}

/// Metadata for an individual SST tracked by the planner.
#[derive(Clone, Debug)]
pub struct LevelFile {
    /// Identifier of the SSTable represented by this entry.
    pub sst_id: SsTableId,
    /// Persisted statistics (key bounds, row counts, etc.) for the table.
    pub stats: Option<SsTableStats>,
}

impl LevelFile {
    /// Construct a new entry.
    pub fn new(sst_id: SsTableId, stats: Option<SsTableStats>) -> Self {
        Self { sst_id, stats }
    }
}

/// Compaction planner configuration knobs.
#[derive(Clone, Debug)]
pub struct LeveledPlannerConfig {
    /// Number of L0 files that should trigger compaction into L1.
    pub l0_trigger: usize,
    /// Maximum number of L0 files to include in a single plan.
    pub l0_max_inputs: usize,
    /// Per-level thresholds (starting at L1) for how many files can reside in a level
    /// before the planner schedules a compaction into the next level.
    pub level_thresholds: Vec<usize>,
    /// Maximum number of files to compact in a single task for leveled runs.
    pub max_inputs_per_task: usize,
}

impl Default for LeveledPlannerConfig {
    fn default() -> Self {
        Self {
            l0_trigger: 8,
            l0_max_inputs: 6,
            level_thresholds: vec![10, 12, 14],
            max_inputs_per_task: 10,
        }
    }
}

/// Description of a scheduled compaction.
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionTask {
    /// Level from which input SSTs are being compacted.
    pub source_level: usize,
    /// Level that will receive the compacted output.
    pub target_level: usize,
    /// Identifiers of SSTs that must be compacted together.
    pub input: Vec<SsTableId>,
    /// Aggregated key range covered by the selected SSTs (when stats are available).
    pub key_range: Option<(KeyOwned, KeyOwned)>,
}

/// Simple leveled compaction planner that enforces fan-in thresholds per level.
#[derive(Clone, Debug)]
pub struct LeveledCompactionPlanner {
    cfg: LeveledPlannerConfig,
}

impl LeveledCompactionPlanner {
    /// Create a planner using the provided configuration.
    pub fn new(cfg: LeveledPlannerConfig) -> Self {
        Self { cfg }
    }

    /// Examine the snapshot of levels and return the next compaction task, if any.
    pub fn plan(&self, snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
        if let Some(task) = self.plan_l0(snapshot) {
            return Some(task);
        }
        self.plan_leveled(snapshot)
    }

    fn plan_l0(&self, snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
        let level0 = snapshot.level(0)?;
        if level0.len() < self.cfg.l0_trigger {
            return None;
        }
        let take = self.cfg.l0_max_inputs.min(level0.len()).max(1);
        let selected: Vec<&LevelFile> = level0.files().iter().take(take).collect();
        let input: Vec<SsTableId> = selected.iter().map(|file| file.sst_id.clone()).collect();
        Some(CompactionTask {
            source_level: 0,
            target_level: 1,
            key_range: aggregate_key_range(selected.iter().copied()),
            input,
        })
    }

    fn plan_leveled(&self, snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
        for level_info in snapshot.levels().iter().filter(|info| info.level > 0) {
            let idx = level_info.level - 1;
            let Some(limit) = self.cfg.level_thresholds.get(idx).copied() else {
                continue;
            };
            if level_info.len() <= limit {
                continue;
            }
            let take = self.cfg.max_inputs_per_task.min(level_info.len()).max(1);
            let selected = select_compaction_run(level_info.files(), take);
            if selected.is_empty() {
                continue;
            }
            let input: Vec<SsTableId> = selected.iter().map(|file| file.sst_id.clone()).collect();
            return Some(CompactionTask {
                source_level: level_info.level,
                target_level: level_info.level + 1,
                key_range: aggregate_key_range(selected.iter().copied()),
                input,
            });
        }
        None
    }
}

fn select_compaction_run(files: &[LevelFile], take: usize) -> Vec<&LevelFile> {
    if files.is_empty() {
        return Vec::new();
    }
    let mut ordered: Vec<&LevelFile> = files.iter().collect();
    ordered.sort_by(|lhs, rhs| compare_key(lhs, rhs));
    ordered.into_iter().take(take).collect()
}

fn compare_key(lhs: &LevelFile, rhs: &LevelFile) -> Ordering {
    match (&lhs.stats, &rhs.stats) {
        (Some(ls), Some(rs)) => match (&ls.min_key, &rs.min_key) {
            (Some(lk), Some(rk)) => lk.cmp(rk),
            _ => Ordering::Equal,
        },
        _ => Ordering::Equal,
    }
}

fn aggregate_key_range<'a>(
    files: impl Iterator<Item = &'a LevelFile>,
) -> Option<(KeyOwned, KeyOwned)> {
    let mut min_key: Option<KeyOwned> = None;
    let mut max_key: Option<KeyOwned> = None;
    for file in files {
        let stats = file.stats.as_ref()?;
        if let Some(candidate) = stats.min_key.as_ref() {
            min_key = match &min_key {
                Some(current) if current <= candidate => Some(current.clone()),
                _ => Some(candidate.clone()),
            };
        }
        if let Some(candidate) = stats.max_key.as_ref() {
            max_key = match &max_key {
                Some(current) if current >= candidate => Some(current.clone()),
                _ => Some(candidate.clone()),
            };
        }
    }
    match (min_key, max_key) {
        (Some(min), Some(max)) => Some((min, max)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats_with_keys(min: &str, max: &str) -> SsTableStats {
        SsTableStats {
            rows: 1,
            bytes: 1,
            tombstones: 0,
            min_key: Some(KeyOwned::from(min)),
            max_key: Some(KeyOwned::from(max)),
            min_commit_ts: None,
            max_commit_ts: None,
        }
    }

    fn file(id: u64, min: &str, max: &str) -> LevelFile {
        LevelFile::new(SsTableId::new(id), Some(stats_with_keys(min, max)))
    }

    #[test]
    fn l0_trigger_schedules_plan() {
        let level0 = LevelInfo::new(
            0,
            vec![
                file(1, "a", "b"),
                file(2, "c", "d"),
                file(3, "e", "f"),
                file(4, "g", "h"),
            ],
        );
        let snapshot = CompactionSnapshot::new(vec![level0]);
        let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
            l0_trigger: 3,
            l0_max_inputs: 2,
            ..Default::default()
        });
        let task = planner.plan(&snapshot).expect("plan");
        assert_eq!(task.source_level, 0);
        assert_eq!(task.target_level, 1);
        assert_eq!(task.input, vec![SsTableId::new(1), SsTableId::new(2)]);
        let (min, max) = task.key_range.expect("key range");
        assert_eq!(min.as_utf8(), Some("a"));
        assert_eq!(max.as_utf8(), Some("d"));
    }

    #[test]
    fn leveled_threshold_schedules_next_level() {
        let level1 = LevelInfo::new(
            1,
            vec![
                file(10, "a", "c"),
                file(11, "d", "f"),
                file(12, "g", "i"),
                file(13, "j", "l"),
            ],
        );
        let snapshot = CompactionSnapshot::new(vec![LevelInfo::new(0, vec![]), level1]);
        let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
            l0_trigger: usize::MAX,
            l0_max_inputs: 0,
            level_thresholds: vec![3],
            max_inputs_per_task: 2,
        });
        let task = planner.plan(&snapshot).expect("plan");
        assert_eq!(task.source_level, 1);
        assert_eq!(task.target_level, 2);
        assert_eq!(task.input.len(), 2);
        assert_eq!(task.input[0], SsTableId::new(10));
        assert_eq!(task.input[1], SsTableId::new(11));
    }

    #[test]
    fn key_range_absent_when_stats_missing() {
        let files = vec![LevelFile::new(SsTableId::new(7), None)];
        let level1 = LevelInfo::new(1, files);
        let snapshot = CompactionSnapshot::new(vec![level1]);
        let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
            l0_trigger: usize::MAX,
            l0_max_inputs: 0,
            level_thresholds: vec![0],
            max_inputs_per_task: 1,
        });
        let task = planner.plan(&snapshot).expect("plan");
        assert!(task.key_range.is_none());
    }
}
