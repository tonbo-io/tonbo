//! Leveled compaction planner utilities used to schedule background merges.

use std::cmp::Ordering;

use crate::{
    key::KeyOwned,
    manifest::VersionState,
    ondisk::sstable::{SsTableId, SsTableStats},
};

/// Abstract compaction planner interface to support selectable strategies.
pub(crate) trait CompactionPlanner {
    /// Examine the snapshot of levels and return the next compaction task, if any.
    fn plan(&self, snapshot: &CompactionSnapshot) -> Option<CompactionTask>;
}

/// Available compaction strategies selectable via configuration.
#[derive(Clone, Debug)]
pub enum CompactionStrategy {
    /// Leveled compaction (tiered levels, non-overlapping within a level).
    Leveled(LeveledPlannerConfig),
}

impl Default for CompactionStrategy {
    fn default() -> Self {
        Self::Leveled(LeveledPlannerConfig::default())
    }
}

/// Planner enum used to keep a concrete planner instance around even as strategy becomes pluggable.
#[derive(Clone, Debug)]
pub(crate) enum CompactionPlannerKind {
    /// Leveled planner implementation.
    Leveled(LeveledCompactionPlanner),
}

impl CompactionPlanner for CompactionPlannerKind {
    fn plan(&self, snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
        match self {
            Self::Leveled(planner) => LeveledCompactionPlanner::plan(planner, snapshot),
        }
    }
}

impl CompactionStrategy {
    /// Build a concrete planner for the selected strategy.
    pub(crate) fn build(self) -> CompactionPlannerKind {
        match self {
            Self::Leveled(cfg) => {
                CompactionPlannerKind::Leveled(LeveledCompactionPlanner::new(cfg))
            }
        }
    }
}

/// Snapshot of SST layout across levels used for leveled compaction planning.
#[derive(Clone, Debug, Default)]
pub(crate) struct CompactionSnapshot {
    levels: Vec<LevelInfo>,
}

impl CompactionSnapshot {
    /// Build a snapshot from the provided levels.
    #[cfg(test)]
    pub(crate) fn new(levels: Vec<LevelInfo>) -> Self {
        Self { levels }
    }

    /// Access the recorded levels.
    pub(crate) fn levels(&self) -> &[LevelInfo] {
        &self.levels
    }

    /// Access a specific level by its numeric identifier.
    pub(crate) fn level(&self, level: usize) -> Option<&LevelInfo> {
        self.levels.iter().find(|info| info.level == level)
    }

    /// Returns true when no levels/files have been recorded.
    pub(crate) fn is_empty(&self) -> bool {
        self.levels.iter().all(LevelInfo::is_empty)
    }
}

/// Metadata about a single compaction level.
#[derive(Clone, Debug)]
pub(crate) struct LevelInfo {
    level: usize,
    files: Vec<LevelFile>,
}

impl LevelInfo {
    /// Create a new level with the provided files.
    pub(crate) fn new(level: usize, files: Vec<LevelFile>) -> Self {
        Self { level, files }
    }

    /// Files recorded for this level.
    pub(crate) fn files(&self) -> &[LevelFile] {
        &self.files
    }

    /// Number of files stored in this level.
    pub(crate) fn len(&self) -> usize {
        self.files.len()
    }

    /// Returns `true` if the level has no files.
    pub(crate) fn is_empty(&self) -> bool {
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
pub(crate) struct LevelFile {
    /// Identifier of the SSTable represented by this entry.
    pub(crate) sst_id: SsTableId,
    /// Persisted statistics (key bounds, row counts, etc.) for the table.
    stats: Option<SsTableStats>,
}

impl LevelFile {
    /// Construct a new entry.
    pub(crate) fn new(sst_id: SsTableId, stats: Option<SsTableStats>) -> Self {
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
    /// Total bytes in L0 that should trigger compaction into L1.
    pub l0_max_bytes: Option<usize>,
    /// Per-level thresholds (starting at L1) for how many files can reside in a level
    /// before the planner schedules a compaction into the next level.
    pub level_thresholds: Vec<usize>,
    /// Per-level byte thresholds (starting at L1) used to trigger compaction into the next level.
    pub level_max_bytes: Vec<Option<usize>>,
    /// Maximum number of files to compact in a single task for leveled runs.
    pub max_inputs_per_task: usize,
    /// Maximum total bytes to include in a single compaction task across all selected inputs.
    pub max_task_bytes: Option<usize>,
}

impl Default for LeveledPlannerConfig {
    fn default() -> Self {
        Self {
            l0_trigger: 8,
            l0_max_inputs: 6,
            l0_max_bytes: None,
            level_thresholds: vec![10, 12, 14],
            level_max_bytes: Vec::new(),
            max_inputs_per_task: 10,
            max_task_bytes: None,
        }
    }
}

/// Description of a scheduled compaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompactionInput {
    /// Level that currently owns the SST being compacted.
    pub(crate) level: usize,
    /// Identifier of the SST.
    pub(crate) sst_id: SsTableId,
}

/// Description of a scheduled compaction.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CompactionTask {
    /// Level from which input SSTs are being compacted.
    pub(crate) source_level: usize,
    /// Level that will receive the compacted output.
    pub(crate) target_level: usize,
    /// Identifiers of SSTs that must be compacted together, tagged with their owning level.
    pub(crate) input: Vec<CompactionInput>,
    /// Aggregated key range covered by the selected SSTs (when stats are available).
    pub(crate) key_range: Option<(KeyOwned, KeyOwned)>,
}

/// Simple leveled compaction planner that enforces fan-in thresholds per level.
#[derive(Clone, Debug)]
pub(crate) struct LeveledCompactionPlanner {
    cfg: LeveledPlannerConfig,
}

impl LeveledCompactionPlanner {
    /// Create a planner using the provided configuration.
    pub(crate) fn new(cfg: LeveledPlannerConfig) -> Self {
        Self { cfg }
    }

    /// Examine the snapshot of levels and return the next compaction task, if any.
    fn plan(&self, snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
        if let Some(task) = self.plan_l0(snapshot) {
            return Some(task);
        }
        self.plan_leveled(snapshot)
    }

    fn plan_l0(&self, snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
        let level0 = snapshot.level(0)?;
        let over_l0_bytes = self
            .cfg
            .l0_max_bytes
            .map(|cap| total_bytes(level0.files()) >= cap)
            .unwrap_or(false);
        if level0.len() < self.cfg.l0_trigger && !over_l0_bytes {
            return None;
        }
        let take = self.cfg.l0_max_inputs.min(level0.len()).max(1);
        let selected: Vec<&LevelFile> =
            select_compaction_run(level0.files(), take, self.cfg.max_task_bytes);
        let input: Vec<CompactionInput> = selected
            .iter()
            .map(|file| CompactionInput {
                level: 0,
                sst_id: file.sst_id.clone(),
            })
            .collect();
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
            let max_bytes = self.cfg.level_max_bytes.get(idx).and_then(|b| *b);
            let over_bytes = max_bytes
                .map(|cap| total_bytes(level_info.files()) >= cap)
                .unwrap_or(false);
            if level_info.len() <= limit && !over_bytes {
                continue;
            }
            let take = self.cfg.max_inputs_per_task.min(level_info.len()).max(1);
            let selected = select_compaction_run(level_info.files(), take, self.cfg.max_task_bytes);
            if selected.is_empty() {
                continue;
            }
            let mut input: Vec<CompactionInput> = selected
                .iter()
                .map(|file| CompactionInput {
                    level: level_info.level,
                    sst_id: file.sst_id.clone(),
                })
                .collect();
            let mut key_range = aggregate_key_range(selected.iter().copied());
            // Pull overlapping SSTs from the next level to reduce post-compaction overlap.
            if let Some(next_level) = snapshot.level(level_info.level + 1)
                && let Some((min_key, max_key)) = key_range.as_ref()
            {
                let mut overlaps: Vec<&LevelFile> = Vec::new();
                for file in next_level.files() {
                    let Some(stats) = file.stats.as_ref() else {
                        continue;
                    };
                    match (&stats.min_key, &stats.max_key) {
                        (Some(file_min), Some(file_max))
                            if file_min <= max_key && file_max >= min_key =>
                        {
                            overlaps.push(file);
                            input.push(CompactionInput {
                                level: next_level.level,
                                sst_id: file.sst_id.clone(),
                            });
                        }
                        _ => {}
                    }
                }
                if !overlaps.is_empty() {
                    key_range = aggregate_key_range(selected.iter().copied().chain(overlaps));
                }
            }
            return Some(CompactionTask {
                source_level: level_info.level,
                target_level: level_info.level + 1,
                key_range,
                input,
            });
        }
        None
    }
}

impl CompactionPlanner for LeveledCompactionPlanner {
    fn plan(&self, snapshot: &CompactionSnapshot) -> Option<CompactionTask> {
        LeveledCompactionPlanner::plan(self, snapshot)
    }
}

fn select_compaction_run(
    files: &[LevelFile],
    take: usize,
    max_bytes: Option<usize>,
) -> Vec<&LevelFile> {
    if files.is_empty() {
        return Vec::new();
    }
    let mut ordered: Vec<&LevelFile> = files.iter().collect();
    ordered.sort_by(|lhs, rhs| compare_key(lhs, rhs));
    let mut selected = Vec::new();
    let mut bytes = 0usize;
    for file in ordered.into_iter() {
        if selected.len() >= take {
            break;
        }
        let file_bytes = file.stats.as_ref().map(|s| s.bytes).unwrap_or(0);
        if let Some(cap) = max_bytes
            && !selected.is_empty()
            && bytes + file_bytes > cap
        {
            break;
        }
        bytes += file_bytes;
        selected.push(file);
    }
    selected
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

fn total_bytes(files: &[LevelFile]) -> usize {
    files
        .iter()
        .filter_map(|file| file.stats.as_ref())
        .map(|stats| stats.bytes)
        .sum()
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

    fn stats_with_bytes(min: &str, max: &str, bytes: usize) -> SsTableStats {
        SsTableStats {
            rows: 1,
            bytes,
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

    fn file_with_bytes(id: u64, min: &str, max: &str, bytes: usize) -> LevelFile {
        LevelFile::new(SsTableId::new(id), Some(stats_with_bytes(min, max, bytes)))
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
        assert_eq!(
            task.input,
            vec![
                CompactionInput {
                    level: 0,
                    sst_id: SsTableId::new(1),
                },
                CompactionInput {
                    level: 0,
                    sst_id: SsTableId::new(2),
                },
            ]
        );
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
            l0_max_bytes: None,
            level_max_bytes: Vec::new(),
            max_task_bytes: None,
        });
        let task = planner.plan(&snapshot).expect("plan");
        assert_eq!(task.source_level, 1);
        assert_eq!(task.target_level, 2);
        assert_eq!(task.input.len(), 2);
        assert_eq!(
            task.input[0],
            CompactionInput {
                level: 1,
                sst_id: SsTableId::new(10)
            }
        );
        assert_eq!(
            task.input[1],
            CompactionInput {
                level: 1,
                sst_id: SsTableId::new(11)
            }
        );
    }

    #[test]
    fn l0_byte_threshold_triggers_plan() {
        let level0 = LevelInfo::new(
            0,
            vec![
                file_with_bytes(1, "a", "b", 60),
                file_with_bytes(2, "c", "d", 60),
            ],
        );
        let snapshot = CompactionSnapshot::new(vec![level0]);
        let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
            l0_trigger: usize::MAX,
            l0_max_inputs: 2,
            l0_max_bytes: Some(50),
            ..Default::default()
        });
        let task = planner.plan(&snapshot).expect("plan");
        assert_eq!(task.input.len(), 2);
    }

    #[test]
    fn max_task_bytes_limits_selection() {
        let level1 = LevelInfo::new(
            1,
            vec![
                file_with_bytes(1, "a", "b", 40),
                file_with_bytes(2, "c", "d", 40),
                file_with_bytes(3, "e", "f", 40),
            ],
        );
        let snapshot = CompactionSnapshot::new(vec![LevelInfo::new(0, vec![]), level1]);
        let planner = LeveledCompactionPlanner::new(LeveledPlannerConfig {
            l0_trigger: usize::MAX,
            l0_max_inputs: 0,
            level_thresholds: vec![1],
            level_max_bytes: vec![None],
            max_inputs_per_task: 3,
            max_task_bytes: Some(80),
            ..Default::default()
        });

        let task = planner.plan(&snapshot).expect("plan");
        assert_eq!(task.input.len(), 2);
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
            l0_max_bytes: None,
            level_max_bytes: Vec::new(),
            max_task_bytes: None,
        });
        let task = planner.plan(&snapshot).expect("plan");
        assert!(task.key_range.is_none());
    }
}
