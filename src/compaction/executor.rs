//! Compaction executor contracts and scaffolding for major compaction.
//!
//! Design note: the executor is responsible for constructing target descriptors/config
//! rather than receiving pre-baked outputs from `DB::run_compaction_task`. This keeps
//! compaction policy (target level/paths/compression/tuning) local to the executor,
//! allows swapping executors without changing DB plumbing, and mirrors the autonomy we
//! want for remote/serverless compactors. The DB only supplies planner output and input
//! descriptors; the executor decides how to materialize new SSTs and report edits.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use ulid::Ulid;

use crate::{
    compaction::planner::CompactionTask,
    manifest::{ManifestError, SstEntry, VersionEdit, WalSegmentRef},
    mode::DynMode,
    mvcc::Timestamp,
    ondisk::{
        merge::cleanup_descriptors,
        sstable::{SsTableConfig, SsTableDescriptor, SsTableId, SsTableMerger},
    },
};

/// Lease/ownership token used when delegating compaction to a remote worker.
#[derive(Debug, Clone)]
pub struct CompactionLease {
    /// Unique identifier for the leased job.
    pub id: Ulid,
    /// Human-readable owner/worker identifier for observability.
    pub owner: String,
    /// Lease time-to-live in milliseconds; renew before expiry to retain ownership.
    pub ttl_ms: u64,
}

/// API for acquiring/renewing/releasing compaction leases (placeholder for remote executors).
#[allow(dead_code)]
pub trait CompactionLeaseManager {
    /// Acquire a lease for the given task.
    fn acquire(
        &self,
        task: &CompactionTask,
    ) -> Pin<Box<dyn Future<Output = Result<CompactionLease, CompactionLeaseError>> + Send + '_>>;

    /// Renew an existing lease to extend its TTL.
    fn renew(
        &self,
        lease: &CompactionLease,
    ) -> Pin<Box<dyn Future<Output = Result<(), CompactionLeaseError>> + Send + '_>>;

    /// Release a lease after completion.
    fn release(
        &self,
        lease: CompactionLease,
    ) -> Pin<Box<dyn Future<Output = Result<(), CompactionLeaseError>> + Send + '_>>;
}

/// Execution context for a single planned compaction.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct CompactionJob {
    /// Planner output describing which level/inputs to merge.
    pub task: CompactionTask,
    /// Resolved input SST descriptors (including stats/paths) for the task.
    pub inputs: Vec<SsTableDescriptor>,
    /// Optional lease token when jobs are handed out to remote executors.
    pub lease: Option<CompactionLease>,
}

/// Outcome of a successful compaction run.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct CompactionOutcome {
    /// Newly produced SST entries to be added to the target level.
    pub(crate) add_ssts: Vec<SstEntry>,
    /// SST identifiers that should be removed once compaction finishes.
    pub(crate) remove_ssts: Vec<SsTableDescriptor>,
    /// Target level receiving the new SSTs.
    pub(crate) target_level: u32,
    /// Complete WAL segment set that remains visible after the compaction.
    pub(crate) wal_segments: Option<Vec<WalSegmentRef>>,
    /// Optional watermark for tombstone visibility.
    pub(crate) tombstone_watermark: Option<u64>,
    /// SST outputs produced by the executor (useful for GC hints/tests).
    pub(crate) outputs: Vec<SsTableDescriptor>,
    /// Obsolete SST ids eligible for GC.
    pub(crate) obsolete_sst_ids: Vec<SsTableId>,
    /// Optional WAL floor advancement hint (last visible segment).
    pub(crate) wal_floor: Option<WalSegmentRef>,
    /// WAL segments made obsolete by this compaction (for GC hints).
    pub(crate) obsolete_wal_segments: Vec<WalSegmentRef>,
}

impl CompactionOutcome {
    /// Build manifest edits representing the outcome.
    #[allow(dead_code)]
    pub(crate) fn to_version_edits(&self) -> Vec<VersionEdit> {
        let mut edits = Vec::new();
        let mut level_for_sst: HashMap<SsTableId, u32> = HashMap::new();
        for desc in &self.remove_ssts {
            level_for_sst.insert(desc.id().clone(), desc.level() as u32);
        }

        let mut remove_by_level: HashMap<u32, Vec<SsTableId>> = HashMap::new();
        for desc in &self.remove_ssts {
            remove_by_level
                .entry(desc.level() as u32)
                .or_default()
                .push(desc.id().clone());
        }
        for sst_id in &self.obsolete_sst_ids {
            if let Some(level) = level_for_sst.get(sst_id).copied() {
                remove_by_level
                    .entry(level)
                    .or_default()
                    .push(sst_id.clone());
            }
        }

        for (level, mut sst_ids) in remove_by_level {
            sst_ids.sort_by_key(SsTableId::raw);
            sst_ids.dedup();
            if !sst_ids.is_empty() {
                edits.push(VersionEdit::RemoveSsts { level, sst_ids });
            }
        }
        if !self.add_ssts.is_empty() {
            edits.push(VersionEdit::AddSsts {
                level: self.target_level,
                entries: self.add_ssts.clone(),
            });
        }
        let wal_segments = self
            .wal_segments
            .clone()
            .or_else(|| self.wal_floor.as_ref().map(|floor| vec![floor.clone()]));
        if let Some(segments) = wal_segments {
            edits.push(VersionEdit::SetWalSegments { segments });
        }
        if let Some(watermark) = self.tombstone_watermark {
            edits.push(VersionEdit::SetTombstoneWatermark { watermark });
        }
        edits
    }

    /// Build a compaction outcome from finished SST descriptors, validating required paths.
    #[allow(dead_code)]
    pub(crate) fn from_outputs(
        outputs: Vec<SsTableDescriptor>,
        remove_ssts: Vec<SsTableDescriptor>,
        target_level: u32,
        wal_segments: Option<Vec<WalSegmentRef>>,
    ) -> Result<Self, CompactionError> {
        let mut add_ssts = Vec::with_capacity(outputs.len());
        let mut max_commit: Option<Timestamp> = None;
        for desc in &outputs {
            let data_path = desc
                .data_path()
                .cloned()
                .ok_or(CompactionError::MissingPath("data"))?;
            let delete_path = desc.delete_path().cloned();
            if let Some(stats) = desc.stats()
                && let Some(ts) = stats.max_commit_ts
            {
                max_commit = match max_commit {
                    Some(current) if current >= ts => Some(current),
                    _ => Some(ts),
                };
            }
            let entry = SstEntry::new(
                desc.id().clone(),
                desc.stats().cloned(),
                desc.wal_ids().map(|ids| ids.to_vec()),
                data_path,
                delete_path,
            );
            add_ssts.push(entry);
        }
        Ok(Self {
            add_ssts,
            remove_ssts,
            target_level,
            wal_segments,
            outputs,
            obsolete_sst_ids: Vec::new(),
            wal_floor: None,
            tombstone_watermark: max_commit.map(|ts| ts.get()),
            obsolete_wal_segments: Vec::new(),
        })
    }
}

/// Errors that can surface while executing compaction.
#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    /// Planner or manifest interaction failed.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    /// Lease validation failed or no lease was supplied when required.
    #[error("compaction lease missing or invalid")]
    LeaseMissing,
    /// CAS conflict while publishing manifest edits.
    #[error("manifest CAS conflict")]
    CasConflict,
    /// An expected storage path was missing from an SST descriptor.
    #[error("compaction output missing path for {0}")]
    MissingPath(&'static str),
    /// SST execution failed.
    #[error(transparent)]
    Sst(#[from] crate::ondisk::sstable::SsTableError),
    /// Executor invoked without any inputs to merge.
    #[error("compaction executor received no inputs")]
    NoInputs,
    /// Output would exceed configured size cap.
    #[error("compaction output rows {0} exceed cap {1}")]
    OutputTooLarge(usize, usize),
    /// Placeholder until the executor is implemented.
    #[error("compaction executor not implemented")]
    Unimplemented,
}

/// Errors that can occur while acquiring or renewing compaction leases.
#[derive(Debug, thiserror::Error)]
pub enum CompactionLeaseError {
    /// Lease operations are not supported by the current executor/scheduler.
    #[error("compaction leases are not supported")]
    Unsupported,
}

/// Async trait for orchestrating a major compaction over SST inputs.
#[allow(dead_code)]
pub(crate) trait CompactionExecutor {
    /// Execute a compaction job and return a manifest edit describing the change.
    fn execute(
        &self,
        job: CompactionJob,
    ) -> Pin<Box<dyn Future<Output = Result<CompactionOutcome, CompactionError>> + Send + '_>>;

    /// Best-effort cleanup hook for outputs produced during execution. Used when manifest
    /// publication fails (e.g., CAS conflict) so temporary artifacts do not leak.
    fn cleanup_outputs<'a>(
        &'a self,
        outputs: &'a [SsTableDescriptor],
    ) -> Pin<Box<dyn Future<Output = Result<(), CompactionError>> + Send + 'a>>;
}

/// Local no-op executor placeholder. Returns `Unimplemented` until merge plumbing lands.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct LocalCompactionExecutor {
    config: Arc<SsTableConfig>,
    next_id: Arc<AtomicU64>,
    max_output_rows: Option<usize>,
    max_output_bytes: Option<usize>,
    #[cfg(test)]
    corrupt_descriptors: bool,
}

const DEFAULT_BASE_OUTPUT_BYTES: usize = 128 * 1024 * 1024; // 128 MiB
const DEFAULT_LEVEL_MULTIPLIER: usize = 10;
const DEFAULT_OUTPUT_HARD_CAP_BYTES: usize = 512 * 1024 * 1024; // 512 MiB safety cap

impl LocalCompactionExecutor {
    /// Build a local executor that will use `config` for outputs and allocate SST ids starting at
    /// `start_id`.
    #[allow(dead_code)]
    pub fn new(config: Arc<SsTableConfig>, start_id: u64) -> Self {
        Self {
            config,
            next_id: Arc::new(AtomicU64::new(start_id)),
            max_output_rows: None,
            max_output_bytes: None,
            #[cfg(test)]
            corrupt_descriptors: false,
        }
    }

    /// Cap the number of rows per output SST. Helpful for preventing oversized single-file outputs
    /// in the happy-path executor.
    #[allow(dead_code)]
    pub fn with_max_output_rows(mut self, max_output_rows: usize) -> Self {
        self.max_output_rows = Some(max_output_rows.max(1));
        self
    }

    /// Cap the number of bytes per output SST. Prevents oversized single files when splitting.
    #[allow(dead_code)]
    pub fn with_max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        self.max_output_bytes = Some(max_output_bytes.max(1));
        self
    }

    /// Test-only hook to corrupt descriptors before outcome construction and exercise cleanup.
    #[cfg(test)]
    pub(crate) fn with_corrupt_descriptors_for_test(mut self) -> Self {
        self.corrupt_descriptors = true;
        self
    }

    fn default_output_bytes_for_level(level: usize) -> usize {
        let mut size = DEFAULT_BASE_OUTPUT_BYTES;
        for _ in 0..level {
            size = size.saturating_mul(DEFAULT_LEVEL_MULTIPLIER);
            if size >= DEFAULT_OUTPUT_HARD_CAP_BYTES {
                return DEFAULT_OUTPUT_HARD_CAP_BYTES;
            }
        }
        size.min(DEFAULT_OUTPUT_HARD_CAP_BYTES)
    }

    fn output_caps_for_level(&self, level: usize) -> (Option<usize>, Option<usize>) {
        let rows_cap = self.max_output_rows;
        // Only apply hard cap to defaults; respect explicit caller overrides.
        let bytes_cap = self
            .max_output_bytes
            .or_else(|| Some(Self::default_output_bytes_for_level(level)));
        (rows_cap, bytes_cap)
    }

    fn alloc_descriptor(&self, level: usize) -> SsTableDescriptor {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        SsTableDescriptor::new(SsTableId::new(id), level)
    }
}

impl CompactionExecutor for LocalCompactionExecutor {
    fn execute(
        &self,
        job: CompactionJob,
    ) -> Pin<Box<dyn Future<Output = Result<CompactionOutcome, CompactionError>> + Send + '_>> {
        Box::pin(async move {
            if job.inputs.is_empty() {
                return Err(CompactionError::NoInputs);
            }
            let target = self.alloc_descriptor(job.task.target_level);
            let (max_rows, max_bytes) = self.output_caps_for_level(job.task.target_level);
            let merger =
                SsTableMerger::<DynMode>::new(Arc::clone(&self.config), job.inputs.clone(), target)
                    .with_output_id_allocator(Arc::clone(&self.next_id))
                    .with_output_caps(max_rows, max_bytes);
            let merged = merger.execute().await?;
            let descriptors: Vec<_> = merged.iter().map(|sst| sst.descriptor().clone()).collect();
            let descriptors_for_outcome = {
                #[cfg(test)]
                {
                    if self.corrupt_descriptors {
                        let mut corrupted = descriptors.clone();
                        if let Some(first) = corrupted.first_mut() {
                            *first = SsTableDescriptor::new(first.id().clone(), first.level());
                        }
                        corrupted
                    } else {
                        descriptors.clone()
                    }
                }
                #[cfg(not(test))]
                {
                    descriptors.clone()
                }
            };
            match CompactionOutcome::from_outputs(
                descriptors_for_outcome,
                job.inputs,
                job.task.target_level as u32,
                None,
            ) {
                Ok(outcome) => Ok(outcome),
                Err(err) => {
                    // Best-effort cleanup for partial outputs; surface original error.
                    let _ = self.cleanup_outputs(&descriptors).await;
                    Err(err)
                }
            }
        })
    }

    fn cleanup_outputs<'a>(
        &'a self,
        outputs: &'a [SsTableDescriptor],
    ) -> Pin<Box<dyn Future<Output = Result<(), CompactionError>> + Send + 'a>> {
        Box::pin(async move {
            cleanup_descriptors(&self.config, outputs).await;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use fusio::{disk::LocalFs, dynamic::DynFs, path::Path};
    use futures::StreamExt;
    use tempfile::tempdir;
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        compaction::planner::{CompactionInput, CompactionTask},
        id::FileIdGenerator,
        inmem::immutable::memtable::segment_from_batch_with_key_name,
        mode::DynMode,
        ondisk::sstable::{
            SsTableBuilder, SsTableConfig, SsTableDescriptor, SsTableId, SsTableStats,
        },
        schema::SchemaBuilder,
        test_util::build_batch,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_executor_cleans_outputs_when_outcome_build_fails() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let tmpdir = tempdir().expect("tempdir");
        let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
            .primary_key("id")
            .build()
            .expect("schema builder");
        let extractor = Arc::clone(&mode_cfg.extractor);
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let cfg = Arc::new(
            SsTableConfig::new(
                Arc::clone(&mode_cfg.schema),
                fs,
                Path::from(tmpdir.path().to_string_lossy().to_string()),
            )
            .with_key_extractor(extractor),
        );

        let batch = build_batch(
            Arc::clone(&schema),
            vec![
                DynRow(vec![Some(DynCell::Str("a".into())), Some(DynCell::I32(1))]),
                DynRow(vec![Some(DynCell::Str("b".into())), Some(DynCell::I32(2))]),
            ],
        )
        .expect("batch");
        let immutable =
            segment_from_batch_with_key_name(batch, "id").expect("immutable segment from batch");
        let mut builder = SsTableBuilder::<DynMode>::new(
            Arc::clone(&cfg),
            SsTableDescriptor::new(SsTableId::new(1), 0),
        );
        builder.add_immutable(&immutable).expect("stage seg");
        let input = builder.finish().await.expect("sst");

        let task = CompactionTask {
            source_level: 0,
            target_level: 1,
            input: vec![CompactionInput {
                level: 0,
                sst_id: input.descriptor().id().clone(),
            }],
            key_range: None,
        };
        let job = CompactionJob {
            task,
            inputs: vec![input.descriptor().clone()],
            lease: None,
        };

        let executor =
            LocalCompactionExecutor::new(Arc::clone(&cfg), 10).with_corrupt_descriptors_for_test();
        let result = executor.execute(job).await;
        match result {
            Err(CompactionError::MissingPath("data")) => {}
            other => panic!("expected missing path error, got {other:?}"),
        }

        let level_dir = cfg.level_dir(1).expect("level dir");
        let mut entries = cfg
            .fs()
            .list(&level_dir)
            .await
            .expect("level dir should exist");
        let mut count = 0usize;
        while let Some(item) = entries.next().await.transpose().expect("stream ok") {
            let name = item.path.as_ref();
            if name.ends_with(".parquet") || name.ends_with(".delete.parquet") {
                count += 1;
            }
        }
        assert_eq!(count, 0, "expected compaction outputs to be cleaned");

        drop(tmpdir);
    }

    #[test]
    fn outcome_builds_version_edits() {
        let remove = SsTableDescriptor::new(SsTableId::new(7), 0);
        let wal_id = FileIdGenerator::default().generate();
        let add = SstEntry::new(
            SsTableId::new(9),
            Some(SsTableStats::default()),
            Some(vec![wal_id]),
            Path::from("L1/000000000000000009.parquet"),
            None,
        );
        let outcome = CompactionOutcome {
            add_ssts: vec![add.clone()],
            remove_ssts: vec![remove.clone()],
            target_level: 1,
            wal_segments: Some(vec![WalSegmentRef::new(0, wal_id, 0, 0)]),
            tombstone_watermark: Some(42),
            outputs: vec![],
            obsolete_sst_ids: vec![],
            wal_floor: None,
            obsolete_wal_segments: Vec::new(),
        };
        let edits = outcome.to_version_edits();
        assert_eq!(edits.len(), 4);
        assert!(edits.iter().any(|edit| matches!(
            edit,
            VersionEdit::RemoveSsts { sst_ids, .. } if sst_ids.contains(remove.id())
        )));
        assert!(edits.iter().any(|edit| matches!(
            edit,
            VersionEdit::AddSsts { level, entries } if *level == 1 && entries.first().map(|e| e.sst_id()) == Some(add.sst_id())
        )));
        assert!(edits.iter().any(|edit| matches!(
            edit,
            VersionEdit::SetWalSegments { segments } if segments.len() == 1
        )));
        assert!(edits.iter().any(|edit| matches!(
            edit,
            VersionEdit::SetTombstoneWatermark { watermark } if *watermark == 42
        )));
    }

    #[test]
    fn from_outputs_builds_entries_and_watermark() {
        let wal_id = FileIdGenerator::default().generate();
        let stats = SsTableStats {
            rows: 2,
            bytes: 10,
            tombstones: 1,
            min_key: None,
            max_key: None,
            min_commit_ts: None,
            max_commit_ts: Some(Timestamp::new(7)),
        };
        let output = SsTableDescriptor::new(SsTableId::new(11), 1)
            .with_stats(stats.clone())
            .with_wal_ids(Some(vec![wal_id]))
            .with_storage_paths(
                Path::from("L1/000000000000000011.parquet"),
                Some(Path::from("L1/000000000000000011.delete.parquet")),
            );
        let remove = SsTableDescriptor::new(SsTableId::new(5), 0);
        let outcome = CompactionOutcome::from_outputs(
            vec![output.clone()],
            vec![remove.clone()],
            1,
            Some(vec![WalSegmentRef::new(0, wal_id, 0, 0)]),
        )
        .expect("outcome");
        assert_eq!(outcome.add_ssts.len(), 1);
        assert_eq!(outcome.remove_ssts.len(), 1);
        assert_eq!(outcome.target_level, 1);
        assert_eq!(outcome.tombstone_watermark, Some(7));
        assert_eq!(
            outcome
                .add_ssts
                .first()
                .and_then(|entry| entry.stats())
                .map(|s| s.max_commit_ts),
            Some(stats.max_commit_ts)
        );
    }

    #[test]
    fn from_outputs_fails_on_missing_paths() {
        let desc = SsTableDescriptor::new(SsTableId::new(3), 0);
        let err = CompactionOutcome::from_outputs(vec![desc], Vec::new(), 0, None)
            .expect_err("missing paths");
        assert!(matches!(err, CompactionError::MissingPath("data")));
    }

    #[test]
    fn default_output_caps_are_applied() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let tmpdir = tempfile::tempdir().expect("temp dir");
        let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
            .primary_key("id")
            .build()
            .expect("schema builder");
        let fs: Arc<dyn DynFs> = Arc::new(fusio::disk::LocalFs {});
        let cfg = Arc::new(
            SsTableConfig::new(
                Arc::clone(&mode_cfg.schema),
                fs,
                Path::from(tmpdir.path().to_string_lossy().to_string()),
            )
            .with_key_extractor(Arc::clone(&mode_cfg.extractor)),
        );
        let exec = LocalCompactionExecutor::new(cfg, 0);
        let (rows_cap, bytes_cap) = exec.output_caps_for_level(1);
        assert!(rows_cap.is_none());
        let bytes = bytes_cap.expect("default bytes cap");
        assert!(bytes <= DEFAULT_OUTPUT_HARD_CAP_BYTES);
        // Keep tmpdir alive for the duration of the test.
        drop(tmpdir);
    }

    #[test]
    fn outcome_emits_per_level_removals() {
        let remove_l1 = SsTableDescriptor::new(SsTableId::new(1), 1);
        let remove_l2 = SsTableDescriptor::new(SsTableId::new(2), 2);
        let outcome = CompactionOutcome {
            add_ssts: Vec::new(),
            remove_ssts: vec![remove_l1.clone(), remove_l2.clone()],
            target_level: 2,
            wal_segments: None,
            tombstone_watermark: None,
            outputs: vec![],
            obsolete_sst_ids: vec![remove_l2.id().clone()],
            wal_floor: None,
            obsolete_wal_segments: Vec::new(),
        };

        let edits = outcome.to_version_edits();
        let mut levels: Vec<u32> = Vec::new();
        let mut remove_ids: Vec<Vec<SsTableId>> = Vec::new();
        for edit in edits {
            if let VersionEdit::RemoveSsts { level, sst_ids } = edit {
                levels.push(level);
                remove_ids.push(sst_ids);
            }
        }
        assert_eq!(levels.len(), 2);
        assert!(levels.contains(&1));
        assert!(levels.contains(&2));
        assert!(remove_ids.iter().any(|ids| ids.contains(remove_l1.id())));
        assert!(remove_ids.iter().any(|ids| ids.contains(remove_l2.id())));
    }

    #[test]
    fn to_version_edits_dedups_remove_and_falls_back_to_floor() {
        let wal_id = FileIdGenerator::default().generate();
        let wal_floor = WalSegmentRef::new(7, wal_id, 0, 10);
        let remove = SsTableDescriptor::new(SsTableId::new(1), 0);
        let outcome = CompactionOutcome {
            add_ssts: Vec::new(),
            remove_ssts: vec![remove.clone()],
            target_level: 0,
            wal_segments: None,
            tombstone_watermark: None,
            outputs: Vec::new(),
            obsolete_sst_ids: vec![remove.id().clone()],
            wal_floor: Some(wal_floor.clone()),
            obsolete_wal_segments: Vec::new(),
        };
        let edits = outcome.to_version_edits();
        assert_eq!(edits.len(), 2);
        assert!(matches!(
            &edits[0],
            VersionEdit::RemoveSsts { sst_ids, .. }
                if sst_ids.len() == 1 && sst_ids.contains(remove.id())
        ));
        assert!(matches!(
            &edits[1],
            VersionEdit::SetWalSegments { segments }
                if segments.len() == 1 && segments[0].seq() == wal_floor.seq()
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn local_executor_merges_outputs() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let tmpdir = tempfile::tempdir().expect("temp dir");
        let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
            .primary_key("id")
            .build()
            .expect("schema builder");
        let fs: Arc<dyn DynFs> = Arc::new(fusio::disk::LocalFs {});
        let cfg = Arc::new(
            SsTableConfig::new(
                Arc::clone(&mode_cfg.schema),
                fs,
                Path::from(tmpdir.path().to_string_lossy().to_string()),
            )
            .with_key_extractor(Arc::clone(&mode_cfg.extractor)),
        );
        let exec = LocalCompactionExecutor::new(cfg, 100);
        let batch = crate::test_util::build_batch(
            Arc::clone(&schema),
            vec![DynRow(vec![
                Some(DynCell::Str("a".into())),
                Some(DynCell::I32(1)),
            ])],
        )
        .expect("batch");
        let immutable =
            segment_from_batch_with_key_name(batch, "id").expect("immutable segment from batch");
        let mut builder = SsTableBuilder::<DynMode>::new(
            Arc::clone(&exec.config),
            SsTableDescriptor::new(SsTableId::new(1), 0),
        );
        builder.add_immutable(&immutable).expect("stage seg");
        let input = builder.finish().await.expect("sst").descriptor().clone();
        let job = CompactionJob {
            task: CompactionTask {
                source_level: 0,
                target_level: 1,
                input: vec![CompactionInput {
                    level: 0,
                    sst_id: SsTableId::new(1),
                }],
                key_range: None,
            },
            inputs: vec![input],
            lease: None,
        };
        let out = exec.execute(job).await.expect("outcome");
        assert_eq!(out.add_ssts.len(), 1);
        assert_eq!(out.target_level, 1);
    }
}
