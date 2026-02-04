use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fusio::{disk::LocalFs, dynamic::DynFs, executor::tokio::TokioExecutor, path::Path};
use typed_arrow_dyn::{DynCell, DynRow};

use super::common::workspace_temp_dir;
use crate::{
    db::{DB, Expr, ScalarValue},
    extractor,
    inmem::policy::BatchesThreshold,
    ondisk::sstable::{SsTableConfig, SsTableDescriptor, SsTableId},
    schema::SchemaBuilder,
    test::{build_batch, compact_merge_l0},
    transaction::Snapshot as TxSnapshot,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Version {
    commit_ts: u64,
    tombstone: bool,
    value: Option<i64>,
}

impl Version {
    fn put(commit_ts: u64, value: i64) -> Self {
        Self {
            commit_ts,
            tombstone: false,
            value: Some(value),
        }
    }

    fn delete(commit_ts: u64) -> Self {
        Self {
            commit_ts,
            tombstone: true,
            value: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VisibleVersion {
    commit_ts: u64,
    tombstone: bool,
    value: Option<i64>,
}

impl From<&Version> for VisibleVersion {
    fn from(version: &Version) -> Self {
        Self {
            commit_ts: version.commit_ts,
            tombstone: version.tombstone,
            value: version.value,
        }
    }
}

#[derive(Default, Debug)]
struct MvccOracle {
    versions: BTreeMap<String, Vec<Version>>,
}

impl MvccOracle {
    // Phase 0 invariants are type-agnostic, so we intentionally keep values as i64 for clarity.
    fn put(&mut self, key: impl Into<String>, commit_ts: u64, value: i64) {
        self.versions
            .entry(key.into())
            .or_default()
            .push(Version::put(commit_ts, value));
    }

    fn delete(&mut self, key: impl Into<String>, commit_ts: u64) {
        self.versions
            .entry(key.into())
            .or_default()
            .push(Version::delete(commit_ts));
    }

    fn visible_version(&self, key: &str, snapshot_ts: u64) -> Option<VisibleVersion> {
        let versions = self.versions.get(key)?;
        let mut best: Option<VisibleVersion> = None;
        for version in versions {
            if version.commit_ts > snapshot_ts {
                continue;
            }
            match &best {
                None => best = Some(VisibleVersion::from(version)),
                Some(best_version) => {
                    if version.commit_ts > best_version.commit_ts {
                        best = Some(VisibleVersion::from(version));
                    } else if version.commit_ts == best_version.commit_ts
                        && version.tombstone
                        && !best_version.tombstone
                    {
                        best = Some(VisibleVersion::from(version));
                    }
                }
            }
        }
        best
    }

    fn get(&self, key: &str, snapshot_ts: u64) -> Option<i64> {
        self.visible_version(key, snapshot_ts).and_then(|version| {
            if version.tombstone {
                None
            } else {
                version.value
            }
        })
    }

    fn scan_range(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        snapshot_ts: u64,
    ) -> Vec<(String, i64)> {
        use std::ops::Bound;

        let start_bound = match start {
            Some(bound) => Bound::Included(bound),
            None => Bound::Unbounded,
        };
        let end_bound = match end {
            Some(bound) => Bound::Included(bound),
            None => Bound::Unbounded,
        };

        let mut rows = Vec::new();
        for (key, _) in self.versions.range::<str, _>((start_bound, end_bound)) {
            if let Some(value) = self.get(key, snapshot_ts) {
                rows.push((key.clone(), value));
            }
        }
        rows
    }
}

struct ScenarioHarness {
    db: DB<LocalFs, TokioExecutor>,
    schema: SchemaRef,
    sst_cfg: Arc<SsTableConfig>,
    next_sst_id: u64,
}

impl ScenarioHarness {
    async fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let db_root = workspace_temp_dir(name);
        let base_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let config = SchemaBuilder::from_schema(Arc::clone(&base_schema))
            .primary_key("id")
            .with_metadata()
            .build()
            .expect("schema builder");
        let schema = Arc::clone(&config.schema);
        let db: DB<LocalFs, TokioExecutor> = DB::<LocalFs, TokioExecutor>::builder(config)
            .on_disk(&db_root)?
            .disable_minor_compaction()
            .build()
            .await?;
        let mut inner = db.into_inner();
        inner.set_seal_policy(Arc::new(BatchesThreshold { batches: 1 }));
        let db = DB::from_inner(Arc::new(inner));

        let extractor = extractor::projection_for_field(Arc::clone(&schema), 0).expect("extractor");
        let sst_root = Path::from_filesystem_path(db_root.join("sst")).expect("sst root path");
        let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let sst_cfg = Arc::new(
            SsTableConfig::new(Arc::clone(&schema), fs, sst_root)
                .with_key_extractor(extractor.into()),
        );

        Ok(Self {
            db,
            schema,
            sst_cfg,
            next_sst_id: 1,
        })
    }

    fn next_commit_ts(&self) -> u64 {
        self.db.inner().commit_clock.peek().get()
    }

    async fn ingest_put(
        &self,
        key: &str,
        value: i64,
        oracle: &mut MvccOracle,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let commit_ts = self.next_commit_ts();
        let batch = row_batch(Arc::clone(&self.schema), key, value)?;
        self.db.ingest(batch).await?;
        oracle.put(key, commit_ts, value);
        Ok(commit_ts)
    }

    async fn ingest_delete(
        &self,
        key: &str,
        oracle: &mut MvccOracle,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let commit_ts = self.next_commit_ts();
        let batch = row_batch(Arc::clone(&self.schema), key, 0)?;
        self.db.ingest_with_tombstones(batch, vec![true]).await?;
        oracle.delete(key, commit_ts);
        Ok(commit_ts)
    }

    async fn flush_immutables_to_l0(&mut self) -> Result<u64, Box<dyn std::error::Error>> {
        let sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        let descriptor = SsTableDescriptor::new(SsTableId::new(sst_id), 0);
        self.db
            .inner()
            .flush_immutables_with_descriptor(Arc::clone(&self.sst_cfg), descriptor)
            .await?;
        Ok(sst_id)
    }

    async fn compact_l0(
        &self,
        sst_ids: Vec<u64>,
        target_level: u32,
    ) -> Result<crate::compaction::executor::CompactionOutcome, Box<dyn std::error::Error>> {
        let start_id = 10_000;
        let outcome = compact_merge_l0(
            self.db.inner().as_ref(),
            sst_ids,
            target_level,
            Arc::clone(&self.sst_cfg),
            start_id,
        )
        .await?;
        Ok(outcome)
    }
}

fn row_batch(
    schema: SchemaRef,
    key: &str,
    value: i64,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let rows = vec![DynRow(vec![
        Some(DynCell::Str(key.into())),
        Some(DynCell::I64(value)),
    ])];
    Ok(build_batch(schema, rows)?)
}

fn collect_rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column");
        for idx in 0..batch.num_rows() {
            rows.push((ids.value(idx).to_string(), values.value(idx)));
        }
    }
    rows
}

async fn scan_all(
    db: &DB<LocalFs, TokioExecutor>,
    snapshot: &TxSnapshot,
) -> Result<Vec<(String, i64)>, Box<dyn std::error::Error>> {
    let batches = snapshot.scan(db).collect().await?;
    Ok(collect_rows(&batches))
}

async fn scan_key(
    db: &DB<LocalFs, TokioExecutor>,
    snapshot: &TxSnapshot,
    key: &str,
) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    let predicate = Expr::eq("id", ScalarValue::from(key));
    let batches = snapshot.scan(db).filter(predicate).collect().await?;
    let mut rows = collect_rows(&batches);
    if rows.is_empty() {
        return Ok(None);
    }
    assert!(
        rows.len() == 1,
        "expected at most one row for key {key}, got {rows:?}"
    );
    Ok(Some(rows.remove(0).1))
}

async fn scan_range(
    db: &DB<LocalFs, TokioExecutor>,
    snapshot: &TxSnapshot,
    start: &str,
    end: &str,
) -> Result<Vec<(String, i64)>, Box<dyn std::error::Error>> {
    let predicate = Expr::between("id", ScalarValue::from(start), ScalarValue::from(end), true);
    let batches = snapshot.scan(db).filter(predicate).collect().await?;
    Ok(collect_rows(&batches))
}

async fn assert_oracle_matches(
    scenario: &str,
    snapshot_ts: u64,
    snapshot: &TxSnapshot,
    oracle: &MvccOracle,
    db: &DB<LocalFs, TokioExecutor>,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_scan = oracle.scan_range(None, None, snapshot_ts);
    let actual_scan = scan_all(db, snapshot).await?;
    assert_eq!(
        actual_scan, expected_scan,
        "scenario={scenario} snapshot_ts={snapshot_ts} scan mismatch: expected={expected_scan:?} \
         actual={actual_scan:?}",
    );

    for key in oracle.versions.keys() {
        let expected = oracle.get(key, snapshot_ts);
        let actual = scan_key(db, snapshot, key).await?;
        assert_eq!(
            actual, expected,
            "scenario={scenario} snapshot_ts={snapshot_ts} key={key} mismatch: \
             expected={expected:?} actual={actual:?}",
        );
    }
    Ok(())
}

async fn assert_range_matches(
    scenario: &str,
    snapshot_ts: u64,
    snapshot: &TxSnapshot,
    oracle: &MvccOracle,
    db: &DB<LocalFs, TokioExecutor>,
    start: &str,
    end: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = oracle.scan_range(Some(start), Some(end), snapshot_ts);
    let actual = scan_range(db, snapshot, start, end).await?;
    assert_eq!(
        actual, expected,
        "scenario={scenario} snapshot_ts={snapshot_ts} range={start}..={end} mismatch: \
         expected={expected:?} actual={actual:?}",
    );
    Ok(())
}

#[test]
fn mvcc_oracle_visible_version_prefers_tombstone() {
    let mut oracle = MvccOracle::default();
    oracle.put("a", 5, 11);
    oracle.delete("a", 5);
    oracle.put("a", 7, 13);
    oracle.put("b", 3, 21);
    oracle.delete("b", 4);
    oracle.put("c", 2, 31);

    assert_eq!(oracle.visible_version("a", 4), None);
    assert_eq!(
        oracle.visible_version("a", 5),
        Some(VisibleVersion {
            commit_ts: 5,
            tombstone: true,
            value: None,
        })
    );
    assert_eq!(oracle.get("a", 6), None);
    assert_eq!(oracle.get("a", 7), Some(13));

    assert_eq!(oracle.get("b", 3), Some(21));
    assert_eq!(oracle.get("b", 4), None);
    assert_eq!(oracle.get("b", 5), None);

    let scan = oracle.scan_range(Some("a"), Some("c"), 6);
    assert_eq!(scan, vec![("c".to_string(), 31)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_overwrite_chain() -> Result<(), Box<dyn std::error::Error>> {
    let scenario = "overwrite_chain";
    let mut harness = ScenarioHarness::new("compaction-correctness-overwrite-chain").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_put("k1", 10, &mut oracle).await?;
    let sst0 = harness.flush_immutables_to_l0().await?;
    let _ts1 = harness.ingest_put("k1", 20, &mut oracle).await?;
    let sst1 = harness.flush_immutables_to_l0().await?;
    let _ts2 = harness.ingest_put("k1", 30, &mut oracle).await?;
    let sst2 = harness.flush_immutables_to_l0().await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();

    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db).await?;

    let outcome = harness.compact_l0(vec![sst0, sst1, sst2], 1).await?;
    assert!(
        !outcome.add_ssts.is_empty(),
        "scenario={scenario} expected compaction to add SSTs"
    );
    assert_eq!(
        outcome.remove_ssts.len(),
        3,
        "scenario={scenario} expected compaction to remove 3 SSTs"
    );
    assert_eq!(
        outcome.target_level, 1,
        "scenario={scenario} expected target level 1"
    );

    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_delete_heavy() -> Result<(), Box<dyn std::error::Error>> {
    let scenario = "delete_heavy";
    let mut harness = ScenarioHarness::new("compaction-correctness-delete-heavy").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_put("k1", 10, &mut oracle).await?;
    let _ts1 = harness.ingest_delete("k1", &mut oracle).await?;
    let sst0 = harness.flush_immutables_to_l0().await?;

    let _ts2 = harness.ingest_put("k2", 20, &mut oracle).await?;
    let _ts3 = harness.ingest_delete("k2", &mut oracle).await?;
    let sst1 = harness.flush_immutables_to_l0().await?;

    let _ts4 = harness.ingest_put("k3", 30, &mut oracle).await?;
    let _ts5 = harness.ingest_delete("k3", &mut oracle).await?;
    let sst2 = harness.flush_immutables_to_l0().await?;

    let _ts6 = harness.ingest_put("k4", 40, &mut oracle).await?;
    let sst3 = harness.flush_immutables_to_l0().await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();

    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db).await?;

    let outcome = harness.compact_l0(vec![sst0, sst1, sst2, sst3], 1).await?;
    assert!(
        !outcome.add_ssts.is_empty(),
        "scenario={scenario} expected compaction to add SSTs"
    );
    assert_eq!(
        outcome.remove_ssts.len(),
        4,
        "scenario={scenario} expected compaction to remove 4 SSTs"
    );
    assert_eq!(
        outcome.target_level, 1,
        "scenario={scenario} expected target level 1"
    );

    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_correctness_range_scan_with_deletes() -> Result<(), Box<dyn std::error::Error>>
{
    let scenario = "range_scan_with_deletes";
    let mut harness = ScenarioHarness::new("compaction-correctness-range-scan").await?;
    let mut oracle = MvccOracle::default();

    let _ts0 = harness.ingest_put("k01", 10, &mut oracle).await?;
    let _ts1 = harness.ingest_put("k02", 20, &mut oracle).await?;
    let _ts2 = harness.ingest_put("k03", 30, &mut oracle).await?;
    let _ts3 = harness.ingest_delete("k02", &mut oracle).await?;
    let sst0 = harness.flush_immutables_to_l0().await?;

    let _ts4 = harness.ingest_put("k04", 40, &mut oracle).await?;
    let _ts5 = harness.ingest_put("k05", 50, &mut oracle).await?;
    let _ts6 = harness.ingest_delete("k05", &mut oracle).await?;
    let _ts7 = harness.ingest_put("k06", 60, &mut oracle).await?;
    let sst1 = harness.flush_immutables_to_l0().await?;

    let snapshot = harness.db.begin_snapshot().await?;
    let snapshot_ts = snapshot.read_view().read_ts().get();

    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db).await?;
    assert_range_matches(
        scenario,
        snapshot_ts,
        &snapshot,
        &oracle,
        &harness.db,
        "k02",
        "k05",
    )
    .await?;

    let outcome = harness.compact_l0(vec![sst0, sst1], 1).await?;
    assert!(
        !outcome.add_ssts.is_empty(),
        "scenario={scenario} expected compaction to add SSTs"
    );
    assert_eq!(
        outcome.remove_ssts.len(),
        2,
        "scenario={scenario} expected compaction to remove 2 SSTs"
    );
    assert_eq!(
        outcome.target_level, 1,
        "scenario={scenario} expected target level 1"
    );

    assert_oracle_matches(scenario, snapshot_ts, &snapshot, &oracle, &harness.db).await?;
    assert_range_matches(
        scenario,
        snapshot_ts,
        &snapshot,
        &oracle,
        &harness.db,
        "k02",
        "k05",
    )
    .await?;

    Ok(())
}
