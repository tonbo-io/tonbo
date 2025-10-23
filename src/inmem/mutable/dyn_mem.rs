use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use super::{
    MutableLayout,
    memtable::{DynLayout, DynRowScan},
};
use crate::{
    inmem::{
        immutable::memtable::ImmutableMemTable,
        policy::{MemStats, StatsProvider},
    },
    mvcc::Timestamp,
    record::extract::{DynKeyExtractor, KeyDyn, KeyExtractError},
    scan::RangeSet,
};

/// Opaque dynamic mutable store for dynamic-mode DBs.
///
/// This wraps the internal `DynLayout` to avoid exposing private types via the
/// public `Mode` trait while preserving performance and behavior.
pub struct DynMem(pub(crate) DynLayout);

impl DynMem {
    pub(crate) fn new() -> Self {
        Self(DynLayout::new())
    }

    pub(crate) fn insert_batch(
        &mut self,
        extractor: &dyn DynKeyExtractor,
        batch: RecordBatch,
        commit_ts: Timestamp,
    ) -> Result<(), KeyExtractError> {
        self.0.insert_batch(extractor, batch, commit_ts)
    }

    pub(crate) fn insert_batch_with_ts<F>(
        &mut self,
        extractor: &dyn DynKeyExtractor,
        batch: RecordBatch,
        commit_ts: Timestamp,
        tombstone_at: F,
    ) -> Result<(), KeyExtractError>
    where
        F: FnMut(usize) -> bool,
    {
        self.0
            .insert_batch_with_ts(extractor, batch, commit_ts, tombstone_at)
    }

    pub(crate) fn seal_into_immutable(
        &mut self,
        schema: &SchemaRef,
    ) -> Result<Option<ImmutableMemTable<KeyDyn, RecordBatch>>, KeyExtractError> {
        self.0.seal_into_immutable(schema)
    }

    pub(crate) fn scan_rows<'t, 's>(&'t self, ranges: &'s RangeSet<KeyDyn>) -> DynRowScan<'t, 's> {
        self.0.scan_rows(ranges)
    }

    pub(crate) fn scan_rows_at<'t, 's>(
        &'t self,
        ranges: &'s RangeSet<KeyDyn>,
        read_ts: Timestamp,
    ) -> DynRowScan<'t, 's> {
        self.0.scan_rows_at(ranges, read_ts)
    }
}

impl MutableLayout<KeyDyn> for DynMem {
    fn approx_bytes(&self) -> usize {
        self.0.approx_bytes()
    }
}

impl StatsProvider for DynMem {
    fn build_stats(&self, since_last_seal: Option<std::time::Duration>) -> MemStats {
        self.0.build_stats(since_last_seal)
    }
}
