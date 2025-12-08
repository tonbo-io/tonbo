//! Merge pipeline for SSTables.
//!
//! This module hosts the streaming merge implementation used by major
//! compaction. It stays separate from `sstable.rs` to keep the writer/reader
//! surface focused while merge-specific plumbing evolves.

use std::{
    collections::BinaryHeap,
    sync::{Arc, atomic::AtomicU64},
};

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fusio::executor::Executor;
use futures::StreamExt;
#[cfg(all(test, feature = "tokio"))]
use futures::stream;
use parquet::errors::ParquetError;

use super::sstable::{
    SsTable, SsTableConfig, SsTableDescriptor, SsTableError, SsTableId, SsTableReader,
    take_record_batch,
};
use crate::{
    extractor::{KeyExtractError, KeyProjection, projection_for_columns},
    id::FileId,
    inmem::immutable::memtable::{DeleteSidecar, MVCC_COMMIT_COL},
    key::KeyOwned,
    mvcc::Timestamp,
    ondisk::sstable::ParquetTableWriter,
};

const MVCC_SEQUENCE_COL: &str = "_sequence";

/// Combined row payload (data + MVCC + delete sidecar) yielded by an SST reader.
pub struct SsTableStreamBatch {
    /// User data batch for this SST slice (includes `_commit_ts` column).
    pub data: RecordBatch,
    /// Optional delete sidecar batch for tombstones.
    pub delete: Option<RecordBatch>,
}

/// Merge source that streams ordered batches from a single SST.
pub struct SsTableMergeSource {
    /// Ordered batches produced by the SST reader.
    stream: futures::stream::BoxStream<'static, Result<SsTableStreamBatch, SsTableError>>,
}

impl SsTableMergeSource {
    /// Create a merge source from an SSTable descriptor and shared config.
    pub async fn new<E>(
        config: Arc<SsTableConfig>,
        descriptor: SsTableDescriptor,
        executor: E,
    ) -> Result<Self, SsTableError>
    where
        E: Executor + Clone + 'static,
    {
        let reader = SsTableReader::open(config, descriptor).await?;
        let stream = reader.into_stream(Timestamp::MAX, None, executor).await?;
        Ok(Self { stream })
    }

    /// Replace the current buffer with pre-fetched batches (useful for tests).
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn with_batches(batches: Vec<SsTableStreamBatch>) -> Self
    where
        SsTableStreamBatch: Send,
    {
        let stream = stream::iter(batches.into_iter().map(Ok)).boxed();
        Self {
            stream: Box::pin(stream),
        }
    }

    /// Fetch the next ordered record batch slice for merge consumption.
    pub async fn next(&mut self) -> Result<Option<SsTableStreamBatch>, SsTableError> {
        self.stream.next().await.transpose()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RowKind {
    Data,
    Delete,
}

#[derive(Clone, Debug)]
struct HeapEntry {
    key: KeyOwned,
    commit_ts: Timestamp,
    seq: u64,
    kind: RowKind,
    source_idx: usize,
    row_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.commit_ts == other.commit_ts
            && self.seq == other.seq
            && self.kind == other.kind
            && self.source_idx == other.source_idx
            && self.row_idx == other.row_idx
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match self.key.cmp(&other.key) {
            Ordering::Less => Ordering::Greater,
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => match self.commit_ts.cmp(&other.commit_ts) {
                Ordering::Equal => match self.seq.cmp(&other.seq) {
                    Ordering::Equal => match (self.kind, other.kind) {
                        (RowKind::Delete, RowKind::Data) => Ordering::Greater,
                        (RowKind::Data, RowKind::Delete) => Ordering::Less,
                        _ => Ordering::Equal,
                    },
                    other => other,
                },
                other => other,
            },
        }
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct SourceCursor {
    source_idx: usize,
    extractor: Arc<dyn KeyProjection>,
    source: SsTableMergeSource,
    batch: Option<SsTableStreamBatch>,
    data_idx: usize,
    delete_idx: usize,
}

impl SourceCursor {
    fn new(
        source_idx: usize,
        extractor: Arc<dyn KeyProjection>,
        source: SsTableMergeSource,
    ) -> Self {
        Self {
            source_idx,
            extractor,
            source,
            batch: None,
            data_idx: 0,
            delete_idx: 0,
        }
    }

    async fn ensure_batch(&mut self) -> Result<(), SsTableError> {
        if self.batch.is_none() {
            self.batch = self.source.next().await?;
            self.data_idx = 0;
            self.delete_idx = 0;
        }
        Ok(())
    }

    async fn enqueue(
        &mut self,
        kind: RowKind,
        heap: &mut BinaryHeap<HeapEntry>,
        skip_key: Option<&KeyOwned>,
    ) -> Result<(), SsTableError> {
        loop {
            self.ensure_batch().await?;
            let Some(batch) = self.batch.as_ref() else {
                return Ok(());
            };

            match kind {
                RowKind::Data => {
                    if batch.data.num_rows() == 0 {
                        self.data_idx = batch.data.num_rows();
                        return Ok(());
                    }
                    if self.data_idx >= batch.data.num_rows() {
                        return Ok(());
                    }
                    let key = extract_key_at(&batch.data, self.extractor.as_ref(), self.data_idx)?;
                    if skip_key.is_some_and(|k| k == &key) {
                        self.advance(kind);
                        continue;
                    }
                    let commit_ts = commit_ts_at(&batch.data, self.data_idx)?;
                    let seq = sequence_at(&batch.data, self.data_idx)?;
                    heap.push(HeapEntry {
                        key,
                        commit_ts,
                        seq,
                        kind,
                        source_idx: self.source_idx,
                        row_idx: self.data_idx,
                    });
                    return Ok(());
                }
                RowKind::Delete => {
                    let Some(delete_batch) = batch.delete.as_ref() else {
                        return Ok(());
                    };
                    if delete_batch.num_rows() == 0 {
                        self.delete_idx = delete_batch.num_rows();
                        return Ok(());
                    }
                    if self.delete_idx >= delete_batch.num_rows() {
                        return Ok(());
                    }
                    let key = extract_delete_key_at(
                        delete_batch,
                        self.extractor.as_ref(),
                        self.delete_idx,
                    )?;
                    if skip_key.is_some_and(|k| k == &key) {
                        self.advance(kind);
                        continue;
                    }
                    let commit_ts = commit_ts_at_delete(delete_batch, self.delete_idx)?;
                    let seq = sequence_at_delete(delete_batch, self.delete_idx)?;
                    heap.push(HeapEntry {
                        key,
                        commit_ts,
                        seq,
                        kind,
                        source_idx: self.source_idx,
                        row_idx: self.delete_idx,
                    });
                    return Ok(());
                }
            }
        }
    }

    fn advance(&mut self, kind: RowKind) {
        match kind {
            RowKind::Data => self.data_idx += 1,
            RowKind::Delete => self.delete_idx += 1,
        }

        if let Some(batch) = &self.batch {
            let data_done = self.data_idx >= batch.data.num_rows();
            let delete_done = batch
                .delete
                .as_ref()
                .is_none_or(|d| self.delete_idx >= d.num_rows());
            if data_done && delete_done {
                self.batch = None;
                self.data_idx = 0;
                self.delete_idx = 0;
            }
        }
    }

    fn append_data(&self, row_idx: usize, output: &mut OutputState) -> Result<(), SsTableError> {
        let Some(batch) = self.batch.as_ref() else {
            return Ok(());
        };
        let index = [row_idx as u32];
        let data = take_record_batch(&batch.data, &index)?;
        output.push_data(data);
        Ok(())
    }

    fn append_delete(&self, row_idx: usize, output: &mut OutputState) -> Result<(), SsTableError> {
        let Some(batch) = self.batch.as_ref() else {
            return Ok(());
        };
        let Some(delete_batch) = batch.delete.as_ref() else {
            return Ok(());
        };
        let index = [row_idx as u32];
        let delete = take_record_batch(delete_batch, &index)?;
        output.push_delete(delete);
        Ok(())
    }
}

struct OutputState {
    batches: Vec<SsTableStreamBatch>,
    data_parts: Vec<RecordBatch>,
    delete_parts: Vec<RecordBatch>,
    pending_rows: usize,
    rows: usize,
    tombstones: usize,
    bytes: usize,
    min_key: Option<KeyOwned>,
    max_key: Option<KeyOwned>,
    min_commit_ts: Option<Timestamp>,
    max_commit_ts: Option<Timestamp>,
}

impl OutputState {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            data_parts: Vec::new(),
            delete_parts: Vec::new(),
            pending_rows: 0,
            rows: 0,
            tombstones: 0,
            bytes: 0,
            min_key: None,
            max_key: None,
            min_commit_ts: None,
            max_commit_ts: None,
        }
    }

    fn push_data(&mut self, data: RecordBatch) {
        self.pending_rows += data.num_rows();
        self.rows += data.num_rows();
        self.bytes += data.get_array_memory_size();
        self.data_parts.push(data);
    }

    fn push_delete(&mut self, delete: RecordBatch) {
        self.pending_rows += delete.num_rows();
        self.tombstones += delete.num_rows();
        self.bytes += delete.get_array_memory_size();
        self.delete_parts.push(delete);
    }

    fn update_key_bounds(&mut self, key: &KeyOwned) {
        self.min_key = match self.min_key.take() {
            Some(cur) if cur <= *key => Some(cur),
            _ => Some(key.clone()),
        };
        self.max_key = match self.max_key.take() {
            Some(cur) if cur >= *key => Some(cur),
            _ => Some(key.clone()),
        };
    }

    fn update_commit_bounds(&mut self, ts: Timestamp) {
        self.min_commit_ts = match self.min_commit_ts.take() {
            Some(cur) if cur <= ts => Some(cur),
            _ => Some(ts),
        };
        self.max_commit_ts = match self.max_commit_ts.take() {
            Some(cur) if cur >= ts => Some(cur),
            _ => Some(ts),
        };
    }

    fn should_flush_chunk(&self, chunk_rows: usize) -> bool {
        self.pending_rows >= chunk_rows
    }

    fn flush_chunk(&mut self, schema: &SchemaRef) -> Result<(), SsTableError> {
        if self.data_parts.is_empty() && self.delete_parts.is_empty() {
            return Ok(());
        }
        let data_schema = if let Some(first) = self.data_parts.first() {
            first.schema().clone()
        } else {
            let mut fields = schema.fields().to_vec();
            fields.push(Field::new(MVCC_COMMIT_COL, DataType::UInt64, false).into());
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
        };
        let data_batch = if self.data_parts.is_empty() {
            RecordBatch::new_empty(data_schema.clone())
        } else {
            concat_batches(&data_schema, &self.data_parts).map_err(SsTableError::Parquet)?
        };
        let delete_batch = if self.delete_parts.is_empty() {
            None
        } else {
            let schema = self.delete_parts[0].schema();
            Some(concat_batches(&schema, &self.delete_parts).map_err(SsTableError::Parquet)?)
        };
        self.batches.push(SsTableStreamBatch {
            data: data_batch,
            delete: delete_batch,
        });
        self.data_parts.clear();
        self.delete_parts.clear();
        self.pending_rows = 0;
        Ok(())
    }

    fn has_pending(&self) -> bool {
        !self.batches.is_empty()
            || !self.data_parts.is_empty()
            || !self.delete_parts.is_empty()
            || self.rows > 0
            || self.tombstones > 0
    }

    fn exceeds_caps(&self, max_rows: Option<usize>, max_bytes: Option<usize>) -> bool {
        let over_rows = max_rows.map(|cap| self.rows >= cap).unwrap_or(false);
        let over_bytes = max_bytes.map(|cap| self.bytes >= cap).unwrap_or(false);
        over_rows || over_bytes
    }

    async fn finish<E>(
        &mut self,
        descriptor: SsTableDescriptor,
        config: &Arc<SsTableConfig>,
        extractor: &Arc<dyn KeyProjection>,
        wal_ids: &[FileId],
        executor: E,
    ) -> Result<SsTable, SsTableError>
    where
        E: Executor + Clone,
    {
        self.flush_chunk(config.schema())?;

        let mut writer = ParquetTableWriter::new(Arc::clone(config), descriptor);
        if !wal_ids.is_empty() {
            let mut wal = wal_ids.to_vec();
            wal.sort();
            wal.dedup();
            writer.set_wal_ids(Some(wal));
        }

        for batch in &self.batches {
            writer.stage_stream_batch(batch, extractor.as_ref())?;
        }

        writer.finish(executor).await
    }

    fn reset_for_next(&mut self) {
        self.batches.clear();
        self.data_parts.clear();
        self.delete_parts.clear();
        self.pending_rows = 0;
        self.rows = 0;
        self.tombstones = 0;
        self.bytes = 0;
        self.min_key = None;
        self.max_key = None;
        self.min_commit_ts = None;
        self.max_commit_ts = None;
    }
}

/// Planner/executor scaffold for K-way merging SSTables into a new output table.
pub struct SsTableMerger {
    config: Arc<SsTableConfig>,
    inputs: Vec<SsTableDescriptor>,
    target: SsTableDescriptor,
    next_output_id: Option<Arc<AtomicU64>>,
    max_output_rows: Option<usize>,
    max_output_bytes: Option<usize>,
    chunk_rows: usize,
    max_iterations: Option<usize>,
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::sync::{Arc, atomic::AtomicU64};

    use arrow_schema::{DataType, Field, Schema};
    use fusio::{executor::NoopExecutor, path::Path};
    use tempfile::tempdir;
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        inmem::immutable::memtable::segment_from_batch_with_key_name,
        ondisk::sstable::{SsTableBuilder, SsTableConfig, SsTableDescriptor, SsTableId},
        schema::SchemaBuilder,
        test::build_batch,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn iteration_budget_exceeded_returns_error() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let tmpdir = tempdir().expect("temp dir");
        let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
            .primary_key("id")
            .build()
            .expect("schema builder");
        let fs: Arc<dyn fusio::dynamic::DynFs> = Arc::new(fusio::disk::LocalFs {});
        let cfg = Arc::new(
            SsTableConfig::new(
                Arc::clone(&mode_cfg.schema),
                fs,
                Path::from(tmpdir.path().to_string_lossy().to_string()),
            )
            .with_key_extractor(Arc::clone(&mode_cfg.extractor)),
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
        let mut builder = SsTableBuilder::new(
            Arc::clone(&cfg),
            SsTableDescriptor::new(SsTableId::new(1), 0),
        );
        builder.add_immutable(&immutable).expect("stage seg");
        let input = builder.finish(NoopExecutor).await.expect("sst");

        let target = SsTableDescriptor::new(SsTableId::new(10), 1);
        let merger = SsTableMerger::new(Arc::clone(&cfg), vec![input.descriptor().clone()], target)
            .with_output_id_allocator(Arc::new(AtomicU64::new(11)))
            .with_iteration_budget(1);

        match merger.execute(NoopExecutor).await {
            Err(SsTableError::MergeIterationBudgetExceeded { budget }) => {
                assert_eq!(budget, 1);
            }
            Ok(_) => panic!("expected iteration budget error"),
            Err(other) => panic!("unexpected merge error: {other:?}"),
        }
        // Keep tmpdir alive until here.
        drop(tmpdir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn merger_cleans_outputs_on_error() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let tmpdir = tempdir().expect("temp dir");
        let mode_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
            .primary_key("id")
            .build()
            .expect("schema builder");
        let fs: Arc<dyn fusio::dynamic::DynFs> = Arc::new(fusio::disk::LocalFs {});
        let root = Path::from(tmpdir.path().to_string_lossy().to_string());
        let cfg = Arc::new(
            SsTableConfig::new(Arc::clone(&mode_cfg.schema), fs, root.clone())
                .with_key_extractor(Arc::clone(&mode_cfg.extractor)),
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
        let mut builder = SsTableBuilder::new(
            Arc::clone(&cfg),
            SsTableDescriptor::new(SsTableId::new(1), 0),
        );
        builder.add_immutable(&immutable).expect("stage seg");
        let input = builder.finish(NoopExecutor).await.expect("sst");

        // Force an error by using an impossible iteration budget after writing the first row.
        let target = SsTableDescriptor::new(SsTableId::new(10), 1);
        let merger = SsTableMerger::new(
            Arc::clone(&cfg),
            vec![input.descriptor().clone()],
            target.clone(),
        )
        .with_output_caps(Some(1), None)
        .with_chunk_rows(1); // will error when allocator is missing for the second output
        let result = merger.execute(NoopExecutor).await;
        assert!(matches!(result, Err(SsTableError::MissingIdAllocator)));

        // All files should be gone after cleanup.
        let level_dir = cfg.level_dir(target.level()).expect("level dir");
        let mut entries = cfg
            .fs()
            .list(&level_dir)
            .await
            .expect("level dir should exist");
        let mut count = 0usize;
        while let Some(item) = entries.next().await.transpose().expect("stream ok") {
            let name = item.path.as_ref();
            if name.ends_with(".parquet")
                || name.ends_with(".mvcc.parquet")
                || name.ends_with(".delete.parquet")
            {
                count += 1;
            }
        }
        assert_eq!(count, 0, "expected no SST files after cleanup");

        drop(tmpdir);
    }
}

impl SsTableMerger {
    /// Create a merger for the provided inputs and target descriptor.
    pub fn new(
        config: Arc<SsTableConfig>,
        inputs: Vec<SsTableDescriptor>,
        target: SsTableDescriptor,
    ) -> Self {
        debug_assert!(!inputs.is_empty(), "merger requires at least one input");
        let merge_iteration_budget = config.merge_iteration_budget();
        let max_iterations =
            (merge_iteration_budget != usize::MAX).then_some(merge_iteration_budget);
        Self {
            config,
            inputs,
            target,
            next_output_id: None,
            max_output_rows: None,
            max_output_bytes: None,
            chunk_rows: 1024,
            max_iterations,
        }
    }

    /// Provide an allocator for additional output SST ids (needed when splitting outputs).
    pub fn with_output_id_allocator(mut self, next_output_id: Arc<AtomicU64>) -> Self {
        self.next_output_id = Some(next_output_id);
        self
    }

    /// Configure row and byte caps per output SST.
    pub fn with_output_caps(mut self, max_rows: Option<usize>, max_bytes: Option<usize>) -> Self {
        self.max_output_rows = max_rows;
        self.max_output_bytes = max_bytes;
        self
    }

    /// Configure an iteration budget for merge safety; returns an error when exceeded.
    pub fn with_iteration_budget(mut self, max_iterations: usize) -> Self {
        self.max_iterations = Some(max_iterations.max(1));
        self
    }

    /// Set the number of rows to accumulate before flushing a chunk into the output buffer.
    pub fn with_chunk_rows(mut self, chunk_rows: usize) -> Self {
        self.chunk_rows = chunk_rows.max(1);
        self
    }

    /// Execute the merge and return a newly written SSTable.
    pub async fn execute<E>(self, executor: E) -> Result<Vec<SsTable>, SsTableError>
    where
        E: Executor + Clone + 'static,
    {
        let mut written_desc: Vec<SsTableDescriptor> = Vec::new();
        let config = Arc::clone(&self.config);
        let result = self.execute_inner(&mut written_desc, executor).await;
        match result {
            Ok(outputs) => Ok(outputs),
            Err(err) => {
                cleanup_descriptors(&config, &written_desc).await;
                Err(err)
            }
        }
    }

    async fn execute_inner<E>(
        self,
        written_desc: &mut Vec<SsTableDescriptor>,
        executor: E,
    ) -> Result<Vec<SsTable>, SsTableError>
    where
        E: Executor + Clone + 'static,
    {
        let extractor = self
            .config
            .key_extractor()
            .cloned()
            .ok_or(SsTableError::MissingKeyExtractor)?;
        let mut wal_ids: Vec<FileId> = Vec::new();

        let mut sources: Vec<SourceCursor> = Vec::new();
        for (idx, descriptor) in self.inputs.iter().cloned().enumerate() {
            if let Some(ids) = descriptor.wal_ids() {
                wal_ids.extend_from_slice(ids);
            }
            let source =
                SsTableMergeSource::new(Arc::clone(&self.config), descriptor, executor.clone())
                    .await?;
            let mut cursor = SourceCursor::new(idx, Arc::clone(&extractor), source);
            cursor.ensure_batch().await?;
            sources.push(cursor);
        }

        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        for cursor in &mut sources {
            cursor.enqueue(RowKind::Data, &mut heap, None).await?;
            cursor.enqueue(RowKind::Delete, &mut heap, None).await?;
        }

        if heap.is_empty() {
            return Err(SsTableError::EmptyMergeInput);
        }

        let chunk_rows = self.chunk_rows.max(1);
        let mut output_state = OutputState::new();
        let mut outputs: Vec<SsTable> = Vec::new();
        let mut current_descriptor = Some(self.target.clone());
        let mut iterations = 0usize;

        while let Some(entry) = heap.pop() {
            iterations += 1;
            if let Some(max_iters) = self.max_iterations
                && iterations > max_iters
            {
                return Err(SsTableError::MergeIterationBudgetExceeded { budget: max_iters });
            }
            let mut group = vec![entry];
            while let Some(next) = heap.peek() {
                if next.key == group[0].key {
                    group.push(heap.pop().expect("heap pop must succeed"));
                } else {
                    break;
                }
            }

            for candidate in &group {
                output_state.update_commit_bounds(candidate.commit_ts);
            }

            let winner = &group[0];
            let group_key = winner.key.clone();
            output_state.update_key_bounds(&group_key);
            match winner.kind {
                RowKind::Data => {
                    sources[winner.source_idx].append_data(winner.row_idx, &mut output_state)?
                }
                RowKind::Delete => {
                    sources[winner.source_idx].append_delete(winner.row_idx, &mut output_state)?
                }
            }

            for candidate in group {
                sources[candidate.source_idx].advance(candidate.kind);
                sources[candidate.source_idx]
                    .enqueue(candidate.kind, &mut heap, Some(&group_key))
                    .await?;
            }

            if output_state.should_flush_chunk(chunk_rows) {
                output_state.flush_chunk(self.config.schema())?;
            }

            if output_state.exceeds_caps(self.max_output_rows, self.max_output_bytes) {
                let descriptor = current_descriptor.take().ok_or(SsTableError::InvalidPath(
                    "missing target descriptor".into(),
                ))?;
                let table = output_state
                    .finish(
                        descriptor,
                        &self.config,
                        &extractor,
                        &wal_ids,
                        executor.clone(),
                    )
                    .await?;
                written_desc.push(table.descriptor().clone());
                outputs.push(table);
                output_state.reset_for_next();
                let next_descriptor = self.allocate_next_descriptor()?;
                current_descriptor = Some(next_descriptor);
            }
        }

        if output_state.has_pending() {
            let descriptor = current_descriptor.take().ok_or(SsTableError::InvalidPath(
                "missing target descriptor".into(),
            ))?;
            outputs.push(
                output_state
                    .finish(
                        descriptor,
                        &self.config,
                        &extractor,
                        &wal_ids,
                        executor.clone(),
                    )
                    .await?,
            );
            if let Some(desc) = outputs.last().map(|sst| sst.descriptor().clone()) {
                written_desc.push(desc);
            }
        }

        if outputs.is_empty() {
            return Err(SsTableError::EmptyMergeOutput);
        }
        Ok(outputs)
    }

    fn allocate_next_descriptor(&self) -> Result<SsTableDescriptor, SsTableError> {
        let Some(generator) = &self.next_output_id else {
            return Err(SsTableError::MissingIdAllocator);
        };
        let id = generator.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(SsTableDescriptor::new(
            SsTableId::new(id),
            self.target.level(),
        ))
    }
}

pub(crate) fn extract_key_at(
    batch: &RecordBatch,
    extractor: &dyn KeyProjection,
    row: usize,
) -> Result<KeyOwned, SsTableError> {
    if row >= batch.num_rows() {
        return Err(SsTableError::KeyExtract(KeyExtractError::RowOutOfBounds(
            row,
            batch.num_rows(),
        )));
    }
    let keys = extractor.project_view(batch, &[row])?;
    let key_row = keys
        .into_iter()
        .next()
        .ok_or(SsTableError::SidecarMismatch("key projection empty"))?;
    KeyOwned::from_key_row(&key_row).map_err(SsTableError::KeyOwned)
}

pub(crate) fn extract_delete_key_at(
    batch: &RecordBatch,
    extractor: &dyn KeyProjection,
    row: usize,
) -> Result<KeyOwned, SsTableError> {
    let key_len = extractor.key_indices().len();
    let projection = projection_for_columns(batch.schema(), (0..key_len).collect())
        .map_err(SsTableError::KeyExtract)?;
    let keys = projection.project_view(batch, &[row])?;
    let key_row = keys
        .into_iter()
        .next()
        .ok_or(SsTableError::SidecarMismatch("delete key projection empty"))?;
    KeyOwned::from_key_row(&key_row).map_err(SsTableError::KeyOwned)
}

pub(crate) fn commit_ts_at(batch: &RecordBatch, row: usize) -> Result<Timestamp, SsTableError> {
    let commit_col = batch
        .column_by_name(MVCC_COMMIT_COL)
        .and_then(|arr| arr.as_any().downcast_ref::<UInt64Array>())
        .ok_or(SsTableError::SidecarMismatch("commit_ts column missing"))?;
    if row >= commit_col.len() {
        return Err(SsTableError::KeyExtract(KeyExtractError::RowOutOfBounds(
            row,
            commit_col.len(),
        )));
    }
    Ok(Timestamp::new(commit_col.value(row)))
}

pub(crate) fn commit_ts_at_delete(
    batch: &RecordBatch,
    row: usize,
) -> Result<Timestamp, SsTableError> {
    let commit_col = batch
        .column_by_name(MVCC_COMMIT_COL)
        .and_then(|arr| arr.as_any().downcast_ref::<UInt64Array>())
        .ok_or(SsTableError::SidecarMismatch(
            "delete sidecar missing commit_ts column",
        ))?;
    if row >= commit_col.len() {
        return Err(SsTableError::KeyExtract(KeyExtractError::RowOutOfBounds(
            row,
            commit_col.len(),
        )));
    }
    Ok(Timestamp::new(commit_col.value(row)))
}

pub(crate) fn sequence_at(batch: &RecordBatch, row: usize) -> Result<u64, SsTableError> {
    if let Some(seq_col) = batch
        .column_by_name(MVCC_SEQUENCE_COL)
        .and_then(|arr| arr.as_any().downcast_ref::<UInt64Array>())
    {
        if row >= seq_col.len() {
            return Err(SsTableError::KeyExtract(KeyExtractError::RowOutOfBounds(
                row,
                seq_col.len(),
            )));
        }
        return Ok(seq_col.value(row));
    }
    Ok(row as u64)
}

pub(crate) fn sequence_at_delete(batch: &RecordBatch, row: usize) -> Result<u64, SsTableError> {
    if let Some(seq_col) = batch
        .column_by_name(MVCC_SEQUENCE_COL)
        .and_then(|arr| arr.as_any().downcast_ref::<UInt64Array>())
    {
        if row >= seq_col.len() {
            return Err(SsTableError::KeyExtract(KeyExtractError::RowOutOfBounds(
                row,
                seq_col.len(),
            )));
        }
        return Ok(seq_col.value(row));
    }
    Ok(row as u64)
}

pub(crate) fn decode_delete_sidecar(
    batch: &RecordBatch,
    extractor: &dyn KeyProjection,
) -> Result<DeleteSidecar, SsTableError> {
    let key_len = extractor.key_indices().len();
    let key_fields: Vec<_> = batch.schema().fields()[0..key_len]
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let key_columns: Vec<_> = (0..key_len).map(|idx| batch.column(idx).clone()).collect();
    let key_schema = Arc::new(Schema::new(key_fields));
    let keys = RecordBatch::try_new(key_schema, key_columns)
        .map_err(|err| SsTableError::Parquet(ParquetError::ArrowError(err.to_string())))?;
    let commits = batch
        .column_by_name(MVCC_COMMIT_COL)
        .and_then(|arr| arr.as_any().downcast_ref::<UInt64Array>())
        .ok_or(SsTableError::SidecarMismatch(
            "delete sidecar missing commit_ts column",
        ))?;
    let commit_ts: Vec<Timestamp> = (0..commits.len())
        .map(|idx| Timestamp::new(commits.value(idx)))
        .collect();
    Ok(DeleteSidecar::new(keys, commit_ts))
}

pub(crate) fn concat_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<RecordBatch, ParquetError> {
    arrow_select::concat::concat_batches(schema, batches)
        .map_err(|err| ParquetError::ArrowError(err.to_string()))
}

/// Best-effort cleanup of SST files referenced by descriptors.
/// Continues on individual file errors to avoid leaving partial orphans.
pub(crate) async fn cleanup_descriptors(config: &SsTableConfig, descriptors: &[SsTableDescriptor]) {
    let fs = config.fs();
    for desc in descriptors {
        if let Some(path) = desc.data_path() {
            let _ = fs.remove(path).await;
        }
        if let Some(path) = desc.mvcc_path() {
            let _ = fs.remove(path).await;
        }
        if let Some(path) = desc.delete_path() {
            let _ = fs.remove(path).await;
        }
    }
}
