//! Aisle-based pruning implementation.
//!
//! Keeps Aisle dependencies isolated within the pruning layer.

use std::collections::HashMap;

use aisle::{AsyncBloomFilterProvider, PruneRequest, PruneResult};
use parquet::{bloom_filter::Sbbf, file::metadata::ParquetMetaData};

use super::{adapter::to_aisle_expr, config::PruningConfig};
use crate::{
    db::StorageBackend,
    pruning::{PruneBloomProvider, PruneError, PruneFuture, PruneInput, PruneOutput, Pruner},
};

/// Aisle-based pruner with internal configuration.
#[derive(Clone, Debug)]
pub(crate) struct AislePruner {
    config: PruningConfig,
}

impl AislePruner {
    /// Create a new Aisle pruner with explicit configuration.
    pub(crate) fn new(config: PruningConfig) -> Self {
        Self { config }
    }

    fn effective_config(&self) -> PruningConfig {
        self.config.effective()
    }

    fn read_all(metadata: &ParquetMetaData) -> PruneOutput {
        let row_groups = (0..metadata.num_row_groups()).collect();
        PruneOutput {
            row_groups,
            row_selection: None,
        }
    }

    fn into_output(result: PruneResult) -> PruneOutput {
        PruneOutput {
            row_groups: result.row_groups().to_vec(),
            row_selection: result.row_selection().cloned(),
        }
    }
}

impl Default for AislePruner {
    fn default() -> Self {
        Self::new(PruningConfig::default())
    }
}

struct BloomProviderAdapter<'a> {
    inner: &'a mut dyn PruneBloomProvider,
}

impl AsyncBloomFilterProvider for BloomProviderAdapter<'_> {
    async fn bloom_filter(&mut self, row_group_idx: usize, column_idx: usize) -> Option<Sbbf> {
        self.inner.bloom_filter(row_group_idx, column_idx).await
    }

    async fn bloom_filters_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> HashMap<(usize, usize), Sbbf> {
        self.inner.bloom_filters_batch(requests).await
    }
}

impl Pruner for AislePruner {
    fn prune<'a>(&self, mut input: PruneInput<'a>) -> PruneFuture<'a> {
        let config = self.effective_config();
        Box::pin(async move {
            if config.no_pruning {
                return Ok(Self::read_all(input.metadata));
            }

            let Some(predicate) = input.predicate else {
                return Ok(Self::read_all(input.metadata));
            };

            let Some(expr) = to_aisle_expr(predicate) else {
                return Ok(Self::read_all(input.metadata));
            };

            let enable_bloom = config.enable_bloom
                && matches!(input.storage_kind, StorageBackend::S3)
                && input.bloom_provider.is_some();

            let request = PruneRequest::new(input.metadata, input.schema)
                .with_predicate(&expr)
                .enable_page_index(config.enable_page_index)
                .enable_bloom_filter(enable_bloom)
                .emit_roaring(false);

            let result = if enable_bloom {
                let Some(provider) = input.bloom_provider.take() else {
                    return Err(PruneError::message(
                        "bloom provider missing for async pruning",
                    ));
                };
                let mut adapter = BloomProviderAdapter { inner: provider };
                request.prune_async(&mut adapter).await
            } else {
                request.prune()
            };

            Ok(Self::into_output(result))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use tonbo_predicate::{ColumnRef, Predicate, ScalarValue};

    use super::*;

    fn build_metadata() -> (ParquetMetaData, Arc<Schema>) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids = Int64Array::from(vec![1, 2, 100, 200]);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).expect("batch");

        let props = WriterProperties::builder()
            .set_max_row_group_size(2)
            .build();
        let cursor = Cursor::new(Vec::new());
        let mut writer =
            ArrowWriter::try_new(cursor, Arc::clone(&schema), Some(props)).expect("writer");
        writer.write(&batch).expect("write");
        let metadata = writer.close().expect("close");
        (metadata, schema)
    }

    #[test]
    fn prunes_supported_predicate() {
        let (metadata, schema) = build_metadata();
        let predicate = Predicate::lt(ColumnRef::new("id"), ScalarValue::from(10_i64));
        let pruner = AislePruner::new(PruningConfig {
            enable_page_index: false,
            ..PruningConfig::default()
        });

        let input = PruneInput {
            metadata: &metadata,
            schema: &schema,
            predicate: Some(&predicate),
            storage_kind: StorageBackend::Disk,
            bloom_provider: None,
        };

        let output = futures::executor::block_on(pruner.prune(input)).expect("prune");
        assert_eq!(output.row_groups, vec![0]);
        assert!(output.row_selection.is_none());
    }

    #[test]
    fn unsupported_predicate_reads_all() {
        let (metadata, schema) = build_metadata();
        let predicate = Predicate::eq(ColumnRef::new("a"), ColumnRef::new("b"));
        let pruner = AislePruner::default();

        let input = PruneInput {
            metadata: &metadata,
            schema: &schema,
            predicate: Some(&predicate),
            storage_kind: StorageBackend::Disk,
            bloom_provider: None,
        };

        let output = futures::executor::block_on(pruner.prune(input)).expect("prune");
        assert_eq!(output.row_groups, vec![0, 1]);
    }
}
