//! No-op pruning implementation.

use crate::pruning::{PruneFuture, PruneInput, PruneOutput, Pruner};

/// Pruner that always scans all row groups.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NoopPruner;

impl NoopPruner {
    fn read_all(input: &PruneInput<'_>) -> PruneOutput {
        let row_groups = (0..input.metadata.num_row_groups()).collect();
        PruneOutput {
            row_groups,
            row_selection: None,
        }
    }
}

impl Pruner for NoopPruner {
    fn prune<'a>(&self, input: PruneInput<'a>) -> PruneFuture<'a> {
        Box::pin(async move { Ok(Self::read_all(&input)) })
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
    use crate::db::StorageBackend;

    fn build_metadata() -> (parquet::file::metadata::ParquetMetaData, Arc<Schema>) {
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
    fn returns_all_row_groups_even_with_predicate() {
        let (metadata, schema) = build_metadata();
        let predicate = Predicate::lt(ColumnRef::new("id"), ScalarValue::from(10_i64));
        let pruner = NoopPruner::default();

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

    #[test]
    fn row_selection_is_none() {
        let (metadata, schema) = build_metadata();
        let pruner = NoopPruner::default();

        let input = PruneInput {
            metadata: &metadata,
            schema: &schema,
            predicate: None,
            storage_kind: StorageBackend::Disk,
            bloom_provider: None,
        };

        let output = futures::executor::block_on(pruner.prune(input)).expect("prune");
        assert!(output.row_selection.is_none());
    }
}
