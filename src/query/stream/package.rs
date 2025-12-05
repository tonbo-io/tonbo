//! Package merged rows into Arrow record batches.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::Stream;
use pin_project_lite::pin_project;
use predicate::{
    ComparisonOp, Operand, Predicate, PredicateNode, PredicateVisitor, ScalarValue, VisitOutcome,
};
use thiserror::Error;
use typed_arrow_dyn::{DynBuilders, DynProjection, DynRow, DynSchema};

use crate::{
    inmem::immutable::memtable::RecordBatchStorage,
    query::stream::{StreamError, merge::MergeStream},
};

pin_project! {
    /// Stream adapter that batches merged rows into `RecordBatch` chunks.
    pub struct PackageStream<'t, S>
    where
        S: RecordBatchStorage,
    {
        row_count: usize,
        batch_size: usize,
        #[pin]
        inner: MergeStream<'t, S>,
        builder: DynRecordBatchBuilder,
    residual_predicate: Option<Predicate>,
    residual: Option<ResidualEvaluator>,
        scan_schema: SchemaRef,
        scan_dyn_schema: DynSchema,
        projection: Option<DynProjection>,
    }
}

impl<'t, S> PackageStream<'t, S>
where
    S: RecordBatchStorage,
{
    pub(crate) fn new(
        batch_size: usize,
        merge: MergeStream<'t, S>,
        scan_schema: SchemaRef,
        result_schema: SchemaRef,
        residual_predicate: Option<Predicate>,
    ) -> Result<Self, StreamError> {
        assert!(batch_size > 0, "batch size must be greater than zero");
        let residual = residual_predicate
            .as_ref()
            .map(|_| ResidualEvaluator::new(&scan_schema));
        let projection = if scan_schema.as_ref() == result_schema.as_ref() {
            None
        } else {
            Some(DynProjection::from_schema(
                scan_schema.as_ref(),
                result_schema.as_ref(),
            )?)
        };
        Ok(Self {
            row_count: 0,
            batch_size,
            inner: merge,
            builder: DynRecordBatchBuilder::new(result_schema, batch_size),
            residual_predicate,
            residual,
            scan_schema: Arc::clone(&scan_schema),
            scan_dyn_schema: DynSchema::from_ref(scan_schema),
            projection,
        })
    }
}

impl<'t, S> Stream for PackageStream<'t, S>
where
    S: RecordBatchStorage,
{
    type Item = Result<RecordBatch, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let mut upstream_done = false;

        while *this.row_count < *this.batch_size {
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(entry))) => {
                    if let Some(row) = entry.into_row() {
                        if let (Some(predicate), Some(evaluator)) =
                            (this.residual_predicate.as_ref(), this.residual.as_ref())
                        {
                            match evaluator.matches_owned(predicate, &row) {
                                Ok(true) => {}
                                Ok(false) => continue,
                                Err(err) => {
                                    return Poll::Ready(Some(Err(err.into())));
                                }
                            }
                        }
                        let projected = if let Some(projection) = this.projection.as_ref() {
                            // Row is already owned, use it directly
                            let mut builders = DynBuilders::new(Arc::clone(&*this.scan_schema), 1);
                            builders.append_option_row(Some(row))?;
                            let batch = builders.try_finish_into_batch()?;
                            let raw =
                                projection.project_row_raw(&*this.scan_dyn_schema, &batch, 0)?;
                            raw.into_owned().map_err(StreamError::from)?
                        } else {
                            // Row is already owned
                            row
                        };
                        if let Err(err) = this.builder.append_row(projected) {
                            return Poll::Ready(Some(Err(err)));
                        }
                        *this.row_count += 1;
                        continue;
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => {
                    upstream_done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        if *this.row_count == 0 {
            return if upstream_done {
                Poll::Ready(None)
            } else {
                Poll::Pending
            };
        }

        let batch = match this.builder.finish_batch() {
            Ok(batch) => batch,
            Err(err) => return Poll::Ready(Some(Err(err))),
        };
        *this.row_count = 0;

        Poll::Ready(Some(Ok(batch)))
    }
}

struct DynRecordBatchBuilder {
    schema: SchemaRef,
    batch_size: usize,
    builders: DynBuilders,
}

impl DynRecordBatchBuilder {
    fn new(schema: SchemaRef, batch_size: usize) -> Self {
        let builders = DynBuilders::new(schema.clone(), batch_size);
        Self {
            schema,
            batch_size,
            builders,
        }
    }

    fn append_row(&mut self, row: DynRow) -> Result<(), StreamError> {
        self.builders.append_option_row(Some(row))?;
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<RecordBatch, StreamError> {
        let builders = std::mem::replace(
            &mut self.builders,
            DynBuilders::new(self.schema.clone(), self.batch_size),
        );
        Ok(builders.try_finish_into_batch()?)
    }
}

#[derive(Debug, Error)]
pub enum ResidualError {
    #[error("column {0} not found in projection")]
    MissingColumn(Arc<str>),
    #[error("unsupported column type for predicate evaluation")]
    UnsupportedColumn,
    #[error("invalid utf8 data")]
    InvalidUtf8,
    #[error("predicate evaluation returned no value")]
    MissingValue,
    #[error("predicate evaluation produced a residual clause")]
    UnexpectedResidual,
}

struct ResidualEvaluator {
    column_map: HashMap<Arc<str>, usize>,
}

impl ResidualEvaluator {
    fn new(schema: &SchemaRef) -> Self {
        let column_map = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| (Arc::<str>::from(field.name().as_str()), idx))
            .collect();
        Self { column_map }
    }

    fn matches_owned(&self, predicate: &Predicate, row: &DynRow) -> Result<bool, ResidualError> {
        let mut visitor = ResidualOwnedRowVisitor {
            row,
            column_map: &self.column_map,
        };
        let outcome = predicate.accept(&mut visitor)?;
        if outcome.residual.is_some() {
            return Err(ResidualError::UnexpectedResidual);
        }
        outcome.value.ok_or(ResidualError::MissingValue)
    }
}

struct ResidualOwnedRowVisitor<'a> {
    row: &'a DynRow,
    column_map: &'a HashMap<Arc<str>, usize>,
}

impl<'a> ResidualOwnedRowVisitor<'a> {
    fn resolve_operand(&self, operand: &Operand) -> Result<ScalarValue, ResidualError> {
        match operand {
            Operand::Literal(literal) => Ok(literal.clone()),
            Operand::Column(column) => {
                let idx = column
                    .index
                    .or_else(|| self.column_map.get(column.name.as_ref()).copied())
                    .ok_or_else(|| ResidualError::MissingColumn(Arc::clone(&column.name)))?;
                let cell = self
                    .row
                    .0
                    .get(idx)
                    .ok_or_else(|| ResidualError::MissingColumn(Arc::clone(&column.name)))?;
                match cell {
                    None => Ok(ScalarValue::null()),
                    Some(c) => convert_owned_cell(c),
                }
            }
        }
    }

    fn require_value(&self, outcome: VisitOutcome<bool>) -> Result<bool, ResidualError> {
        if outcome.residual.is_some() {
            return Err(ResidualError::UnexpectedResidual);
        }
        outcome.value.ok_or(ResidualError::MissingValue)
    }
}

impl<'a> PredicateVisitor for ResidualOwnedRowVisitor<'a> {
    type Error = ResidualError;
    type Value = bool;

    fn visit_leaf(
        &mut self,
        leaf: &PredicateNode,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
        let result = match leaf {
            PredicateNode::Compare { left, op, right } => {
                let lhs = self.resolve_operand(left)?;
                let rhs = self.resolve_operand(right)?;
                match op {
                    ComparisonOp::Equal => lhs == rhs,
                    ComparisonOp::NotEqual => lhs != rhs,
                    ComparisonOp::LessThan => lhs
                        .compare(&rhs)
                        .map(|ord| ord == std::cmp::Ordering::Less)
                        .unwrap_or(false),
                    ComparisonOp::LessThanOrEqual => lhs
                        .compare(&rhs)
                        .map(|ord| ord != std::cmp::Ordering::Greater)
                        .unwrap_or(false),
                    ComparisonOp::GreaterThan => lhs
                        .compare(&rhs)
                        .map(|ord| ord == std::cmp::Ordering::Greater)
                        .unwrap_or(false),
                    ComparisonOp::GreaterThanOrEqual => lhs
                        .compare(&rhs)
                        .map(|ord| ord != std::cmp::Ordering::Less)
                        .unwrap_or(false),
                }
            }
            PredicateNode::InList {
                expr,
                list,
                negated,
            } => {
                let value = self.resolve_operand(expr)?;
                if value.is_null() {
                    *negated
                } else {
                    let contains = list.contains(&value);
                    if *negated { !contains } else { contains }
                }
            }
            PredicateNode::IsNull { expr, negated } => {
                let value = self.resolve_operand(expr)?;
                if *negated {
                    !value.is_null()
                } else {
                    value.is_null()
                }
            }
            PredicateNode::Not(_) | PredicateNode::And(_) | PredicateNode::Or(_) => {
                unreachable!("visit_leaf only handles terminal variants")
            }
        };
        Ok(VisitOutcome::value(result))
    }

    fn combine_not(
        &mut self,
        _original: &Predicate,
        child: VisitOutcome<Self::Value>,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
        let value = self.require_value(child)?;
        Ok(VisitOutcome::value(!value))
    }

    fn combine_and(
        &mut self,
        _original: &Predicate,
        children: Vec<VisitOutcome<Self::Value>>,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
        for outcome in children {
            let value = self.require_value(outcome)?;
            if !value {
                return Ok(VisitOutcome::value(false));
            }
        }
        Ok(VisitOutcome::value(true))
    }

    fn combine_or(
        &mut self,
        _original: &Predicate,
        children: Vec<VisitOutcome<Self::Value>>,
    ) -> Result<VisitOutcome<Self::Value>, Self::Error> {
        for outcome in children {
            let value = self.require_value(outcome)?;
            if value {
                return Ok(VisitOutcome::value(true));
            }
        }
        Ok(VisitOutcome::value(false))
    }
}

fn convert_owned_cell(cell: &typed_arrow_dyn::DynCell) -> Result<ScalarValue, ResidualError> {
    use typed_arrow_dyn::DynCell;
    match cell {
        DynCell::Str(s) => Ok(ScalarValue::from(s.to_string())),
        DynCell::Bin(b) => Ok(ScalarValue::from(b.clone())),
        DynCell::Bool(v) => Ok(ScalarValue::from(*v)),
        DynCell::I8(v) => Ok(ScalarValue::from(*v as i64)),
        DynCell::I16(v) => Ok(ScalarValue::from(*v as i64)),
        DynCell::I32(v) => Ok(ScalarValue::from(*v as i64)),
        DynCell::I64(v) => Ok(ScalarValue::from(*v)),
        DynCell::U8(v) => Ok(ScalarValue::from(*v as u64)),
        DynCell::U16(v) => Ok(ScalarValue::from(*v as u64)),
        DynCell::U32(v) => Ok(ScalarValue::from(*v as u64)),
        DynCell::U64(v) => Ok(ScalarValue::from(*v)),
        DynCell::F32(v) => Ok(ScalarValue::from(*v as f64)),
        DynCell::F64(v) => Ok(ScalarValue::from(*v)),
        DynCell::Null => Ok(ScalarValue::null()),
        _ => Err(ResidualError::UnsupportedColumn),
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::{StreamExt, executor::block_on};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        extractor::projection_for_field,
        inmem::mutable::memtable::DynMem,
        mvcc::Timestamp,
        query::{
            ColumnRef, Predicate, ScalarValue,
            stream::{Order, ScanStream, merge::MergeStream},
        },
        test::build_batch,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn package_stream_emits_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let extractor = projection_for_field(schema.clone(), 0).expect("extractor");
        let mutable = DynMem::new(schema.clone());

        let rows = (0..5)
            .map(|idx| {
                DynRow(vec![
                    Some(DynCell::Str(format!("k{idx}").into())),
                    Some(DynCell::I64(idx as i64)),
                ])
            })
            .collect();
        let batch = build_batch(schema.clone(), rows).expect("batch");
        mutable
            .insert_batch(extractor.as_ref(), batch, Timestamp::new(1))
            .expect("insert batch");

        let mutable_scan = mutable
            .scan_visible(None, Timestamp::MAX)
            .expect("scan rows");
        let merge = MergeStream::from_vec(
            vec![ScanStream::<'_, RecordBatch>::from(mutable_scan)],
            Timestamp::MAX,
            None,
            Some(Order::Asc),
        )
        .await
        .expect("merge stream");

        let mut stream = Box::pin(
            PackageStream::new(2, merge, Arc::clone(&schema), Arc::clone(&schema), None)
                .expect("package stream"),
        );
        let batches = block_on(async {
            let mut out = Vec::new();
            while let Some(batch) = stream.next().await {
                out.push(batch.expect("batch ok"));
            }
            out
        });

        assert_eq!(batches.len(), 3, "expected three batches");
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 2);
        assert_eq!(batches[2].num_rows(), 1);

        let mut collected = Vec::new();
        for batch in batches {
            let array = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int column");
            for i in 0..array.len() {
                collected.push(array.value(i));
            }
        }
        assert_eq!(collected, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn package_stream_applies_residual_predicate() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let extractor = projection_for_field(schema.clone(), 0).expect("extractor");
        let mutable = DynMem::new(schema.clone());

        let rows = vec![
            DynRow(vec![
                Some(DynCell::Str("keep".into())),
                Some(DynCell::I64(10)),
            ]),
            DynRow(vec![
                Some(DynCell::Str("drop".into())),
                Some(DynCell::I64(-5)),
            ]),
        ];
        let batch = build_batch(schema.clone(), rows).expect("batch");
        mutable
            .insert_batch(extractor.as_ref(), batch, Timestamp::new(1))
            .expect("insert batch");

        let predicate = Predicate::gt(ColumnRef::new("v", None), ScalarValue::from(0i64));

        let mutable_scan = mutable
            .scan_visible(None, Timestamp::MAX)
            .expect("scan rows");
        let merge = block_on(MergeStream::from_vec(
            vec![ScanStream::<'_, RecordBatch>::from(mutable_scan)],
            Timestamp::MAX,
            None,
            Some(Order::Asc),
        ))
        .expect("merge stream");

        let mut stream = Box::pin(
            PackageStream::new(
                1,
                merge,
                Arc::clone(&schema),
                Arc::clone(&schema),
                Some(predicate),
            )
            .expect("package stream"),
        );
        let collected = block_on(async {
            let mut out = Vec::new();
            while let Some(batch) = stream.next().await {
                let batch = batch.expect("batch ok");
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("string column");
                for i in 0..ids.len() {
                    out.push(ids.value(i).to_string());
                }
            }
            out
        });

        assert_eq!(collected, vec!["keep"]);
    }

    #[test]
    fn package_stream_surfaces_predicate_errors() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let extractor = projection_for_field(schema.clone(), 0).expect("extractor");
        let mutable = DynMem::new(schema.clone());

        let rows = vec![DynRow(vec![
            Some(DynCell::Str("only".into())),
            Some(DynCell::I64(1)),
        ])];
        let batch = build_batch(schema.clone(), rows).expect("batch");
        mutable
            .insert_batch(extractor.as_ref(), batch, Timestamp::new(1))
            .expect("insert batch");

        // Predicate references a missing column.
        let predicate = Predicate::gt(ColumnRef::new("missing", None), ScalarValue::from(0i64));

        let mutable_scan = mutable
            .scan_visible(None, Timestamp::MAX)
            .expect("scan rows");
        let merge = block_on(MergeStream::from_vec(
            vec![ScanStream::<'_, RecordBatch>::from(mutable_scan)],
            Timestamp::MAX,
            None,
            Some(Order::Asc),
        ))
        .expect("merge stream");

        let mut stream = Box::pin(
            PackageStream::new(
                1,
                merge,
                Arc::clone(&schema),
                Arc::clone(&schema),
                Some(predicate),
            )
            .expect("package stream"),
        );
        let err = block_on(async {
            stream
                .next()
                .await
                .expect("stream should yield error")
                .expect_err("expected error")
        });
        match err {
            StreamError::Predicate(ResidualError::MissingColumn(column)) => {
                assert_eq!(column.as_ref(), "missing");
            }
            other => panic!("unexpected error {other:?}"),
        }
    }
}
