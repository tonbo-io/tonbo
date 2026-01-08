//! Package merged rows into Arrow record batches.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::ScalarValue;
use fusio::executor::Executor;
use futures::Stream;
use pin_project_lite::pin_project;
use thiserror::Error;
use typed_arrow_dyn::{DynBuilders, DynProjection, DynRow, DynSchema};

use crate::query::{
    CmpOp, Expr,
    stream::{StreamError, merge::MergeStream},
};

pin_project! {
    /// Stream adapter that batches merged rows into `RecordBatch` chunks.
    pub struct PackageStream<'t, E>
    where
        E: Executor,
    {
        row_count: usize,
        batch_size: usize,
        #[pin]
        inner: MergeStream<'t, E>,
        builder: DynRecordBatchBuilder,
        residual_predicate: Option<Expr>,
        residual: Option<ResidualEvaluator>,
        scan_schema: SchemaRef,
        scan_dyn_schema: DynSchema,
        projection: Option<DynProjection>,
        // Row limit applied after predicate evaluation.
        limit: Option<usize>,
        // Total rows emitted across all batches (for limit tracking).
        total_emitted: usize,
    }
}

impl<'t, E> PackageStream<'t, E>
where
    E: Executor + Clone + 'static,
{
    #[cfg(test)]
    pub(crate) fn new(
        batch_size: usize,
        merge: MergeStream<'t, E>,
        scan_schema: SchemaRef,
        result_schema: SchemaRef,
        residual_predicate: Option<Expr>,
    ) -> Result<Self, StreamError> {
        Self::with_limit(
            batch_size,
            merge,
            scan_schema,
            result_schema,
            residual_predicate,
            None,
        )
    }

    /// Create a PackageStream with an optional row limit applied after predicate evaluation.
    pub(crate) fn with_limit(
        batch_size: usize,
        merge: MergeStream<'t, E>,
        scan_schema: SchemaRef,
        result_schema: SchemaRef,
        residual_predicate: Option<Expr>,
        limit: Option<usize>,
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
            limit,
            total_emitted: 0,
        })
    }
}

impl<'t, E> Stream for PackageStream<'t, E>
where
    E: Executor + Clone + 'static,
{
    type Item = Result<RecordBatch, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Check if we've already hit the limit
        if let Some(limit) = *this.limit
            && *this.total_emitted >= limit
        {
            return Poll::Ready(None);
        }

        let mut upstream_done = false;

        // Calculate effective batch size considering remaining limit
        let remaining_limit = this
            .limit
            .map(|l| l.saturating_sub(*this.total_emitted))
            .unwrap_or(usize::MAX);
        let effective_batch_size = (*this.batch_size).min(remaining_limit);

        while *this.row_count < effective_batch_size {
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
        *this.total_emitted += batch.num_rows();
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TriState {
    True,
    False,
    Unknown,
}

impl TriState {
    fn from_bool(value: bool) -> Self {
        if value { Self::True } else { Self::False }
    }

    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }

    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }

    fn is_true(self) -> bool {
        matches!(self, Self::True)
    }
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

    fn matches_owned(&self, predicate: &Expr, row: &DynRow) -> Result<bool, ResidualError> {
        let outcome = self.evaluate_expr(predicate, row)?;
        Ok(outcome.is_true())
    }

    fn evaluate_expr(&self, expr: &Expr, row: &DynRow) -> Result<TriState, ResidualError> {
        match expr {
            Expr::True => Ok(TriState::True),
            Expr::False => Ok(TriState::False),
            Expr::Cmp { column, op, value } => self.evaluate_cmp(column, *op, value, row),
            Expr::Between {
                column,
                low,
                high,
                inclusive,
            } => self.evaluate_between(column, low, high, *inclusive, row),
            Expr::InList { column, values } => self.evaluate_in_list(column, values, row),
            Expr::BloomFilterEq { .. } | Expr::BloomFilterInList { .. } => Ok(TriState::True),
            Expr::StartsWith { column, prefix } => self.evaluate_starts_with(column, prefix, row),
            Expr::IsNull { column, negated } => self.evaluate_is_null(column, *negated, row),
            Expr::And(children) => {
                if children.is_empty() {
                    return Ok(TriState::True);
                }
                let mut result = TriState::True;
                for child in children {
                    result = result.and(self.evaluate_expr(child, row)?);
                    if result == TriState::False {
                        return Ok(result);
                    }
                }
                Ok(result)
            }
            Expr::Or(children) => {
                if children.is_empty() {
                    return Ok(TriState::False);
                }
                let mut result = TriState::False;
                for child in children {
                    result = result.or(self.evaluate_expr(child, row)?);
                    if result == TriState::True {
                        return Ok(result);
                    }
                }
                Ok(result)
            }
            Expr::Not(child) => Ok(self.evaluate_expr(child, row)?.not()),
        }
    }

    fn evaluate_cmp(
        &self,
        column: &str,
        op: CmpOp,
        value: &ScalarValue,
        row: &DynRow,
    ) -> Result<TriState, ResidualError> {
        let Some(lhs) = self.resolve_column(column, row)? else {
            return Ok(TriState::Unknown);
        };
        if lhs.is_null() || value.is_null() {
            return Ok(TriState::Unknown);
        }
        let ordering = compare_scalar_values(&lhs, value);
        let result = match op {
            CmpOp::Eq => ordering.map(|ord| ord == std::cmp::Ordering::Equal),
            CmpOp::NotEq => ordering.map(|ord| ord != std::cmp::Ordering::Equal),
            CmpOp::Lt => ordering.map(|ord| ord == std::cmp::Ordering::Less),
            CmpOp::LtEq => ordering.map(|ord| ord != std::cmp::Ordering::Greater),
            CmpOp::Gt => ordering.map(|ord| ord == std::cmp::Ordering::Greater),
            CmpOp::GtEq => ordering.map(|ord| ord != std::cmp::Ordering::Less),
        };
        Ok(result.map_or(TriState::Unknown, TriState::from_bool))
    }

    fn evaluate_between(
        &self,
        column: &str,
        low: &ScalarValue,
        high: &ScalarValue,
        inclusive: bool,
        row: &DynRow,
    ) -> Result<TriState, ResidualError> {
        let op_low = if inclusive { CmpOp::GtEq } else { CmpOp::Gt };
        let op_high = if inclusive { CmpOp::LtEq } else { CmpOp::Lt };
        let lower = self.evaluate_cmp(column, op_low, low, row)?;
        let upper = self.evaluate_cmp(column, op_high, high, row)?;
        Ok(lower.and(upper))
    }

    fn evaluate_in_list(
        &self,
        column: &str,
        values: &[ScalarValue],
        row: &DynRow,
    ) -> Result<TriState, ResidualError> {
        let Some(lhs) = self.resolve_column(column, row)? else {
            return Ok(TriState::Unknown);
        };
        if lhs.is_null() {
            return Ok(TriState::Unknown);
        }
        let mut saw_null = false;
        for value in values {
            if value.is_null() {
                saw_null = true;
                continue;
            }
            if let Some(ordering) = compare_scalar_values(&lhs, value) {
                if ordering == std::cmp::Ordering::Equal {
                    return Ok(TriState::True);
                }
            } else {
                saw_null = true;
            }
        }
        if saw_null {
            Ok(TriState::Unknown)
        } else {
            Ok(TriState::False)
        }
    }

    fn evaluate_starts_with(
        &self,
        column: &str,
        prefix: &str,
        row: &DynRow,
    ) -> Result<TriState, ResidualError> {
        let Some(value) = self.resolve_column(column, row)? else {
            return Ok(TriState::Unknown);
        };
        if value.is_null() {
            return Ok(TriState::Unknown);
        }
        match value {
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)) => {
                Ok(TriState::from_bool(value.starts_with(prefix)))
            }
            _ => Err(ResidualError::UnsupportedColumn),
        }
    }

    fn evaluate_is_null(
        &self,
        column: &str,
        negated: bool,
        row: &DynRow,
    ) -> Result<TriState, ResidualError> {
        let value = self.resolve_column(column, row)?;
        let is_null = value.as_ref().is_none_or(ScalarValue::is_null);
        Ok(TriState::from_bool(if negated {
            !is_null
        } else {
            is_null
        }))
    }

    fn resolve_column(
        &self,
        column: &str,
        row: &DynRow,
    ) -> Result<Option<ScalarValue>, ResidualError> {
        let idx = self
            .column_map
            .get(column)
            .copied()
            .ok_or_else(|| ResidualError::MissingColumn(Arc::<str>::from(column)))?;
        let cell = row
            .0
            .get(idx)
            .ok_or_else(|| ResidualError::MissingColumn(Arc::<str>::from(column)))?;
        match cell {
            None => Ok(None),
            Some(c) => convert_owned_cell(c).map(Some),
        }
    }
}

fn compare_scalar_values(lhs: &ScalarValue, rhs: &ScalarValue) -> Option<std::cmp::Ordering> {
    if lhs.data_type() == rhs.data_type() {
        return lhs.partial_cmp(rhs);
    }
    numeric_compare(lhs, rhs)
}

fn numeric_compare(lhs: &ScalarValue, rhs: &ScalarValue) -> Option<std::cmp::Ordering> {
    if is_float_scalar(lhs) || is_float_scalar(rhs) {
        let lhs_val = scalar_to_f64(lhs)?;
        let rhs_val = scalar_to_f64(rhs)?;
        return lhs_val.partial_cmp(&rhs_val);
    }
    let lhs_val = scalar_to_i128(lhs)?;
    let rhs_val = scalar_to_i128(rhs)?;
    Some(lhs_val.cmp(&rhs_val))
}

fn is_float_scalar(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Float16(_) | ScalarValue::Float32(_) | ScalarValue::Float64(_)
    )
}

fn scalar_to_i128(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(i128::from(*v)),
        _ => None,
    }
}

fn scalar_to_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float16(Some(v)) => Some(f32::from(*v) as f64),
        ScalarValue::Float32(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::Int8(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt8(Some(v)) => Some(f64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(f64::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(f64::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        _ => None,
    }
}

fn convert_owned_cell(cell: &typed_arrow_dyn::DynCell) -> Result<ScalarValue, ResidualError> {
    use typed_arrow_dyn::DynCell;
    match cell {
        DynCell::Str(s) => Ok(ScalarValue::Utf8(Some(s.to_string()))),
        DynCell::Bin(b) => Ok(ScalarValue::Binary(Some(b.clone()))),
        DynCell::Bool(v) => Ok(ScalarValue::Boolean(Some(*v))),
        DynCell::I8(v) => Ok(ScalarValue::Int8(Some(*v))),
        DynCell::I16(v) => Ok(ScalarValue::Int16(Some(*v))),
        DynCell::I32(v) => Ok(ScalarValue::Int32(Some(*v))),
        DynCell::I64(v) => Ok(ScalarValue::Int64(Some(*v))),
        DynCell::U8(v) => Ok(ScalarValue::UInt8(Some(*v))),
        DynCell::U16(v) => Ok(ScalarValue::UInt16(Some(*v))),
        DynCell::U32(v) => Ok(ScalarValue::UInt32(Some(*v))),
        DynCell::U64(v) => Ok(ScalarValue::UInt64(Some(*v))),
        DynCell::F32(v) => Ok(ScalarValue::Float32(Some(*v))),
        DynCell::F64(v) => Ok(ScalarValue::Float64(Some(*v))),
        DynCell::Null => Ok(ScalarValue::Null),
        _ => Err(ResidualError::UnsupportedColumn),
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use futures::{StreamExt, executor::block_on};
    use typed_arrow_dyn::{DynCell, DynRow};

    use super::*;
    use crate::{
        extractor::{KeyProjection, projection_for_columns, projection_for_field},
        inmem::mutable::memtable::DynMem,
        mvcc::Timestamp,
        query::{
            Expr, ScalarValue,
            stream::{Order, OwnedMutableScan, ScanStream, merge::MergeStream},
        },
        test::build_batch,
    };

    fn make_test_mem(schema: SchemaRef) -> DynMem {
        let extractor: Arc<dyn KeyProjection> = projection_for_field(schema.clone(), 0)
            .expect("extractor")
            .into();
        let delete_projection: Arc<dyn KeyProjection> = projection_for_columns(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![0],
        )
        .expect("delete projection")
        .into();
        DynMem::new(schema, extractor, delete_projection)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn package_stream_emits_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let mutable = make_test_mem(schema.clone());

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
            .insert_batch(batch, Timestamp::new(1))
            .expect("insert batch");

        let mutable_guard = mutable.read();
        let mutable_scan =
            OwnedMutableScan::from_guard(mutable_guard, None, Timestamp::MAX).expect("scan rows");
        let merge = MergeStream::from_vec(
            vec![ScanStream::<'_, fusio::executor::NoopExecutor>::from(
                mutable_scan,
            )],
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
        let mutable = make_test_mem(schema.clone());

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
            .insert_batch(batch, Timestamp::new(1))
            .expect("insert batch");

        let predicate = Expr::gt("v", ScalarValue::from(0i64));

        let mutable_guard = mutable.read();
        let mutable_scan =
            OwnedMutableScan::from_guard(mutable_guard, None, Timestamp::MAX).expect("scan rows");
        let merge = block_on(MergeStream::from_vec(
            vec![ScanStream::<'_, fusio::executor::NoopExecutor>::from(
                mutable_scan,
            )],
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
        let mutable = make_test_mem(schema.clone());

        let rows = vec![DynRow(vec![
            Some(DynCell::Str("only".into())),
            Some(DynCell::I64(1)),
        ])];
        let batch = build_batch(schema.clone(), rows).expect("batch");
        mutable
            .insert_batch(batch, Timestamp::new(1))
            .expect("insert batch");

        // Predicate references a missing column.
        let predicate = Expr::gt("missing", ScalarValue::from(0i64));

        let mutable_guard = mutable.read();
        let mutable_scan =
            OwnedMutableScan::from_guard(mutable_guard, None, Timestamp::MAX).expect("scan rows");
        let merge = block_on(MergeStream::from_vec(
            vec![ScanStream::<'_, fusio::executor::NoopExecutor>::from(
                mutable_scan,
            )],
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
