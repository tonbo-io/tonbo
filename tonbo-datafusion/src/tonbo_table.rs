use std::{any::Any, fmt::Debug, sync::Arc};

use aisle::{Expr as AisleExpr, compile_pruning_ir};
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr as DfExpr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use fusio::{disk::TokioFs, executor::tokio::TokioExecutor};
use futures::{SinkExt, StreamExt, channel::mpsc};
use tokio_util::task::LocalPoolHandle;
use tonbo::db::DB;

pub struct TonboTable {
    db: Arc<DB<TokioFs, TokioExecutor>>,
    schema: SchemaRef,
    local_pool: LocalPoolHandle,
}

impl TonboTable {
    pub fn from(db: Arc<DB<TokioFs, TokioExecutor>>, schema: SchemaRef) -> Self {
        Self {
            db,
            schema,
            local_pool: LocalPoolHandle::new(1),
        }
    }
}
impl Debug for TonboTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TonboTableProvider")
            .field("db", &"db")
            .field("schema", &self.schema)
            .finish()
    }
}
#[async_trait]
impl TableProvider for TonboTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[DfExpr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TonboExec::new(
            self.db.clone(),
            self.schema.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
            self.local_pool.clone(),
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Tonbo uses filters for pruning, while DataFusion retains the exact
        // filter as a residual to preserve correctness.
        Ok(inexact_filter_pushdowns(filters))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct TonboExec {
    db: Arc<DB<TokioFs, TokioExecutor>>,
    schema: Arc<Schema>,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    filters: Vec<DfExpr>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
    local_pool: LocalPoolHandle,
}

impl TonboExec {
    pub fn new(
        db: Arc<DB<TokioFs, TokioExecutor>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<DfExpr>,
        limit: Option<usize>,
        local_pool: LocalPoolHandle,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(&schema, projection.as_deref())?;
        let output_schema = projected_schema.clone();
        let instance = Self {
            db,
            schema: schema.clone(),
            projection,
            projected_schema,
            filters,
            limit,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(output_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
            local_pool,
        };
        Ok(instance)
    }
    pub fn aisle_expr(&self, filters: &[DfExpr]) -> AisleExpr {
        df_to_aisle(self.schema.as_ref(), filters)
    }
}

impl Debug for TonboExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TonboExec")
            .field("db", &"DB")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .field("filters", &self.filters)
            .field("limit", &self.limit)
            .field("properties", &self.properties)
            .finish()
    }
}

impl DisplayAs for TonboExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            f,
            "TonboExec: projection={:?}, filters={}, limit={:?}",
            self.projection,
            self.filters.len(),
            self.limit
        )
    }
}

impl ExecutionPlan for TonboExec {
    fn name(&self) -> &str {
        "TonboExecutionPlan"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        validate_leaf_children(children.len())?;
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        validate_partition(partition)?;

        let filter = self.aisle_expr(&self.filters);
        let db = Arc::clone(&self.db);
        let projected_schema = Arc::clone(&self.projected_schema);
        let output_schema = Arc::clone(&self.projected_schema);
        let has_projection = self.projection.is_some();
        let pool = self.local_pool.clone();
        let (mut sender, receiver) = mpsc::channel(2);

        // Tonbo's scan() is a !Send so, we isolate that
        let _scan_task = pool.spawn_pinned(move || async move {
            let scan = db.scan().filter(filter);
            let scan = if has_projection {
                scan.projection(projected_schema)
            } else {
                scan
            };

            match scan.stream().await {
                Ok(stream) => {
                    futures::pin_mut!(stream);
                    while let Some(batch) = stream.next().await {
                        let batch =
                            batch.map_err(|error| DataFusionError::Execution(error.to_string()));
                        if sender.send(batch).await.is_err() {
                            break;
                        }
                    }
                }
                Err(error) => {
                    let _ = sender
                        .send(Err(DataFusionError::Execution(error.to_string())))
                        .await;
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            receiver,
        )))
    }
}

/// Datafusion Expr to Aisle Expr
fn df_to_aisle(schema: &Schema, filters: &[DfExpr]) -> AisleExpr {
    let Some((first, rest)) = filters.split_first() else {
        return AisleExpr::True;
    };
    let predicate = rest.iter().cloned().fold(first.clone(), DfExpr::and);
    let result = compile_pruning_ir(&predicate, schema);
    AisleExpr::and(result.ir_exprs().to_vec())
}

fn project_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> DataFusionResult<SchemaRef> {
    match projection {
        Some(indices) => Ok(Arc::new(schema.project(indices)?)),
        None => Ok(Arc::clone(schema)),
    }
}

fn inexact_filter_pushdowns(filters: &[&DfExpr]) -> Vec<TableProviderFilterPushDown> {
    filters
        .iter()
        .map(|_| TableProviderFilterPushDown::Inexact)
        .collect()
}

fn validate_partition(partition: usize) -> DataFusionResult<()> {
    if partition == 0 {
        Ok(())
    } else {
        Err(DataFusionError::Execution(format!(
            "invalid Tonbo partition: {partition}"
        )))
    }
}

fn validate_leaf_children(child_count: usize) -> DataFusionResult<()> {
    if child_count == 0 {
        Ok(())
    } else {
        Err(DataFusionError::Plan(
            "TonboExec does not accept child execution plans".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field},
        logical_expr::{col, lit},
    };

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn project_schema_selects_requested_columns_in_order() {
        let schema = test_schema();

        let projected = project_schema(&schema, Some(&[2, 0])).expect("valid projection");

        assert_eq!(projected.fields().len(), 2);
        assert_eq!(projected.field(0).name(), "name");
        assert_eq!(projected.field(1).name(), "id");
    }

    #[test]
    fn project_schema_rejects_unknown_column_index() {
        let schema = test_schema();

        assert!(project_schema(&schema, Some(&[3])).is_err());
    }

    #[test]
    fn empty_filters_compile_to_true_predicate() {
        assert!(matches!(
            df_to_aisle(test_schema().as_ref(), &[]),
            AisleExpr::True
        ));
    }

    #[test]
    fn supported_filter_compiles_to_non_trivial_pruning_predicate() {
        let schema = test_schema();
        let filter = col("score").gt(lit(80_i64));

        assert!(!matches!(
            df_to_aisle(schema.as_ref(), &[filter]),
            AisleExpr::True
        ));
    }

    #[test]
    fn filters_are_reported_as_inexact_pushdowns() {
        let filters = vec![col("score").gt(lit(80_i64)), col("name").is_not_null()];
        let filter_refs = filters.iter().collect::<Vec<_>>();

        let pushdowns = inexact_filter_pushdowns(&filter_refs);

        assert_eq!(pushdowns.len(), filters.len());
        assert!(
            pushdowns
                .iter()
                .all(|pushdown| matches!(pushdown, TableProviderFilterPushDown::Inexact))
        );
    }

    #[test]
    fn execution_plan_rejects_invalid_partition_and_children() {
        assert!(validate_partition(0).is_ok());
        assert!(matches!(
            validate_partition(1),
            Err(DataFusionError::Execution(_))
        ));
        assert!(validate_leaf_children(0).is_ok());
        assert!(matches!(
            validate_leaf_children(1),
            Err(DataFusionError::Plan(_))
        ));
    }
}
