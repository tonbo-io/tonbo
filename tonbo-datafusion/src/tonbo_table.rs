use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use aisle::{Expr as AisleExpr, compile_pruning_ir};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr as DfExpr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, Partitioning, PlanProperties, stream::RecordBatchStreamAdapter,
};
use fusio::disk::TokioFs;
use fusio::executor::tokio::TokioExecutor;
use futures::TryStreamExt;
use tokio_util::task::LocalPoolHandle;
use tonbo::db::DB;

pub struct TonboTable {
    db: Arc<DB<TokioFs, TokioExecutor>>,
    schema: SchemaRef,
}
impl TonboTable {
    pub fn from(db: Arc<DB<TokioFs, TokioExecutor>>, schema: SchemaRef) -> Self {
        Self { db, schema }
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
            projection.map(|p| p.clone()),
            filters.to_vec(),
            limit,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DfExpr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
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
    local_pool: LocalPoolHandle, // think about creating shared pool
}

impl TonboExec {
    pub fn new(
        db: Arc<DB<TokioFs, TokioExecutor>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<DfExpr>,
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        // Excluding the columns that we don't want to read
        let projected_schema: SchemaRef = match &projection {
            Some(indices) => Arc::new(schema.project(indices)?),
            None => schema.clone(),
        };
        let output_schema = projected_schema.clone();
        let instance = Self {
            db,
            schema: schema.clone(),
            projection,
            projected_schema,
            filters,
            limit,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(output_schema.into()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
            local_pool: LocalPoolHandle::new(1),
        };
        Ok(instance)
    }
    pub fn df_to_aisle(&self, filters: &Vec<DfExpr>) -> AisleExpr {
        let filter = if filters.is_empty() {
            AisleExpr::True
        } else {
            let predicate = filters.iter().cloned().reduce(DfExpr::and).unwrap();
            let result = compile_pruning_ir(&predicate, self.schema.as_ref());
            AisleExpr::and(result.ir_exprs().to_vec())
        };

        filter
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
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let filter = self.df_to_aisle(&self.filters);
        let db = self.db.clone();
        //let schema = Arc::clone(&self.schema);
        let local_pool = self.local_pool.clone();
        let projected_schema = Arc::clone(&self.projected_schema);
        let output_schema = Arc::clone(&self.projected_schema);
        let has_projection = self.projection.is_some();

        let future = async move {
            local_pool
                .spawn_pinned(move || async move {
                    let scan = db.scan().filter(filter);
                    let scan = if has_projection {
                        scan.projection(projected_schema)
                    } else {
                        scan
                    };
                    scan.collect().await
                })
                .await
                .map_err(|e| DataFusionError::Execution(format!("scan task panicked: {e}")))?
                .map_err(|e| DataFusionError::Execution(e.to_string()))
                .map(|batches| {
                    futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>))
                })
        };

        let stream = futures::stream::once(future).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}
