use std::{
    any::Any,
    collections::Bound,
    fmt::{Debug, Formatter},
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    arrow::util::pretty,
    catalog::Session,
    common::internal_err,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result},
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execute_stream, execution_plan::Boundedness, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties,
    },
    prelude::*,
    sql::parser::DFParser,
};
use fusio::path::Path;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::fs;
use tonbo::{
    executor::tokio::TokioExecutor, inmem::immutable::ArrowArrays, DbOption, PrimaryKey, DB,
};
use tonbo_macros::Record;

#[derive(Record, Debug)]
pub struct Music {
    #[record(primary_key)]
    id: u64,
    name: String,
    like: i64,
}

struct MusicProvider {
    db: Arc<DB<Music, TokioExecutor>>,
}

struct MusicExec {
    cache: PlanProperties,
    db: Arc<DB<Music, TokioExecutor>>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    range: (Bound<PrimaryKey>, Bound<PrimaryKey>),
}

struct MusicStream {
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>,
}

impl Debug for MusicProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MusicProvider")
            .field("db", &"Music")
            .finish()
    }
}

#[async_trait]
impl TableProvider for MusicProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Music::arrow_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut exec = MusicExec::new(self.db.clone(), projection);

        // TODO: filters to range detach
        // exec.range =
        exec.projection = projection.cloned();
        if let Some(projection) = exec.projection.as_mut() {
            for index in projection {
                *index = index.checked_sub(2).unwrap_or(0);
            }
        }

        exec.limit = limit;

        Ok(Arc::new(exec))
    }
}

impl MusicExec {
    fn new(db: Arc<DB<Music, TokioExecutor>>, projection: Option<&Vec<usize>>) -> Self {
        let schema = Music::arrow_schema();
        let schema = if let Some(projection) = &projection {
            Arc::new(schema.project(projection).unwrap())
        } else {
            schema.clone()
        };

        MusicExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new_with_orderings(schema, &[]),
                datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
                datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            db,
            projection: None,
            limit: None,
            range: (Bound::Unbounded, Bound::Unbounded),
        }
    }
}

impl Stream for MusicStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        pin!(&mut self.stream).poll_next(cx)
    }
}

impl RecordBatchStream for MusicStream {
    fn schema(&self) -> SchemaRef {
        Music::arrow_schema().clone()
    }
}

impl DisplayAs for MusicExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let (lower, upper) = &self.range;

        write!(
            f,
            "MusicExec: range:({:?}, {:?}), projection: [{:?}], limit: {:?}",
            lower, upper, self.projection, self.limit
        )
    }
}

impl Debug for MusicExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MusicExec")
            .field("cache", &self.cache)
            .field("limit", &self.limit)
            .field("projection", &self.projection)
            .field("range", &self.range)
            .finish()
    }
}

impl ExecutionPlan for MusicExec {
    fn name(&self) -> &str {
        "MusicExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let db = self.db.clone();
        let (lower, upper) = self.range.clone();
        let limit = self.limit;
        let projection = self.projection.clone();

        Ok(Box::pin(MusicStream {
            stream: Box::pin(stream! {
                let txn = db.transaction().await;

                let mut scan = txn
                    .scan((lower.as_ref(), upper.as_ref()));
                if let Some(limit) = limit {
                    scan = scan.limit(limit);
                }
                if let Some(projection) = projection {
                    scan = scan.projection_with_index(projection.clone());
                }
                let mut scan = scan.package(8192).await.map_err(|err| DataFusionError::Internal(err.to_string()))?;

                while let Some(record) = scan.next().await {
                    yield Ok(record?.as_record_batch().clone())
                }
            }),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // make sure the path exists
    let _ = fs::create_dir_all("./db_path/music").await;

    let options = DbOption::new(Path::from_filesystem_path("./db_path/music").unwrap());

    let db = DB::new(options, TokioExecutor::current(), Music::schema())
        .await
        .unwrap();
    for (id, name, like) in [
        (0, "welcome".to_string(), 0),
        (1, "tonbo".to_string(), 999),
        (2, "star".to_string(), 233),
        (3, "plz".to_string(), 2),
    ] {
        db.insert(Music { id, name, like }).await.unwrap();
    }
    let ctx = SessionContext::new();

    let provider = MusicProvider { db: Arc::new(db) };
    ctx.register_table("music", Arc::new(provider))?;

    {
        let df = ctx.table("music").await?;
        let df = df.select(vec![col("name")])?;
        let batches = df.collect().await?;
        pretty::print_batches(&batches).unwrap();
    }

    {
        // support sql query for tonbo
        let statements = DFParser::parse_sql("select id from music")?;
        let plan = ctx
            .state()
            .statement_to_plan(statements.front().cloned().unwrap())
            .await?;
        let df = ctx.execute_logical_plan(plan).await?;
        let physical_plan = df.create_physical_plan().await?;
        let mut stream = execute_stream(physical_plan, ctx.task_ctx())?;
        while let Some(maybe_batch) = stream.next().await {
            let batch = maybe_batch?;
            pretty::print_batches(&[batch]).unwrap();
        }
    }
    // https://github.com/tonbo-io/tonbo/issues/172
    {
        let statements =
            DFParser::parse_sql("select _null, _ts, id, name from music where id = 1")?;
        let plan = ctx
            .state()
            .statement_to_plan(statements.front().cloned().unwrap())
            .await?;
        let df = ctx.execute_logical_plan(plan).await?;
        let physical_plan = df.create_physical_plan().await?;
        let mut stream = execute_stream(physical_plan, ctx.task_ctx())?;
        while let Some(maybe_batch) = stream.next().await {
            let batch = maybe_batch?;
            pretty::print_batches(&[batch]).unwrap();
        }
    }

    Ok(())
}
