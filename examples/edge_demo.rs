//! Shared helpers for wasm edge/worker demos.

#[cfg(all(target_arch = "wasm32", feature = "web"))]
mod wasm_edge {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use fusio::{executor::web::WebExecutor, impls::remotes::aws::fs::AmazonS3};
    use futures::StreamExt;
    use js_sys::Date;
    use predicate::ScalarValue;
    use tonbo::{
        db::{AwsCreds, DB, DynMode, ObjectSpec, S3Spec},
        query::Predicate,
        schema::SchemaBuilder,
        wal::WalSyncPolicy,
    };

    fn s3_spec(prefix: String) -> Result<S3Spec, String> {
        let bucket = option_env!("FUSIO_EDGE_S3_BUCKET").unwrap_or("fusio-test");
        let endpoint = option_env!("FUSIO_EDGE_S3_ENDPOINT")
            .or(option_env!("AWS_ENDPOINT_URL"))
            .unwrap_or("http://localhost:9000");
        let region = option_env!("FUSIO_EDGE_S3_REGION").unwrap_or("us-east-1");
        let access_key = option_env!("FUSIO_EDGE_S3_ACCESS_KEY").unwrap_or("test");
        let secret_key = option_env!("FUSIO_EDGE_S3_SECRET_KEY").unwrap_or("test");
        let mut spec = S3Spec::new(bucket, prefix, AwsCreds::new(access_key, secret_key));
        spec.endpoint = Some(endpoint.to_string());
        spec.region = Some(region.to_string());
        Ok(spec)
    }

    /// Ingest a small Arrow batch via the wasm `WebExecutor` and stream the rows back.
    pub async fn edge_roundtrip_rows() -> Result<Vec<(String, i32)>, String> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let schema_cfg = SchemaBuilder::from_schema(Arc::clone(&schema))
            .primary_key("id")
            .build()
            .map_err(|err| format!("schema: {err}"))?;

        let now_ms = Date::now() as u128;
        let prefix = format!("edge-demo-{now_ms}");
        let s3_spec = s3_spec(prefix.clone())?;

        let exec = Arc::new(WebExecutor::new());
        let db: DB<DynMode, AmazonS3, WebExecutor> =
            DB::<DynMode, AmazonS3, WebExecutor>::builder(schema_cfg)
                .object_store(ObjectSpec::s3(s3_spec))
                .wal_sync_policy(WalSyncPolicy::Always)
                .build_with_executor(Arc::clone(&exec))
                .await
                .map_err(|err| format!("build: {err}"))?;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["alpha", "beta"])) as _,
                Arc::new(Int32Array::from(vec![1, 2])) as _,
            ],
        )
        .map_err(|err| format!("batch: {err}"))?;

        db.ingest(batch)
            .await
            .map_err(|err| format!("ingest: {err}"))?;

        // Plan a full-range scan and stream Arrow batches back through the DB API.
        let snapshot = db
            .begin_snapshot()
            .await
            .map_err(|err| format!("snapshot: {err}"))?;
        let plan = snapshot
            .plan_scan(
                &db,
                &Predicate::eq(ScalarValue::from(1_i64), ScalarValue::from(1_i64)),
                None,
                None,
            )
            .await
            .map_err(|err| format!("plan: {err}"))?;
        let mut stream = db
            .execute_scan(plan)
            .await
            .map_err(|err| format!("scan: {err}"))?;

        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|err| format!("batch: {err}"))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("id column not utf8")?;
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("value column not int32")?;
            for idx in 0..ids.len() {
                rows.push((ids.value(idx).to_string(), values.value(idx)));
            }
        }

        rows.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(rows)
    }
}

#[cfg(all(target_arch = "wasm32", feature = "web"))]
pub use wasm_edge::edge_roundtrip_rows;

// Fallback so native example builds don't fail when this file is compiled outside wasm+web.
#[cfg(all(target_arch = "wasm32", feature = "web"))]
fn main() {}

#[cfg(not(all(target_arch = "wasm32", feature = "web")))]
fn main() {
    eprintln!("edge_demo is only available on wasm32 with the `web` feature");
}
