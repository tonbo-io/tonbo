#![cfg(feature = "http-server")]

use std::sync::Arc;

use fusio::executor::BlockingExecutor;
use http::StatusCode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonbo::{
    admin::{
        http_loop::serve_single_thread,
        server::{CompactionHttpState, TriggerPayload, handle_trigger},
    },
    compaction::{
        executor::LocalCompactionExecutor,
        planner::{CompactionStrategy, LeveledPlannerConfig},
    },
    db::DB,
    mode::DynMode,
    ondisk::sstable::SsTableConfig,
    schema::SchemaBuilder,
};

async fn build_state() -> CompactionHttpState<
    DynMode,
    BlockingExecutor,
    LocalCompactionExecutor,
    tonbo::compaction::planner::CompactionPlannerKind,
> {
    let schema = SchemaBuilder::from_schema(Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new("v", arrow_schema::DataType::Int32, false),
    ])))
    .primary_key("id")
    .build()
    .expect("schema");

    let planner = CompactionStrategy::Leveled(LeveledPlannerConfig::default())
        .build()
        .expect("planner");
    let fs = Arc::new(fusio::mem::fs::InMemoryFs::new());
    let sst_cfg = Arc::new(SsTableConfig::new(
        Arc::clone(&schema.schema),
        fs,
        fusio::path::Path::from("http-loop"),
    ));
    let executor = LocalCompactionExecutor::new(Arc::clone(&sst_cfg), 1);
    let db: DB<DynMode, BlockingExecutor> = DB::<DynMode, BlockingExecutor>::builder(schema)
        .in_memory("http-loop")
        .build_with_executor(Arc::new(BlockingExecutor))
        .await
        .expect("db");
    CompactionHttpState {
        db: Arc::new(db),
        planner,
        executor,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn http_handler_accepts_trigger_payload() {
    let state = build_state().await;
    let payload = TriggerPayload {
        owner: Some("test".into()),
        idempotency_key: Some("k".into()),
        lease_ms: Some(1000),
    };
    let status = handle_trigger(&state, payload).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::ACCEPTED,
        "unexpected status: {status}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn http_loop_responds_without_hanging() {
    let state = build_state().await;
    // Fixed port for test environment; acceptable for single-thread current_thread runtime.
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 18089));
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let server = tokio::task::spawn_local(serve_single_thread(addr, state));
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
            let body = r#"{"owner":"test","idempotency_key":"k","lease_ms":1000}"#;
            let request = format!(
                "POST /compaction/trigger HTTP/1.1\r\nHost: localhost\r\nContent-Length: \
                 {}\r\nContent-Type: application/json\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(request.as_bytes())
                .await
                .expect("write request");

            let mut resp = Vec::new();
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                stream.read_to_end(&mut resp),
            )
            .await
            .expect("response timeout");
            let resp_text = String::from_utf8_lossy(&resp);
            assert!(
                resp_text.starts_with("HTTP/1.1 200") || resp_text.starts_with("HTTP/1.1 202"),
                "unexpected response: {}",
                resp_text
            );
            server.abort();
        })
        .await;
}
