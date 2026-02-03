//! Observability example: tracing spans and structured logging
//!
//! Tonbo uses the `tracing` crate for observability. This example shows how to
//! configure different subscribers for development, production, and distributed
//! tracing with OpenTelemetry.
//!
//! Run: cargo run --example 11_observability
//!
//! With debug output:
//!   RUST_LOG=tonbo=debug cargo run --example 11_observability

use tonbo::prelude::*;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Record)]
struct Event {
    #[metadata(k = "tonbo.key", v = "true")]
    id: String,
    kind: String,
    payload: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // =========================================================================
    // Option 1: Development - human-readable output with colors
    // =========================================================================
    // Uncomment to use:
    //
    // tracing_subscriber::fmt()
    //     .with_env_filter("info,tonbo=debug")
    //     .init();

    // =========================================================================
    // Option 2: Production - JSON output for log aggregation
    // =========================================================================
    // Uncomment to use:
    //
    // tracing_subscriber::fmt()
    //     .json()
    //     .with_env_filter("info,tonbo=info")
    //     .init();

    // =========================================================================
    // Option 3: Production with file output (non-blocking)
    // =========================================================================
    // Uncomment to use (requires tracing-appender):
    //
    // let file_appender = tracing_appender::rolling::daily("logs", "app.log");
    // let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    // tracing_subscriber::fmt()
    //     .json()
    //     .with_writer(non_blocking)
    //     .with_env_filter("info,tonbo=debug")
    //     .init();

    // =========================================================================
    // Option 4: OpenTelemetry for distributed tracing
    // =========================================================================
    // Uncomment to use (requires opentelemetry crates):
    //
    // use opentelemetry::trace::TracerProvider;
    // use opentelemetry_otlp::WithExportConfig;
    // use opentelemetry_sdk::runtime::Tokio;
    //
    // let exporter = opentelemetry_otlp::SpanExporter::builder()
    //     .with_tonic()
    //     .with_endpoint("http://localhost:4317")
    //     .build()?;
    //
    // let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
    //     .with_batch_exporter(exporter, Tokio)
    //     .build();
    //
    // let tracer = provider.tracer("tonbo-app");
    //
    // tracing_subscriber::registry()
    //     .with(tracing_opentelemetry::layer().with_tracer(tracer))
    //     .with(fmt::layer().with_filter(EnvFilter::from_default_env()))
    //     .init();

    // =========================================================================
    // For this example: simple fmt subscriber with env filter
    // =========================================================================
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,tonbo=debug")),
        )
        .init();

    println!("=== Tonbo Observability Example ===\n");
    println!("Tonbo emits tracing spans for async operations like WAL replay,");
    println!("compaction, and scans. Configure any tracing subscriber to capture them.\n");

    // Create database - this will emit spans for WAL operations
    let db = DbBuilder::from_schema(Event::schema())?
        .on_disk("/tmp/tonbo_observability")?
        .open()
        .await?;

    // Insert some data
    let events = vec![
        Event {
            id: "evt1".into(),
            kind: "click".into(),
            payload: Some("button_ok".into()),
        },
        Event {
            id: "evt2".into(),
            kind: "view".into(),
            payload: None,
        },
    ];

    let mut builders = Event::new_builders(events.len());
    builders.append_rows(events);
    db.ingest(builders.finish().into_record_batch()).await?;

    // Query - this will emit spans for scan operations
    let batches = db.scan().collect().await?;

    println!(
        "Inserted and queried {} events",
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    println!("\nCheck your terminal output above for tracing spans (with RUST_LOG=tonbo=debug)");

    Ok(())
}
