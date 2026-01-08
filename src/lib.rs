#![deny(missing_docs)]
#![deny(clippy::unwrap_used)]
#![cfg_attr(not(feature = "test"), deny(dead_code))]
//! Tonbo is an embedded database for serverless data-intensive applications.
//!
//! - **Arrow-native schemas** with rich, typed structures
//! - **Stores data as Parquet** directly on object storage (S3, R2) or local filesystem
//! - **Fully asynchronous** and runs in multiple runtimes: browsers, edge functions, or inside
//!   other databases
//!
//! No server process to manage. Each database is just a manifest on S3, adding more is trivial.
//!
//! # Quick Start
//!
//! Add Tonbo to your project:
//!
//! ```bash
//! cargo add tonbo tokio
//! ```
//!
//! ## Basic Usage
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use arrow_array::{Int64Array, RecordBatch, StringArray};
//! use arrow_schema::{DataType, Field, Schema};
//! use tonbo::db::{ColumnRef, DbBuilder, Predicate, ScalarValue};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Define schema: User { id: String, name: String, score: i64 }
//!     let schema = Arc::new(Schema::new(vec![
//!         Field::new("id", DataType::Utf8, false),
//!         Field::new("name", DataType::Utf8, false),
//!         Field::new("score", DataType::Int64, true),
//!     ]));
//!
//!     // Open database on local disk
//!     let db = DbBuilder::from_schema_key_name(schema.clone(), "id")?
//!         .on_disk("/tmp/tonbo_doctest")?
//!         .open()
//!         .await?;
//!
//!     // Insert data as Arrow RecordBatch
//!     let batch = RecordBatch::try_new(
//!         schema,
//!         vec![
//!             Arc::new(StringArray::from(vec!["u1", "u2"])),
//!             Arc::new(StringArray::from(vec!["Alice", "Bob"])),
//!             Arc::new(Int64Array::from(vec![100, 85])),
//!         ],
//!     )?;
//!     db.ingest(batch).await?;
//!
//!     // Query: score > 80
//!     let filter = Predicate::gt(ColumnRef::new("score"), ScalarValue::from(80_i64));
//!     let results = db.scan().filter(filter).collect().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! For a more ergonomic API, use [`typed_arrow`]'s `#[derive(Record)]` via `tonbo::prelude::*`
//! (`typed-arrow` feature is enabled by default). Mark your primary key field with
//! `#[metadata(k = "tonbo.key", v = "true")]`:
//! Mark your primary key field with `#[metadata(k = "tonbo.key", v = "true")]`:
//!
//! ```rust,ignore
//! use tonbo::prelude::*;
//!
//! #[derive(Record)]
//! struct User {
//!     #[metadata(k = "tonbo.key", v = "true")]
//!     id: String,
//!     name: String,
//!     score: Option<i64>,
//! }
//!
//! // Key is automatically detected from schema metadata
//! let db = DbBuilder::from_schema(User::schema())?
//!     .on_disk("/tmp/users")?
//!     .open()
//!     .await?;
//!
//! let users = vec![
//!     User { id: "u1".into(), name: "Alice".into(), score: Some(100) },
//! ];
//! let mut builders = User::new_builders(users.len());
//! builders.append_rows(users);
//! db.ingest(builders.finish().into_record_batch()).await?;
//! ```
//!
//! ## Using S3 / Object Storage
//!
//! Tonbo stores data as Parquet files on any S3-compatible storage (AWS S3, Cloudflare R2, MinIO):
//!
//! ```rust,ignore
//! use tonbo::db::{AwsCreds, DbBuilder, ObjectSpec, S3Spec};
//!
//! let credentials = AwsCreds::from_env()?;
//! let mut s3_spec = S3Spec::new("my-bucket", "data/users", credentials);
//! s3_spec.region = Some("us-east-1".into());
//!
//! let db = DbBuilder::from_schema_key_name(User::schema(), "id")?
//!     .object_store(ObjectSpec::s3(s3_spec))?
//!     .open()
//!     .await?;
//! ```
//!
//! # Core Concepts
//!
//! ## Schema Definition
//!
//! Use the `#[derive(Record)]` macro from [`typed_arrow`] (available via `tonbo::prelude::*`) to
//! define your schema.
//! Mark primary key fields with `#[metadata(k = "tonbo.key", v = "true")]`:
//!
//! ```rust,ignore
//! use tonbo::prelude::Record;
//!
//! #[derive(Record)]
//! struct Event {
//!     #[metadata(k = "tonbo.key", v = "true")]
//!     id: String,
//!     timestamp: i64,
//!     event_type: String,
//!     payload: Option<String>, // Nullable field
//! }
//! ```
//!
//! For composite keys, use ordinal values:
//!
//! ```rust,ignore
//! #[derive(Record)]
//! struct TimeSeries {
//!     #[metadata(k = "tonbo.key", v = "0")]
//!     device_id: String,
//!     #[metadata(k = "tonbo.key", v = "1")]
//!     timestamp: i64,
//!     value: f64,
//! }
//! ```
//!
//! ## Database Operations
//!
//! - **[`DbBuilder::from_schema`](db::DbBuilder::from_schema)** - Create DB with auto-detected key
//!   from metadata
//! - **[`DbBuilder`](db::DbBuilder)** - Configure and open a database
//! - **[`DB`](db::DB)** - The main database handle for reads and writes
//! - **[`DB::ingest`](db::DB::ingest)** - Batch insert records
//! - **[`DB::scan`](db::DB::scan)** - Query with filters and projections
//! - **[`DB::begin_transaction`](db::DB::begin_transaction)** - MVCC transactions with
//!   read-your-writes
//!
//! ## Predicates
//!
//! Build query filters using [`Predicate`](db::Predicate):
//!
//! ```rust,ignore
//! use tonbo::db::{ColumnRef, Predicate, ScalarValue};
//!
//! // Equality
//! let filter = Predicate::eq(ColumnRef::new("status"), ScalarValue::from("active"));
//!
//! // Comparison
//! let filter = Predicate::gt(ColumnRef::new("age"), ScalarValue::from(18_i64));
//!
//! // Logical operators
//! let filter = Predicate::and(
//!     Predicate::gt(ColumnRef::new("age"), ScalarValue::from(18_i64)),
//!     Predicate::eq(ColumnRef::new("country"), ScalarValue::from("US")),
//! );
//! ```
//!
//! # Feature Flags
//!
//! Tonbo uses feature flags to configure runtime and storage backends:
//!
//! - **`tokio`** *(default)* - Tokio async runtime with local filesystem support
//! - **`typed-arrow`** *(default)* - Provides [`typed_arrow`] derive helpers via `tonbo::prelude`
//! - **`web`** - WebAssembly support for browsers and edge runtimes
//! - **`web-opfs`** - Browser Origin Private File System storage (requires `web`)
//!
//! ## Default Configuration
//!
//! ```toml
//! [dependencies]
//! tonbo = "0.1"
//! ```
//!
//! This includes both `tokio` runtime and `typed-arrow` for schema derivation.
//!
//! ## WebAssembly / Browser
//!
//! ```toml
//! [dependencies]
//! tonbo = { version = "0.1", default-features = false, features = ["web", "typed-arrow"] }
//! ```
//!
//! # Examples
//!
//! Run examples with `cargo run --example <name>`:
//!
//! | Example | Description |
//! |---------|-------------|
//! | `01_basic` | Define schema, insert, and query in 30 lines |
//! | `02_transaction` | MVCC transactions with upsert, delete, read-your-writes |
//! | `02b_snapshot` | Consistent point-in-time reads while writes continue |
//! | `03_filter` | Predicates: eq, gt, in, is_null, and, or, not |
//! | `04_s3` | Store Parquet files on S3/R2/MinIO |
//! | `05_scan_options` | Projection pushdown reads only needed columns |
//! | `06_composite_key` | Multi-column keys for time-series data |
//! | `07_streaming` | Process millions of rows without loading into memory |
//! | `08_nested_types` | Deep struct nesting + Lists as Arrow StructArray |
//! | `09_time_travel` | Query historical snapshots via MVCC timestamps |
//!
//! # Architecture
//!
//! Tonbo implements an LSM-tree style architecture optimized for analytical workloads:
//!
//! 1. **Write Path**: Data is written to an in-memory buffer, then flushed to immutable Parquet
//!    files on storage
//! 2. **WAL**: Write-ahead log ensures durability before acknowledgment
//! 3. **Manifest**: Tracks all Parquet files and database state; uses compare-and-swap for
//!    coordination on object storage
//! 4. **Compaction**: Background process merges small files into larger ones
//! 5. **MVCC**: Multi-version concurrency control enables snapshot isolation
//!
//! # Platform Support
//!
//! | Platform | Runtime | Storage |
//! |----------|---------|---------|
//! | Linux/macOS/Windows | Tokio | Local filesystem, S3 |
//! | WebAssembly | Browser async | S3, OPFS |
//! | Edge (Deno, Workers) | Platform async | S3 |

pub(crate) mod extractor;
/// File and object identifiers.
pub(crate) mod id;
mod inmem;
/// Zero-copy key projection scaffolding and owned key wrapper.
pub(crate) mod key;
/// Metrics snapshots and observability surface.
pub mod metrics;
pub(crate) mod mode;
pub(crate) mod mutation;
/// Convenience re-exports for common usage.
pub mod prelude;
pub mod schema;

#[cfg(feature = "test")]
/// Test helper re-exports for integration and e2e tests.
pub mod test {
    pub use crate::test_support::*;
}
#[cfg(all(test, not(feature = "test")))]
/// Test helper re-exports for unit tests.
pub(crate) mod test {
    pub(crate) use crate::test_support::*;
}
#[cfg(feature = "test")]
pub mod test_support;
#[cfg(all(test, not(feature = "test")))]
pub(crate) mod test_support;
#[cfg(test)]
mod tests_internal;

/// Generic DB that dispatches between typed and dynamic modes via generic types.
pub mod db;

pub(crate) mod query;

/// Write-ahead log framework (async, fusio-backed).
pub(crate) mod wal;

/// Manifest integration atop `fusio-manifest`.
pub(crate) mod manifest;

/// MVCC primitives shared across modules.
pub(crate) mod mvcc;

/// Optimistic transaction scaffolding (write path focus for now).
pub(crate) mod transaction;

/// On-disk persistence scaffolding (SSTable skeletons).
pub(crate) mod ondisk;

/// Simple compaction orchestrators.
pub(crate) mod compaction;
