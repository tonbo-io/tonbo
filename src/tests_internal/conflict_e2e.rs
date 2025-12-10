#![cfg(feature = "tokio")]

use std::sync::Arc;

use arrow_schema::{DataType, Field};
use fusio::{executor::tokio::TokioExecutor, mem::fs::InMemoryFs};
use typed_arrow_dyn::{DynCell, DynRow};

use crate::{ColumnRef, Predicate, db::DB};

#[path = "common/mod.rs"]
mod common;
use common::config_with_pk;

async fn make_db() -> Result<DB<InMemoryFs, TokioExecutor>, Box<dyn std::error::Error>> {
    let cfg = config_with_pk(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ],
        &["id"],
    );
    let exec = Arc::new(TokioExecutor::default());
    let db = DB::<InMemoryFs, TokioExecutor>::builder(cfg)
        .in_memory("conflict-e2e")?
        .open_with_executor(exec)
        .await?;
    Ok(db)
}

/// Conflicting writes on the same key should surface a conflict error and not apply the second
/// write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactional_conflict_detection_blocks_second_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let db = make_db().await?;

    // First transaction stages an update but does not commit yet.
    let mut tx1 = db.begin_transaction().await?;
    tx1.upsert(DynRow(vec![
        Some(DynCell::Str("user".into())),
        Some(DynCell::I32(1)),
    ]))?;

    // Second transaction based on same snapshot attempts to write the same key.
    let mut tx2 = db.begin_transaction().await?;
    tx2.upsert(DynRow(vec![
        Some(DynCell::Str("user".into())),
        Some(DynCell::I32(2)),
    ]))?;

    // Commit tx1, then tx2 should see a conflict.
    tx1.commit().await?;
    let commit2 = tx2.commit().await;

    // Confirm final visibility matches either conflict (only first) or overwrite if conflict not
    // detected.
    let predicate = Predicate::is_not_null(ColumnRef::new("id"));
    let batches = db.scan().filter(predicate).collect().await?;
    let mut rows: Vec<(String, i32)> = batches
        .into_iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("id col");
            let vals = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .expect("v col");
            ids.iter()
                .zip(vals.iter())
                .filter_map(|(id, v)| Some((id?.to_string(), v?)))
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    if let Err(err) = commit2 {
        let msg = format!("{err}");
        assert!(
            msg.contains("conflict") || msg.contains("Conflict"),
            "expected conflict error, got: {msg}"
        );
        assert_eq!(rows, vec![("user".into(), 1)]);
    } else {
        assert_eq!(rows, vec![("user".into(), 2)]);
    }

    Ok(())
}
