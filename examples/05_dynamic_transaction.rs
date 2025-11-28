// 05: Transactional writes (strict WAL) with optimistic staging and commit

use fusio::executor::tokio::TokioExecutor;
use futures::StreamExt;
use tempfile::tempdir;
use tonbo::{
    db::{DB, DbBuilder, DynMode},
    mvcc::Timestamp,
    query::{ColumnRef, Predicate, ScalarValue},
    transaction::CommitAckMode,
};
use typed_arrow::{Record, prelude::*, schema::SchemaMeta};
use typed_arrow_dyn::DynCell;

#[derive(Record)]
struct UserRow {
    id: String,
    v: Option<i32>,
}

#[tokio::main]
async fn main() {
    // Compile-time schema via typed-arrow derive; v is nullable, id is not.
    let schema = <UserRow as SchemaMeta>::schema();

    // Configure dynamic mode with strict (durable) commit acknowledgements.
    // Use a temporary on-disk layout to enable WAL-backed transactions.
    let temp = tempdir().expect("tempdir");
    let root = temp.path().to_str().expect("utf8 path").to_string();
    let mut db = DbBuilder::from_schema_key_name(schema.clone(), "id")
        .expect("config")
        .with_commit_ack_mode(CommitAckMode::Strict)
        .on_disk(root)
        .expect("on_disk")
        .create_dirs(true)
        .build()
        .await
        .expect("build db");

    // Build a RecordBatch using typed-arrow row builders.
    let rows = vec![
        UserRow {
            id: "user-1".into(),
            v: Some(10),
        },
        UserRow {
            id: "user-2".into(),
            v: None, // demonstrate nullable value
        },
    ];
    let mut builders = <UserRow as BuildRows>::new_builders(rows.len());
    builders.append_rows(rows);
    let batch = builders.finish().into_record_batch();

    // Begin a transaction and stage mutations.
    let mut tx = db.begin_transaction().await.expect("begin tx");
    tx.upsert_batch(&batch).expect("stage batch");
    tx.delete("ghost").expect("stage delete");

    // Read-your-writes inside the transaction.
    let pred = Predicate::eq(ColumnRef::new("id", None), ScalarValue::from("user-1"));
    let preview = tx.scan(&db, &pred, None, None).await.expect("preview");
    println!(
        "preview rows: {:?}",
        preview
            .iter()
            .map(|row| {
                let key = match row.0[0].clone() {
                    Some(DynCell::Str(s)) => s.to_owned(),
                    _ => "<null>".to_string(),
                };
                let val = match row.0[1].clone() {
                    Some(DynCell::I32(v)) => v,
                    _ => 0,
                };
                format!("id={key}, v={val}")
            })
            .collect::<Vec<_>>()
    );

    // Commit with strict WAL durability.
    tx.commit(&mut db).await.expect("commit");

    // Post-commit read via the public scan path.
    let all_pred = Predicate::is_not_null(ColumnRef::new("id", None));
    let committed = scan_pairs(&db, &all_pred).await;
    println!("committed rows: {:?}", committed);
}

async fn scan_pairs(db: &DB<DynMode, TokioExecutor>, predicate: &Predicate) -> Vec<(String, i32)> {
    let plan = db
        .plan_scan(predicate, None, None, Timestamp::MAX)
        .await
        .expect("plan");
    let mut stream = db.execute_scan(plan).await.expect("exec");
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<typed_arrow::arrow_array::StringArray>()
            .expect("id col");
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<typed_arrow::arrow_array::Int32Array>()
            .expect("v col");
        for (id, v) in ids.iter().zip(vals.iter()) {
            if let Some(id) = id {
                out.push((id.to_string(), v.unwrap_or_default()));
            }
        }
    }
    out
}
