// 05: Transactional writes (strict WAL) with optimistic staging and commit

use fusio::{disk::LocalFs, executor::tokio::TokioExecutor};
use futures::StreamExt;
use tonbo::prelude::*;
use typed_arrow::{
    Record,
    arrow_array::{Int32Array, StringArray},
    schema::SchemaMeta,
};

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
    let db = DbBuilder::from_schema_key_name(schema.clone(), "id")
        .expect("config")
        .with_commit_ack_mode(CommitAckMode::Strict)
        .on_disk("/tmp/tonbo")
        .expect("on_disk")
        .open()
        .await
        .expect("open db");

    // // Build a RecordBatch using typed-arrow row builders.
    // let rows = vec![
    //     UserRow {
    //         id: "user-1".into(),
    //         v: Some(10),
    //     },
    //     UserRow {
    //         id: "user-2".into(),
    //         v: None, // demonstrate nullable value
    //     },
    // ];
    // let mut builders = <UserRow as BuildRows>::new_builders(rows.len());
    // builders.append_rows(rows);
    // let batch = builders.finish().into_record_batch();

    // // Begin a transaction and stage mutations.
    let tx = db.begin_transaction().await.expect("begin tx");
    // tx.upsert_batch(&batch).expect("stage batch");
    // tx.delete("ghost").expect("stage delete");

    // Read-your-writes inside the transaction.
    let pred = Expr::eq("id", ScalarValue::from("user-1"));
    let preview_batches = tx.scan().filter(pred).collect().await.expect("preview");
    let mut preview_rows = Vec::new();
    for batch in &preview_batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id col");
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("v col");
        for (id, v) in ids.iter().zip(vals.iter()) {
            let key = id.unwrap_or("<null>").to_string();
            let val = v.unwrap_or(0);
            preview_rows.push(format!("id={key}, v={val}"));
        }
    }
    println!("preview rows: {:?}", preview_rows);

    // Commit with strict WAL durability.
    tx.commit().await.expect("commit");

    // Post-commit read via the public scan path.
    let all_pred = Expr::is_not_null("id");
    let committed = scan_pairs(&db, &all_pred).await;
    println!("committed rows: {:?}", committed);
}

async fn scan_pairs(db: &DB<LocalFs, TokioExecutor>, predicate: &Expr) -> Vec<(String, i32)> {
    let mut stream = db
        .scan()
        .filter(predicate.clone())
        .stream()
        .await
        .expect("scan");
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch");
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id col");
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("v col");
        for (id, v) in ids.iter().zip(vals.iter()) {
            if let Some(id) = id {
                out.push((id.to_string(), v.unwrap_or_default()));
            }
        }
    }
    out
}
