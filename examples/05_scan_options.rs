//! Scan options: projection, limit, and combined queries
//!
//! Run: cargo run --example 05_scan_options

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use tonbo::{
    ColumnRef, Predicate, ScalarValue,
    db::DbBuilder,
    typed_arrow::{Record, prelude::*, schema::SchemaMeta},
};

#[derive(Record)]
struct Order {
    #[metadata(k = "tonbo.key", v = "true")]
    id: String,
    customer: String,
    product: String,
    quantity: i64,
    price: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DbBuilder::from_schema(Order::schema())?
        .on_disk("/tmp/tonbo_scan_options")?
        .open()
        .await?;

    // Insert sample data
    let orders = vec![
        Order {
            id: "o1".into(),
            customer: "Alice".into(),
            product: "Laptop".into(),
            quantity: 1,
            price: 999,
        },
        Order {
            id: "o2".into(),
            customer: "Bob".into(),
            product: "Mouse".into(),
            quantity: 2,
            price: 29,
        },
        Order {
            id: "o3".into(),
            customer: "Alice".into(),
            product: "Keyboard".into(),
            quantity: 1,
            price: 79,
        },
        Order {
            id: "o4".into(),
            customer: "Carol".into(),
            product: "Monitor".into(),
            quantity: 1,
            price: 299,
        },
        Order {
            id: "o5".into(),
            customer: "Bob".into(),
            product: "Headphones".into(),
            quantity: 1,
            price: 149,
        },
        Order {
            id: "o6".into(),
            customer: "Alice".into(),
            product: "Webcam".into(),
            quantity: 1,
            price: 89,
        },
    ];
    let mut builders = Order::new_builders(orders.len());
    builders.append_rows(orders);
    db.ingest(builders.finish().into_record_batch()).await?;

    // 1. Limit: get first 3 orders
    println!("1. LIMIT 3:");
    let batches = db.scan().limit(3).collect().await?;
    for batch in &batches {
        for o in batch.iter_views::<Order>()?.try_flatten()? {
            println!("  {} - {} bought {}", o.id, o.customer, o.product);
        }
    }

    // 2. Projection: select only id and customer columns
    println!("\n2. SELECT id, customer:");
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("customer", DataType::Utf8, false),
    ]));
    let batches = db.scan().projection(projected_schema).collect().await?;
    // Note: with projection, we read raw columns instead of typed views
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let customers = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            println!("  {} - {}", ids.value(i), customers.value(i));
        }
    }

    // 3. Filter + Limit: high price orders, max 2
    println!("\n3. WHERE price > 100 LIMIT 2:");
    let filter = Predicate::gt(ColumnRef::new("price"), ScalarValue::from(100_i64));
    let batches = db.scan().filter(filter).limit(2).collect().await?;
    for batch in &batches {
        for o in batch.iter_views::<Order>()?.try_flatten()? {
            println!("  {} - {} (${}) ", o.id, o.product, o.price);
        }
    }

    // 4. Filter + Projection: high-value orders, show only id and price
    println!("\n4. SELECT id, price WHERE price > 100:");
    let filter = Predicate::gt(ColumnRef::new("price"), ScalarValue::from(100_i64));
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let batches = db
        .scan()
        .filter(filter)
        .projection(projected_schema)
        .collect()
        .await?;
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let prices = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            println!("  {} - ${}", ids.value(i), prices.value(i));
        }
    }

    // 5. All combined: Filter + Projection + Limit
    println!("\n5. SELECT id, product WHERE quantity = 1 LIMIT 3:");
    let filter = Predicate::eq(ColumnRef::new("quantity"), ScalarValue::from(1_i64));
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("product", DataType::Utf8, false),
    ]));
    let batches = db
        .scan()
        .filter(filter)
        .projection(projected_schema)
        .limit(3)
        .collect()
        .await?;
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let products = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            println!("  {} - {}", ids.value(i), products.value(i));
        }
    }

    Ok(())
}
