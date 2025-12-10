//! Query filtering: predicates for eq, gt, lt, and, or, in, is_null
//!
//! Run: cargo run --example 03_filter

use fusio::{disk::LocalFs, executor::tokio::TokioExecutor};
use tonbo::prelude::*;

#[derive(Record)]
struct Product {
    #[metadata(k = "tonbo.key", v = "true")]
    id: String,
    name: String,
    price: i64,
    category: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DbBuilder::from_schema(Product::schema())?
        .on_disk("/tmp/tonbo_filter_example")?
        .open()
        .await?;

    // Insert sample data
    let products = vec![
        Product {
            id: "p1".into(),
            name: "Laptop".into(),
            price: 999,
            category: Some("Electronics".into()),
        },
        Product {
            id: "p2".into(),
            name: "Mouse".into(),
            price: 29,
            category: Some("Electronics".into()),
        },
        Product {
            id: "p3".into(),
            name: "Desk".into(),
            price: 299,
            category: Some("Furniture".into()),
        },
        Product {
            id: "p4".into(),
            name: "Chair".into(),
            price: 199,
            category: Some("Furniture".into()),
        },
        Product {
            id: "p5".into(),
            name: "Notebook".into(),
            price: 5,
            category: Some("Office".into()),
        },
        Product {
            id: "p6".into(),
            name: "Mystery Box".into(),
            price: 50,
            category: None,
        },
    ];
    let mut builders = Product::new_builders(products.len());
    builders.append_rows(products);
    db.ingest(builders.finish().into_record_batch()).await?;

    // 1. Equality: price == 29
    println!("1. price == 29:");
    let filter = Predicate::eq(ColumnRef::new("price"), ScalarValue::from(29_i64));
    print_products(&db, filter).await?;

    // 2. Comparison: price > 100
    println!("\n2. price > 100:");
    let filter = Predicate::gt(ColumnRef::new("price"), ScalarValue::from(100_i64));
    print_products(&db, filter).await?;

    // 3. Range: 50 <= price <= 300
    println!("\n3. 50 <= price <= 300:");
    let filter = Predicate::and(vec![
        Predicate::gte(ColumnRef::new("price"), ScalarValue::from(50_i64)),
        Predicate::lte(ColumnRef::new("price"), ScalarValue::from(300_i64)),
    ]);
    print_products(&db, filter).await?;

    // 4. IN list: category in ["Electronics", "Office"]
    println!("\n4. category IN ['Electronics', 'Office']:");
    let filter = Predicate::in_list(
        ColumnRef::new("category"),
        vec![
            ScalarValue::from("Electronics"),
            ScalarValue::from("Office"),
        ],
    );
    print_products(&db, filter).await?;

    // 5. IS NULL: category is null
    println!("\n5. category IS NULL:");
    let filter = Predicate::is_null(ColumnRef::new("category"));
    print_products(&db, filter).await?;

    // 6. IS NOT NULL: category is not null
    println!("\n6. category IS NOT NULL:");
    let filter = Predicate::is_not_null(ColumnRef::new("category"));
    print_products(&db, filter).await?;

    // 7. AND: Electronics AND price < 100
    println!("\n7. category == 'Electronics' AND price < 100:");
    let filter = Predicate::and(vec![
        Predicate::eq(ColumnRef::new("category"), ScalarValue::from("Electronics")),
        Predicate::lt(ColumnRef::new("price"), ScalarValue::from(100_i64)),
    ]);
    print_products(&db, filter).await?;

    // 8. OR: Furniture OR price < 10
    println!("\n8. category == 'Furniture' OR price < 10:");
    let filter = Predicate::or(vec![
        Predicate::eq(ColumnRef::new("category"), ScalarValue::from("Furniture")),
        Predicate::lt(ColumnRef::new("price"), ScalarValue::from(10_i64)),
    ]);
    print_products(&db, filter).await?;

    // 9. NOT: NOT category == 'Electronics'
    println!("\n9. NOT category == 'Electronics':");
    let filter = Predicate::eq(ColumnRef::new("category"), ScalarValue::from("Electronics")).not();
    print_products(&db, filter).await?;

    // 10. Complex: (Electronics OR Furniture) AND price > 100
    println!("\n10. (Electronics OR Furniture) AND price > 100:");
    let filter = Predicate::and(vec![
        Predicate::or(vec![
            Predicate::eq(ColumnRef::new("category"), ScalarValue::from("Electronics")),
            Predicate::eq(ColumnRef::new("category"), ScalarValue::from("Furniture")),
        ]),
        Predicate::gt(ColumnRef::new("price"), ScalarValue::from(100_i64)),
    ]);
    print_products(&db, filter).await?;

    Ok(())
}

async fn print_products(
    db: &DB<LocalFs, TokioExecutor>,
    filter: Predicate,
) -> Result<(), Box<dyn std::error::Error>> {
    let batches = db.scan().filter(filter).collect().await?;
    let mut found = false;
    for batch in &batches {
        for p in batch.iter_views::<Product>()?.try_flatten()? {
            let cat = p.category.unwrap_or("NULL");
            println!("  {} - {} (${}) [{}]", p.id, p.name, p.price, cat);
            found = true;
        }
    }
    if !found {
        println!("  (no results)");
    }
    Ok(())
}
