// 01: Typed (compile-time) schema: basic insert, scan, and query

use tonbo::{
    db::{DB, TypedMode},
    query::{Expr, Predicate, to_range_set},
    scan::RangeSet,
};

// Define a typed row and mark the key field
#[derive(typed_arrow::Record, Clone, Debug)]
#[record(field_macro = tonbo::key_field)]
struct User {
    #[record(ext(key))]
    id: u32,
    score: i32,
}

fn main() {
    let mut db: DB<TypedMode<User>> = DB::new_typed();

    // Insert regular Rust structs
    for (id, score) in [(1u32, 10), (2, 20), (3, 30)] {
        db.ingest(User { id, score }).unwrap();
    }

    // Scan all rows in key order
    let all = RangeSet::all();
    let rows: Vec<_> = db
        .scan_mutable_rows(&all)
        .map(|r| (r.id, r.score))
        .collect();
    println!("typed rows: {:?}", rows);

    // Query expression: id IN {1,3}
    let expr = Expr::Pred(Predicate::In { set: vec![1u32, 3] });
    let ranges = to_range_set::<User>(&expr);
    let qrows: Vec<_> = db
        .scan_mutable_rows(&ranges)
        .map(|r| (r.id, r.score))
        .collect();
    println!("typed query rows (id IN {{1,3}}): {:?}", qrows);
}
