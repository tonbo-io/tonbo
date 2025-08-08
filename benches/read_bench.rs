mod common;

use std::{
    collections::Bound,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use common::{gen_records, make_rng};
use futures_util::{future::join_all, StreamExt};
use tokio::{fs, io::AsyncWriteExt};

use crate::common::{
    read_tbl, BenchDatabase, BenchReadTransaction, BenchReader, Customer, RedbBenchDatabase,
    RocksdbBenchDatabase, SledBenchDatabase, TonboBenchDataBase, TonboS3BenchDataBase, ITERATIONS,
    NUM_SCAN, READ_TIMES,
};

fn ranges() -> Vec<(Bound<i32>, Bound<i32>)> {
    let mut rand = make_rng();
    let mut ranges = Vec::with_capacity(READ_TIMES);
    for _ in 0..READ_TIMES {
        let left = rand.i32(..);
        let right = rand.i32(left..);
        ranges.push((Bound::Included(left), Bound::Included(right)));
    }
    ranges
}

async fn benchmark<T: BenchDatabase + Send + Sync>(
    db_instance: Option<T>,
    path: impl AsRef<Path> + Clone,
    ranges: &[(Bound<i32>, Bound<i32>)],
) -> Vec<(String, Duration)> {
    let mut results = Vec::new();

    let db = Arc::new(if let Some(instance) = db_instance {
        instance
    } else {
        T::build(path).await
    });

    let txn = db.read_transaction().await;
    {
        for _ in 0..ITERATIONS {
            let start = Instant::now();
            let reader = txn.get_reader();
            for range in ranges.iter() {
                let mut iter = Box::pin(reader.range_from((range.0.as_ref(), range.1.as_ref())));
                for _ in 0..NUM_SCAN {
                    if let Some(_record) = iter.next().await {
                    } else {
                        break;
                    }
                }
            }
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random range read {} elements in {}ms",
                T::db_type_name(),
                READ_TIMES * NUM_SCAN,
                duration.as_millis()
            );
            results.push(("random range reads".to_string(), duration));
        }

        for _ in 0..ITERATIONS {
            let start = Instant::now();
            let reader = txn.get_reader();
            for range in ranges.iter() {
                let mut iter =
                    Box::pin(reader.projection_range_from((range.0.as_ref(), range.1.as_ref())));
                for _ in 0..NUM_SCAN {
                    if let Some(_projection_field) = iter.next().await {
                    } else {
                        break;
                    }
                }
            }
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random range projection read {} elements in {}ms",
                T::db_type_name(),
                READ_TIMES * NUM_SCAN,
                duration.as_millis()
            );
            results.push(("random range projection reads".to_string(), duration));
        }
    }
    drop(txn);

    for num_threads in [4, 8, 16, 32] {
        let start = Instant::now();

        let futures = (0..num_threads).map(|n| {
            let db2 = db.clone();

            async move {
                let txn = db2.read_transaction().await;
                let reader = txn.get_reader();
                let cnt = READ_TIMES / num_threads;
                for i in 0..cnt {
                    let index = (i + n * cnt) % ranges.len();
                    let range = &ranges[index];
                    let mut iter = Box::pin(
                        reader.projection_range_from((range.0.as_ref(), range.1.as_ref())),
                    );
                    for _ in 0..NUM_SCAN {
                        if let Some(_projection_field) = iter.next().await {
                        } else {
                            break;
                        }
                    }
                }
            }
        });
        join_all(futures).await;

        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Random range projection reads ({} threads) {} elements in {}ms",
            T::db_type_name(),
            num_threads,
            READ_TIMES * NUM_SCAN,
            duration.as_millis()
        );
        results.push((
            format!("random range projection reads ({num_threads} threads)"),
            duration,
        ));
    }
    results
}

#[tokio::main]
async fn main() {
    let data_dir = PathBuf::from("./db_path");

    let mut tonbo_db: Option<TonboBenchDataBase> = None;
    let mut rocksdb_db: Option<RocksdbBenchDatabase> = None;
    let mut tonbo_s3_db: Option<TonboS3BenchDataBase> = None;

    #[cfg(feature = "load_tbl")]
    {
        use crate::common::{BenchInserter, BenchWriteTransaction, NUMBER_RECORD};

        let records = gen_records(NUMBER_RECORD);

        async fn load<T: BenchDatabase>(path: impl AsRef<Path>, records: &[Customer]) -> T {
            let database = T::build(path).await;

            for customer in records.iter() {
                let mut tx = database.write_transaction().await;
                let mut inserter = tx.get_inserter();
                inserter.insert(customer.clone()).unwrap();
                drop(inserter);
                tx.commit().await.unwrap();
            }

            println!("{}: loading completed", T::db_type_name());

            database
        }

        tonbo_db = Some(load::<TonboBenchDataBase>(data_dir.join("tonbo"), &records).await);
        rocksdb_db = Some(load::<RocksdbBenchDatabase>(data_dir.join("rocksdb"), &records).await);
        let s3_path = std::fs::canonicalize(&data_dir).unwrap().join("tonbo_s3");
        tonbo_s3_db = Some(load::<TonboS3BenchDataBase>(s3_path, &records).await);
    }

    let ranges = ranges();
    let tonbo_latency_results =
        { benchmark::<TonboBenchDataBase>(tonbo_db, data_dir.join("tonbo"), &ranges).await };
    let rocksdb_results =
        { benchmark::<RocksdbBenchDatabase>(rocksdb_db, data_dir.join("rocksdb"), &ranges).await };
    let s3_path = std::fs::canonicalize(data_dir.join("tonbo_s3")).unwrap();
    let tonbo_s3_latency_results =
        { benchmark::<TonboS3BenchDataBase>(tonbo_s3_db, s3_path, &ranges).await };

    let mut rows: Vec<Vec<String>> = Vec::new();

    for (benchmark, _duration) in &tonbo_latency_results {
        rows.push(vec![benchmark.to_string()]);
    }

    for results in [
        tonbo_latency_results,
        rocksdb_results,
        tonbo_s3_latency_results,
    ] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header(["", "tonbo", "rocksdb", "tonbo_s3"]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");

    let mut file = fs::File::create("read_benchmark.md").await.unwrap();
    file.write_all(b"Read: \n```shell\n").await.unwrap();
    for line in table.lines() {
        file.write_all(line.as_bytes()).await.unwrap();
        file.write_all(b"\n").await.unwrap();
    }
    file.write_all(b"```").await.unwrap();
    file.flush().await.unwrap();
}
