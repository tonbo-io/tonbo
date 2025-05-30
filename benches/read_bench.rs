mod common;

use std::{
    collections::Bound,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use common::{gen_records, make_rng, NUMBER_RECORD};
use futures_util::{future::join_all, StreamExt};
use tokio::{fs, io::AsyncWriteExt};

use crate::common::{
    read_tbl, BenchDatabase, BenchReadTransaction, BenchReader, RedbBenchDatabase,
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
    path: impl AsRef<Path> + Clone,
    ranges: &[(Bound<i32>, Bound<i32>)],
) -> Vec<(String, Duration)> {
    let mut results = Vec::new();
    let db = Arc::new(T::build(path).await);
    let txn = db.read_transaction().await;
    {
        // {
        //     let start = Instant::now();
        //     let len = txn.get_reader().len();
        //     assert_eq!(len, ELEMENTS as u64 + 100_000 + 100);
        //     let end = Instant::now();
        //     let duration = end - start;
        //     println!("{}: len() in {}ms", T::db_type_name(), duration.as_millis());
        //     results.push(("len()".to_string(), duration));
        // }

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
                    let range = &ranges[i + n * cnt];
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

    #[cfg(feature = "load_tbl")]
    {
        use crate::common::{BenchInserter, BenchWriteTransaction};

        let tbl_path = data_dir.join("customer.tbl");

        async fn load<T: BenchDatabase>(tbl_path: impl AsRef<Path>, path: impl AsRef<Path>) {
            if path.as_ref().exists() {
                return;
            }

            println!("{}: start loading", T::db_type_name());
            let database = T::build(path).await;

            for customer in gen_records(NUMBER_RECORD) {
                let mut tx = database.write_transaction().await;
                let mut inserter = tx.get_inserter();
                inserter.insert(customer).unwrap();
                drop(inserter);
                tx.commit().await.unwrap();
            }
            println!("{}: loading completed", T::db_type_name());
        }

        load::<TonboBenchDataBase>(&tbl_path, data_dir.join("tonbo")).await;
        load::<RocksdbBenchDatabase>(&tbl_path, data_dir.join("rocksdb")).await;
        load::<TonboS3BenchDataBase>(&tbl_path, data_dir.join("tonbo_s3")).await;
    }

    let ranges = ranges();
    let tonbo_latency_results =
        { benchmark::<TonboBenchDataBase>(data_dir.join("tonbo"), &ranges).await };
    let rocksdb_results =
        { benchmark::<RocksdbBenchDatabase>(data_dir.join("rocksdb"), &ranges).await };
    let tonbo_s3_latency_results =
        { benchmark::<TonboS3BenchDataBase>(data_dir.join("tonbo_s3"), &ranges).await };

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
