mod common;

use std::{
    collections::Bound,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::{future::join_all, StreamExt};
use tokio::{fs, io::AsyncWriteExt};

use crate::common::{
    read_tbl, BenchDatabase, BenchReadTransaction, BenchReader, RedbBenchDatabase,
    RocksdbBenchDatabase, SledBenchDatabase, TonboBenchDataBase, ITERATIONS, NUM_SCAN, READ_TIMES,
};

async fn benchmark<T: BenchDatabase + Send + Sync>(
    path: impl AsRef<Path> + Clone,
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
            for _ in 0..READ_TIMES {
                let mut iter = Box::pin(reader.range_from((Bound::Unbounded, Bound::Unbounded)));
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
            for _ in 0..READ_TIMES {
                let mut iter =
                    Box::pin(reader.projection_range_from((Bound::Unbounded, Bound::Unbounded)));
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

        let futures = (0..num_threads).map(|_| {
            let db2 = db.clone();

            async move {
                let txn = db2.read_transaction().await;
                let reader = txn.get_reader();
                for _ in 0..(READ_TIMES / num_threads) {
                    let mut iter = Box::pin(
                        reader.projection_range_from((Bound::Unbounded, Bound::Unbounded)),
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
    let data_dir = PathBuf::from("/home/kkould/benchmark");

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

            for customer in read_tbl(tbl_path) {
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
    }

    let tonbo_latency_results = { benchmark::<TonboBenchDataBase>(data_dir.join("tonbo")).await };
    let rocksdb_results = { benchmark::<RocksdbBenchDatabase>(data_dir.join("rocksdb")).await };

    let mut rows: Vec<Vec<String>> = Vec::new();

    for (benchmark, _duration) in &tonbo_latency_results {
        rows.push(vec![benchmark.to_string()]);
    }

    for results in [tonbo_latency_results, rocksdb_results] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header(["", "tonbo", "rocksdb"]);
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
