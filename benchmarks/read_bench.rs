mod common;

use std::{
    collections::Bound,
    env::current_dir,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::{future::join_all, StreamExt};

use crate::common::{
    read_tbl, BenchDatabase, BenchReadTransaction, BenchReader, RedbBenchDatabase,
    RocksdbBenchDatabase, SledBenchDatabase, TonboBenchDataBase, ITERATIONS, READ_TIMES,
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
                while let Some(_result) = iter.next().await {}
            }
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random range read in {}ms",
                T::db_type_name(),
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
                while let Some(_result) = iter.next().await {}
            }
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random range projection read in {}ms",
                T::db_type_name(),
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
                    while let Some(_result) = iter.next().await {}
                }
            }
        });
        join_all(futures).await;

        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Random range projection reads ({} threads) in {}ms",
            T::db_type_name(),
            num_threads,
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
    let data_dir = current_dir().unwrap().join("benchmark_data");

    #[cfg(feature = "load_csv")]
    {
        use crate::common::{BenchInserter, BenchWriteTransaction};

        let tbl_path = current_dir().unwrap().join("./benchmark/customer.tbl");

        async fn load<T: BenchDatabase>(tbl_path: impl AsRef<Path>, path: impl AsRef<Path>) {
            println!("{}: start loading", T::db_type_name());
            let database = T::build(path).await;
            let mut tx = database.write_transaction().await;
            let mut inserter = tx.get_inserter();

            for customer in read_tbl(tbl_path) {
                inserter.insert(customer).unwrap();
            }
            drop(inserter);
            tx.commit().await.unwrap();
            println!("{}: loading completed", T::db_type_name());
        }

        load::<TonboBenchDataBase>(&tbl_path, data_dir.join("tonbo")).await;
        load::<RocksdbBenchDatabase>(&tbl_path, data_dir.join("rocksdb")).await;
        load::<RedbBenchDatabase>(&tbl_path, data_dir.join("redb")).await;
        load::<SledBenchDatabase>(&tbl_path, data_dir.join("sled")).await;
    }

    let tonbo_latency_results = { benchmark::<TonboBenchDataBase>(data_dir.join("tonbo")).await };
    let redb_latency_results = { benchmark::<RedbBenchDatabase>(data_dir.join("redb")).await };
    let rocksdb_results = { benchmark::<RocksdbBenchDatabase>(data_dir.join("rocksdb")).await };
    let sled_results = { benchmark::<SledBenchDatabase>(data_dir.join("sled")).await };

    let mut rows: Vec<Vec<String>> = Vec::new();

    for (benchmark, _duration) in &tonbo_latency_results {
        rows.push(vec![benchmark.to_string()]);
    }

    for results in [
        tonbo_latency_results,
        redb_latency_results,
        rocksdb_results,
        sled_results,
    ] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header(["", "tonbo", "redb", "rocksdb", "sled"]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
