mod common;

use std::{
    env::current_dir,
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use common::*;
use futures_util::{future::join_all, StreamExt};
use tempfile::{NamedTempFile, TempDir};

const WRITE_TIMES: usize = 500_000;
const WRITE_BATCH_TIMES: usize = 5000;
const WRITE_BATCH_SIZE: usize = 100;

async fn benchmark<T: BenchDatabase + Send + Sync>(
    path: impl AsRef<Path> + Clone,
) -> Vec<(String, Duration)> {
    let mut rng = make_rng();
    let mut results = Vec::new();
    let db = Arc::new(T::build(path.clone()).await);

    let start = Instant::now();
    let mut txn = db.write_transaction().await;
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..WRITE_TIMES {
            inserter.insert(gen_record(&mut rng)).unwrap();
        }
    }
    drop(inserter);
    txn.commit().await.unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Bulk loaded {} items in {}ms",
        T::db_type_name(),
        WRITE_TIMES,
        duration.as_millis()
    );
    results.push(("bulk load".to_string(), duration));

    let start = Instant::now();
    {
        for _ in 0..WRITE_TIMES {
            let mut txn = db.write_transaction().await;
            let mut inserter = txn.get_inserter();
            inserter.insert(gen_record(&mut rng)).unwrap();
            drop(inserter);
            txn.commit().await.unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} individual items in {}ms",
        T::db_type_name(),
        WRITE_TIMES,
        duration.as_millis()
    );
    results.push(("individual writes".to_string(), duration));

    for num_threads in [4, 8] {
        let mut rngs = make_rng_shards(num_threads, WRITE_TIMES);
        let start = Instant::now();

        let futures = (0..num_threads).map(|_| {
            let db2 = db.clone();
            let mut rng = rngs.pop().unwrap();

            async move {
                for _ in 0..(WRITE_TIMES / num_threads) {
                    let mut txn = db2.write_transaction().await;
                    let mut inserter = txn.get_inserter();
                    inserter.insert(gen_record(&mut rng)).unwrap();
                    drop(inserter);
                    txn.commit().await.unwrap();
                }
            }
        });
        join_all(futures).await;

        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Wrote {} individual items ({} threads) in {}ms",
            T::db_type_name(),
            WRITE_TIMES,
            num_threads,
            duration.as_millis()
        );
        results.push((
            format!("individual writes ({num_threads} threads)"),
            duration,
        ));
    }

    let start = Instant::now();
    {
        for _ in 0..WRITE_BATCH_TIMES {
            let mut txn = db.write_transaction().await;
            let mut inserter = txn.get_inserter();
            for _ in 0..WRITE_BATCH_SIZE {
                inserter.insert(gen_record(&mut rng)).unwrap();
            }
            drop(inserter);
            txn.commit().await.unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} x {} items in {}ms",
        T::db_type_name(),
        WRITE_BATCH_TIMES,
        WRITE_BATCH_SIZE,
        duration.as_millis()
    );
    results.push(("batch writes".to_string(), duration));

    for num_threads in [4, 8] {
        let mut rngs = make_rng_shards(num_threads, WRITE_BATCH_TIMES);
        let start = Instant::now();

        let futures = (0..num_threads).map(|_| {
            let db2 = db.clone();
            let mut rng = rngs.pop().unwrap();

            async move {
                for _ in 0..(WRITE_BATCH_TIMES / num_threads) {
                    let mut txn = db2.write_transaction().await;
                    let mut inserter = txn.get_inserter();
                    for _ in 0..WRITE_BATCH_SIZE {
                        inserter.insert(gen_record(&mut rng)).unwrap();
                    }
                    drop(inserter);
                    txn.commit().await.unwrap();
                }
            }
        });
        join_all(futures).await;

        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Wrote {} x {} items ({} threads) in {}ms",
            T::db_type_name(),
            WRITE_BATCH_TIMES,
            WRITE_BATCH_SIZE,
            num_threads,
            duration.as_millis()
        );
        results.push((format!("batch writes ({num_threads} threads)"), duration));
    }

    let start = Instant::now();
    let deletes = WRITE_TIMES / 2;
    {
        let mut rng = make_rng();
        let mut txn = db.write_transaction().await;
        let mut inserter = txn.get_inserter();
        for _ in 0..deletes {
            let record = gen_record(&mut rng);
            inserter.remove(record.c_custkey).unwrap();
        }
        drop(inserter);
        txn.commit().await.unwrap();
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Removed {} items in {}ms",
        T::db_type_name(),
        deletes,
        duration.as_millis()
    );
    results.push(("removals".to_string(), duration));

    results
}

#[tokio::main]
async fn main() {
    let tmpdir = current_dir().unwrap().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let tonbo_latency_results = {
        let tmp_dir: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        benchmark::<TonboBenchDataBase>(tmp_dir.path()).await
    };
    let redb_latency_results = {
        let tmp_file: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        benchmark::<RedbBenchDatabase>(tmp_file.path()).await
    };
    let rocksdb_results = {
        let tmp_file: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        benchmark::<RocksdbBenchDatabase>(tmp_file.path()).await
    };
    let sled_results = {
        let tmp_file: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        benchmark::<SledBenchDatabase>(tmp_file.path()).await
    };

    let _ = fs::remove_dir_all(&tmpdir);

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
