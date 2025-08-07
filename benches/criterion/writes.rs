use std::{iter::repeat_with, sync::Arc};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use mimalloc::MiMalloc;
use tonbo::{executor::tokio::TokioExecutor, DbOption, Record, DB};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Record, Debug)]
pub struct KV {
    #[record(primary_key)]
    key: String,
    value: String,
}

#[inline(never)]
async fn tonbo_write(db: &DB<KV, TokioExecutor>, batch_size: usize) {
    let mut kvs = Vec::with_capacity(128);
    for _ in 0..batch_size {
        let key = repeat_with(fastrand::alphanumeric).take(256).collect();
        let value = repeat_with(fastrand::alphanumeric).take(256).collect();
        let kv = KV { key, value };
        kvs.push(kv);
    }

    db.insert_batch(kvs.into_iter()).await.unwrap();
}

#[inline(never)]
async fn sled_write(db: &sled::Db, batch_size: usize) {
    let mut kvs = Vec::with_capacity(128);
    for _ in 0..batch_size {
        let key: String = repeat_with(fastrand::alphanumeric).take(256).collect();
        let value: String = repeat_with(fastrand::alphanumeric).take(256).collect();
        kvs.push((key, value));
    }

    for (key, value) in kvs {
        db.insert(&key, &*value).unwrap();
    }
}

fn single_write(c: &mut Criterion) {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap(),
    );

    let mut group = c.benchmark_group("write");

    let batches = [1, 16, 128];

    let _ = std::fs::remove_dir_all("/tmp/tonbo");
    let _ = std::fs::create_dir_all("/tmp/tonbo");

    for batch in batches {
        let option = DbOption::new(
            fusio::path::Path::from_filesystem_path("/tmp/tonbo").unwrap(),
            &KVSchema,
        )
        .disable_wal();
        let db = runtime
            .block_on(async { DB::new(option, TokioExecutor::default(), KVSchema).await })
            .unwrap();

        group.bench_with_input(BenchmarkId::new("Tonbo", batch), &batch, |b, batch| {
            let r = runtime.clone();
            b.to_async(&*r)
                .iter(|| async { tonbo_write(&db, *batch).await });
        });
        let _ = std::fs::remove_dir_all("/tmp/tonbo");
        let _ = std::fs::create_dir_all("/tmp/tonbo");
    }

    let _ = std::fs::remove_dir_all("/tmp/sled");
    let _ = std::fs::create_dir_all("/tmp/sled");

    for batch in batches {
        let sled = sled::open("/tmp/sled").unwrap();
        group.bench_with_input(BenchmarkId::new("Sled", batch), &batch, |b, batch| {
            let r = runtime.clone();
            b.to_async(&*r)
                .iter(|| async { sled_write(&sled, *batch).await });
        });
        let _ = std::fs::remove_dir_all("/tmp/sled");
        let _ = std::fs::create_dir_all("/tmp/sled");
    }

    group.finish();
}

criterion_group!(benches, single_write);
criterion_main!(benches);
