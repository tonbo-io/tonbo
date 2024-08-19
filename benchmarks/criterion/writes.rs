use std::{iter::repeat_with, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use mimalloc::MiMalloc;
use tonbo::{executor::tokio::TokioExecutor, tonbo_record, DbOption, DB};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tonbo_record]
pub struct KV {
    #[primary_key]
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

    let batches = [1, 128];

    let _ = std::fs::remove_dir_all("/tmp/tonbo");
    for batch in batches {
        let option = DbOption::from("/tmp/tonbo").disable_wal();
        let db = runtime
            .block_on(async { DB::new(option, TokioExecutor::default()).await })
            .unwrap();

        group.bench_with_input(
            format!("tonbo with {} per batch", batch),
            &batch,
            |b, batch| {
                let r = runtime.clone();
                b.to_async(&*r)
                    .iter(|| async { tonbo_write(&db, *batch).await });
            },
        );
        let _ = std::fs::remove_dir_all("/tmp/tonbo");
    }

    let _ = std::fs::remove_dir_all("/tmp/sled");
    for batch in batches {
        let sled = sled::open("/tmp/sled").unwrap();
        group.bench_with_input(
            format!("sled with {} per batch", batch),
            &batch,
            |b, batch| {
                let r = runtime.clone();
                b.to_async(&*r)
                    .iter(|| async { sled_write(&sled, *batch).await });
            },
        );
        let _ = std::fs::remove_dir_all("/tmp/sled");
    }
}

criterion_group!(benches, single_write);
criterion_main!(benches);
