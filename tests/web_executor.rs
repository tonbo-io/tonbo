#![cfg(all(target_arch = "wasm32", feature = "web"))]

use std::time::Duration;

use fusio::{
    Read, Write,
    executor::{Executor, JoinHandle, Timer, web::WebExecutor},
    fs::{Fs, OpenOptions},
    impls::mem::fs::InMemoryFs,
};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn spawn_and_sleep_progresses() {
    let exec = WebExecutor::new();
    let value = WebExecutor::rw_lock(0);

    let handle = exec.spawn({
        let value = value.clone();
        async move {
            let mut guard = value.write().await;
            *guard = 7;
        }
    });

    // Join is unjoinable on web; it should error but the task should still run.
    assert!(handle.join().await.is_err());

    exec.sleep(Duration::from_millis(5)).await;
    assert_eq!(*value.read().await, 7);
}

#[wasm_bindgen_test]
async fn in_memory_fs_roundtrip() {
    let fs = InMemoryFs::new();
    let mut file = fs
        .open_options(
            &"web/roundtrip.txt".into(),
            OpenOptions::default()
                .write(true)
                .create(true)
                .truncate(true),
        )
        .await
        .expect("open file");

    let (res, _) = file.write_all(&b"tonbo-web"[..]).await;
    res.expect("write");
    let (res, buf) = file.read_to_end_at(Vec::new(), 0).await;
    res.expect("read");
    assert_eq!(buf, b"tonbo-web");
    file.close().await.expect("close");
}
