mod backend;
mod core;
mod public_api;
mod wal_gc;
mod wal_recovery;

#[cfg(all(target_arch = "wasm32", feature = "web"))]
mod wasm_web;
