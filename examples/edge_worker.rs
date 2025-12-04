#![cfg(all(target_arch = "wasm32", feature = "web"))]
//! Minimal Worker/edge demo: build with `--no-default-features --features web`
//! and target `wasm32-unknown-unknown`. It ingests a tiny Arrow batch into a
//! memory-backed S3 endpoint and streams it back out through the Parquet reader
//! using the wasm `WebExecutor` plus the fetch-based `wasm-http` client.

use serde_json;
use wasm_bindgen::prelude::*;

mod edge_demo;

#[wasm_bindgen]
pub async fn edge_roundtrip() -> Result<JsValue, JsValue> {
    let rows = edge_demo::edge_roundtrip_rows()
        .await
        .map_err(|err| JsValue::from_str(&err))?;
    let body = serde_json::to_string(&rows)
        .map_err(|err| JsValue::from_str(&format!("serialize: {err}")))?;
    Ok(JsValue::from_str(&body))
}

// Binary entry point required by Cargo examples; unused in wasm bindings.
fn main() {}
