#![cfg(all(target_arch = "wasm32", feature = "web"))]

#[path = "../examples/edge_demo.rs"]
mod edge_demo;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn edge_roundtrip_streams_rows() {
    // Ensure manifest catalog is initialized; fail fast with context instead of synthesizing state.
    let prefix = std::env::var("FUSIO_EDGE_S3_BUCKET").unwrap_or_else(|_| "fusio-test".to_string());
    assert!(
        !prefix.is_empty(),
        "FUSIO_EDGE_S3_BUCKET must be set for edge_roundtrip_streams_rows"
    );

    let rows = edge_demo::edge_roundtrip_rows()
        .await
        .expect("edge roundtrip");
    assert_eq!(
        rows,
        vec![("alpha".to_string(), 1), ("beta".to_string(), 2)]
    );
}
