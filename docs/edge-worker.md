# Edge/Worker WASM demo

This repo includes a minimal edge/Worker example that ingests a tiny Arrow batch into S3 (LocalStack for local runs) and streams it back out using the wasm `WebExecutor` and fetch-based `wasm-http` stack. OPFS is optional and **not** required here. The code lives under `examples/edge_worker.rs` with the shared helper in `examples/edge_demo.rs` (which can also be built directly as the `edge_demo` example for wasm targets).

## Toolchain & Build

Tested with:
- Rust toolchain: `1.90` (see `rust-toolchain.toml`)
- wasm-bindgen: `0.2.106`
- wasm-pack: optional for tests (`wasm-pack test`), latest stable works
- Target: `wasm32-unknown-unknown` (`rustup target add wasm32-unknown-unknown`)

Build:

```
cargo build --target wasm32-unknown-unknown --no-default-features --features web --example edge_worker
```

To produce JS bindings for a Worker-style environment:

```
wasm-bindgen --target web --out-dir dist target/wasm32-unknown-unknown/debug/examples/edge_worker.wasm
```

To exercise the wasm example in-place with the wasm-bindgen test harness (Node):

```
wasm-pack test --node --no-default-features --features web --test edge_worker
```
Make sure your LocalStack (or other S3-compatible) endpoint is running and the env vars above are set so the wasm build bakes them in.

Environment (compile-time for wasm):
- `FUSIO_EDGE_S3_ENDPOINT` (or `AWS_ENDPOINT_URL`): default `http://localhost:9000`
- `FUSIO_EDGE_S3_BUCKET`: default `fusio-test`
- `FUSIO_EDGE_S3_REGION`: default `us-east-1`
- `FUSIO_EDGE_S3_ACCESS_KEY` / `FUSIO_EDGE_S3_SECRET_KEY`: default `test` / `test`

Local S3 via LocalStack:
```
docker run -d --name fusio-manifest-localstack -p 9000:4566 -e SERVICES=s3 -e DEBUG=0 localstack/localstack:latest
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url=http://localhost:9000 s3 mb s3://fusio-test --region us-east-1
```
Pass the env vars above when running `cargo test`/`wasm-bindgen-test` so the wasm build bakes in the endpoint.

### CORS/ETag setup (required for wasm CAS)

Browsers hide `ETag` unless the bucket exposes it via CORS. Apply this once to the LocalStack bucket so wasm paths receive real ETags (no synthetic fallbacks):

```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url=http://localhost:9000 s3api put-bucket-cors --bucket fusio-test --cors-configuration '{
  "CORSRules": [{
    "AllowedOrigins": ["*"],
    "AllowedMethods": ["GET","PUT","HEAD"],
    "AllowedHeaders": ["*"],
    "ExposeHeaders": ["ETag","x-amz-request-id","x-amz-version-id"],
    "MaxAgeSeconds": 300
  }]
}'
```

Quick verification (shows exposed ETag):

```
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url=http://localhost:9000 s3 cp /etc/hosts s3://fusio-test/demo.txt
curl -I -H 'Origin: http://example.com' -H 'Access-Control-Request-Method: GET' http://localhost:9000/fusio-test/demo.txt
# Expect Access-Control-Expose-Headers with ETag and an actual ETag header
```

## Use from a Worker

The example exports `edge_roundtrip() -> Promise<any>` via `wasm-bindgen`. A minimal Worker bootstrap:

```js
import init, { edge_roundtrip } from "./edge_worker.js";

addEventListener("fetch", event => {
  event.respondWith(handle(event.request));
});

async function handle(_req) {
  await init(); // loads edge_worker_bg.wasm
  const rows = await edge_roundtrip(); // [["alpha",1],["beta",2]]
  return new Response(JSON.stringify(rows), { headers: { "content-type": "application/json" }});
}
```

Feature flags to remember:
- Build with `--no-default-features --features web` to avoid tokio/native TLS in wasm.
- Add `web-opfs` only if you need OPFS-backed storage in the browser.
