## Stateless Compaction Trigger (CLI / HTTP)

We deliberately avoided a built-in axum router because the current `DB` is `!Send/!Sync` (mutable/immutable state is not thread-safe). Instead, we provide:

- A CLI trigger: `cargo run --bin tonbo-admin -- trigger --owner cli --idempotency-key <key> --lease-ms 30000`
- Uses the manifest-backed idempotency store (`manifest/.../idempotency/<key>.json`) to reject duplicate work across processes.
  - Demo wiring builds an in-memory DB; replace with your own builder/planner/executor to make it meaningful.

- An HTTP-friendly helper: `admin::server::handle_trigger(state, payload) -> StatusCode`
  - You must host this yourself in a single-threaded runtime (or any framework that doesn’t require `Send + Sync` state). State type is `CompactionHttpState { db, planner, executor }`.
  - You can also use the minimal single-thread listener in `admin::http_loop::serve_single_thread(addr, state)`, which implements a basic `POST /compaction/trigger` handler without needing `Send + Sync`.

Feature flag:
- Enable `http-server` to pull in the optional axum dependency (used only if you build your own router).

Idempotency:
- Duplicate triggers with the same `idempotency_key` are rejected via CAS’d docs under `manifest/version/.../idempotency/<key>.json`.
- Lease TTL defaults to 30s; configurable via CLI `--lease-ms` or HTTP payload `lease_ms`.

Why this shape:
- Avoided forcing `Send + Sync` on `DB` until the runtime is made thread-safe.
- Keeps stateless triggers available to cron/k8s/edge while staying true to current invariants.
