# Tonbo (dev branch)

- **Stateless compaction trigger**: CLI (`tonbo-admin trigger --idempotency-key <key>`) and a helper (`admin::server::handle_trigger`) for embedding in your own single-thread HTTP loop. See `docs/admin_trigger.md`. Idempotency is backed by manifest CAS under `idempotency/`.
