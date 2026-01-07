# RFC: Logging and Tracing

- Status: Draft
- Authors: Tonbo team
- Created: 2026-01-07
- Area: Observability

## Summary

Adopt [`tracing`](https://docs.rs/tracing) as Tonbo's observability facade for structured events and async-aware spans. Tonbo emits events and spans only; applications configure subscribers, formatting, and sinks. This design satisfies Tonbo's library constraints (no global init, opt-in, async-friendly) while providing first-class span support for correlating async storage operations.

## Motivation

Tonbo is a library embedded in user applications. Observability must be:

- **Optional**: zero overhead when disabled; no-op when no subscriber is configured
- **Non-invasive**: Tonbo never initializes global state; configuration is the application's responsibility
- **Async-aware**: storage operations (WAL append, compaction, range scans) span multiple async tasks; causal correlation is essential for debugging
- **Structured**: machine-parsable output enables operational dashboards, alerting, and integration with OpenTelemetry backends
- **Ecosystem-aligned**: major async Rust projects (tokio, tower, axum, sqlx, hyper) have converged on `tracing`

The `log` facade satisfies basic requirements but lacks native spans, making async correlation manual and error-prone. `tracing` provides spans as a first-class primitive while maintaining the same library-safe integration model.

## Goals

- Emit structured events with consistent field conventions (`event`, `component`, key-value pairs)
- Provide spans around high-level async operations (WAL lifecycle, compaction, scans)
- Support OpenTelemetry integration for distributed tracing
- Maintain zero overhead when no subscriber is configured
- Preserve logfmt-compatible output via subscriber configuration

## Non-Goals

- Tonbo will not initialize any global subscriber or logger
- Tonbo will not provide subscriber configuration helpers (docs-only guidance)
- Tonbo will not implement custom tracing backends
- Metrics collection is out of scope (separate concern)

## Design

### Option Comparison

| Criteria | A: log facade | B: Hybrid log+tracing | C: Full tracing | D: slog | E: log+kv |
|----------|---------------|----------------------|-----------------|---------|-----------|
| **Span support** | None | Yes | Yes | Limited | None |
| **Structured output** | String logfmt | String + fields | Native fields | Native fields | Native fields |
| **Ecosystem maturity** | Very high | High | Very high | Medium | Medium |
| **Perf (disabled)** | Lowest | Low | Low | Medium | Low |
| **Perf (enabled)** | Low | Low-medium | Low-medium | Medium | Low |
| **Async correlation** | Manual IDs | Spans | Spans | Context | Manual IDs |
| **Dependencies** | 1 | 2-3 | 1-2 | 2+ | 1 |
| **OpenTelemetry ready** | No | Partial | Yes | Partial | No |
| **Library integration** | Simple | Simple | Simple | Invasive | Simple |
| **WASM compatibility** | Good | Partial | Partial | Limited | Good |

### Option Details

#### Option A: `log` Facade

**Summary**: Use the `log` crate with custom Tonbo macros and logfmt string conventions.

| Aspect | Assessment |
|--------|------------|
| Pros | Minimal dependencies; very low overhead when disabled; maximum ecosystem compatibility |
| Cons | No native spans; structured data is string-based; harder async correlation |
| Trade-offs | Simplicity and maturity vs. weaker observability story |
| Async/spans | None - must emulate with manual IDs |
| Integration | Emit events only; host configures logger |

#### Option B: Hybrid `log` + `tracing`

**Summary**: Keep `log` macros for events while adding `tracing` spans around async workflows; use `tracing-log` bridge for unified output.

| Aspect | Assessment |
|--------|------------|
| Pros | Incremental migration; works with both ecosystems |
| Cons | Two dependencies; dual mental models; risk of double logging |
| Trade-offs | Compatibility vs. complexity |
| Async/spans | Strong - spans propagate across async tasks |
| Integration | Emit log events + tracing spans; apps choose subscriber/logger |

#### Option C: Full `tracing` Adoption

**Summary**: Use `tracing` for all events and spans. Standardize structured fields and rely on subscriber layers in applications.

| Aspect | Assessment |
|--------|------------|
| Pros | First-class spans; structured fields; async correlation; OpenTelemetry ready; rich filtering |
| Cons | Larger dependency than log-only; WASM story still maturing |
| Trade-offs | Best long-term model; slight footprint increase |
| Async/spans | Excellent - core design principle |
| Integration | Emit events/spans only; host configures subscriber |

#### Option D: `slog` Structured Logging

**Summary**: Use `slog` for structured key-value logging with async drains.

| Aspect | Assessment |
|--------|------------|
| Pros | Structured fields first-class; context propagation via Logger hierarchy |
| Cons | Declining ecosystem; no first-class spans; more boilerplate |
| Trade-offs | Strong structured logs vs. ecosystem isolation |
| Async/spans | Limited - context exists but not trace semantics |
| Integration | Expose/accept Logger in builders; more invasive API |

#### Option E: `log` with Key-Value Support

**Summary**: Use `log` crate's unstable `kv` feature for structured fields.

| Aspect | Assessment |
|--------|------------|
| Pros | Minimal migration; structured fields without new dependency |
| Cons | `kv` feature still stabilizing; no spans; backend support varies |
| Trade-offs | Structured logging without tracing overhead vs. unstable API |
| Async/spans | None |
| Integration | Same as Option A with structured fields |

### Recommendation

**Adopt Option C: Full `tracing`**

For the CEO and leadership:

- **Risk**: Low. `tracing` is the de-facto standard for async Rust, maintained by the Tokio project, and adopted by the ecosystem Tonbo targets.
- **Cost**: Minimal. Dependency footprint is comparable to `log` + subscribers. No global init means no integration burden for users.
- **Benefit**: First-class async correlation enables debugging complex storage operations across WAL, compaction, and query paths. OpenTelemetry compatibility positions Tonbo for enterprise observability stacks.

For the engineering team:

- **Why not `log`**: No spans. Async correlation requires manual ID threading, which is error-prone and verbose. The `kv` feature is unstable.
- **Why not `slog`**: Ecosystem is declining. TiKV's usage is legacy; new projects are not adopting it. API is more invasive (Logger passing).
- **Why not hybrid**: Added complexity with no benefit since there's no existing `log` implementation to preserve.

Trade-offs acknowledged:

- WASM subscriber story is less mature than native (mitigation: conditional compilation, feature flags)
- Slightly higher baseline overhead than `log` when enabled (mitigation: callsite filtering, `tracing::enabled!` guards)
- Some users may prefer `log`-only (mitigation: `tracing-log` bridge allows apps to forward to `log` backends)

### Event and Span Conventions

**Events**:
```rust
tracing::info!(
    event = "wal_segment_rotated",
    component = "wal",
    segment_id = %id,
    size_bytes = size,
);
```

**Spans**:
```rust
#[tracing::instrument(name = "wal::replay", skip(self))]
async fn replay(&self) -> Result<()> {
    // ...
}
```

**Field conventions**:
- `event`: snake_case event name (required for discrete events)
- `component`: module/subsystem identifier
- Use `%` for Display, `?` for Debug formatting
- Avoid high-cardinality fields (user IDs, request IDs) without sampling

### Dependencies

```toml
[dependencies]
tracing = { version = "0.1", default-features = false, features = ["attributes"] }

[dev-dependencies]
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

Optional feature for applications wanting spans in release builds:
```toml
[features]
tracing = []  # Enable spans; events always available
```

### Application Integration

**Minimal (development)**:
```rust
fn main() {
    tracing_subscriber::fmt::init();
    // RUST_LOG=tonbo=debug cargo run
}
```

**Production (JSON + non-blocking file)**:
```rust
fn main() {
    let file_appender = tracing_appender::rolling::daily("logs", "app.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .json()
        .with_writer(non_blocking)
        .with_env_filter("info,tonbo=debug")
        .init();
}
```

**OpenTelemetry**:
```rust
fn main() {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .unwrap();

    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(tracing_subscriber::fmt::layer())
        .init();
}
```

## Alternatives Considered

- **`slog`**: Strong structured logging but declining ecosystem and no first-class spans. TiKV pattern is legacy.
- **`log` + `kv`**: Promising but `kv` feature is unstable; no span support regardless.
- **Custom facade**: Unnecessary complexity; standard crates are well-maintained and widely adopted.

## Comparison with Other Systems

| Project | Crate | Spans | Notes |
|---------|-------|-------|-------|
| **Tokio** | tracing | Yes | Feature-gated (`tokio_unstable`); `tokio-console` for runtime observability |
| **Tower** | tracing | Yes | Middleware-based (`TraceLayer`); composable |
| **SQLx** | log+tracing | Yes | Dual support via bridge; query logging with slow-query detection |
| **Rustls** | log | No | Feature-flagged; security focus (no secrets in logs) |
| **Sled** | log | No | Minimal instrumentation |
| **DataFusion** | log | No | Consumer-configured |
| **TiKV** | slog+tracing | Yes | Custom `tikv_util` crate; enterprise-scale |
| **RocksDB** | Custom Logger | No | Pluggable interface; env-based config; maps to Rust's `Log`/`Subscriber` traits |

Tonbo aligns with the tokio/tower/sqlx pattern: `tracing` with library-safe integration (no global init), spans for async operations, and application-controlled subscribers.

## Future Work

- **Span coverage**: Instrument WAL append, replay, compaction, range scans, manifest operations
- **WASM**: Conditional compilation for `wasm32` targets; evaluate `tracing-wasm` or custom subscriber
- **Format standardization**: Document logfmt layer configuration for users preferring RocksDB-style output
- **Cardinality guidelines**: Define which fields are safe for high-volume paths vs. debug-only
- **Sampling**: Evaluate span sampling strategies for hot paths (per-record operations)

## References

- [`tracing`](https://docs.rs/tracing)
- [`tracing-subscriber`](https://docs.rs/tracing-subscriber)
- [`tracing-appender`](https://docs.rs/tracing-appender)
- [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry)
- [`log`](https://docs.rs/log)
- [Tokio tracing guide](https://tokio.rs/tokio/topics/tracing)
- [RocksDB Logger wiki](https://github.com/facebook/rocksdb/wiki/Logger)
