# Observability

Tonbo is a library. Logging is optional and a no-op unless the host configures a backend
for the `log` facade. Tonbo never initializes a global logger.

## Logging format

- Target is `tonbo`.
- Messages are logfmt-style `key=value` pairs.
- `event=<name>` is required and uses stable snake_case names.
- Use `component=<name>` to group events (for example `component=wal`).

## Conventions

- Use `LogContext` only for common, cross-module fields.
- Module-specific fields (for example `wal_dir`) stay in the message.
- Avoid large or unbounded payloads; prefer counts, bytes, and milliseconds.
- Guard expensive work with `log::log_enabled!`.

## Usage

Prefer `log_info!`, `log_debug!`, `log_warn!`, and `log_error!`. `tonbo_log!` is the
primitive used by these wrappers.

```rust,ignore
use crate::logging::{LogContext, log_info, log_debug};

const WAL_LOG_CTX: LogContext = LogContext::new("component=wal");

log_info!(ctx: WAL_LOG_CTX, "wal_enabled", "wal_dir={} queue_size={}", cfg.dir, cfg.queue_size);
log_debug!(
    "scan_started",
    "component=scan table_id={} projection_len={}",
    table_id,
    projection_len
);
```

## WAL event catalog (initial)

- `wal_enabled`: wal_dir, segment_max_bytes, segment_max_age_ms, segment_max_age_set,
  flush_interval_ms, sync_policy, sync_value, recovery_mode, retention_bytes,
  retention_bytes_set, queue_size, state_store_present, prune_dry_run, next_payload_seq,
  initial_frame_seq, tail_present
- `wal_writer_spawned`: queue_size, initial_segment_seq, initial_frame_seq, fs_tag
- `wal_writer_bootstrap`: segment_seq, segment_bytes, next_segment_seq, next_frame_seq,
  completed_segments, active_segment_present, state_dirty
- `wal_writer_shutdown`: synced, active_segment_seq, completed_segments
- `wal_segment_rotated`: sealed_seq, sealed_bytes, new_seq, new_bytes, sync_performed,
  completed_segments
- `wal_retention_pruned`: removed_segments, removed_bytes, retention_bytes, retained_bytes
- `wal_prune_dry_run`: floor_seq, removable_segments
- `wal_prune_completed`: floor_seq, removed_segments
- `replay_started`: op=replay, segment_count, floor_present, floor_seq, floor_first_frame
- `replay_completed`: op=replay, segment_count, segments_scanned, events, duration_ms
- `replay_truncated_tail`: op=replay, segment_seq, offset, reason
- `replay_truncated_payload`: op=replay, segment_seq, offset, payload_end, data_len
