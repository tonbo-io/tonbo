//! Internal logging helpers for structured Tonbo events.
//!
//! Goals:
//! - Single logging target: `tonbo`.
//! - Logging is optional; Tonbo never initializes a global logger.
//! - Messages are logfmt-style `key=value` pairs.
//!
//! Conventions:
//! - `event=<name>` is required and uses stable snake_case names.
//! - Use `LogContext` only for common, cross-module fields (for example `component=wal`).
//! - Module-specific fields (for example `wal_dir`) stay in the message.
//! - Avoid large or unbounded payloads; prefer counts, bytes, and milliseconds.
//! - Guard expensive work with `log::log_enabled!`.
//!
//! Usage:
//! ```rust,ignore
//! use crate::logging::{LogContext, log_info, log_debug};
//!
//! const WAL_LOG_CTX: LogContext = LogContext::new("component=wal");
//! log_info!(ctx: WAL_LOG_CTX, "wal_enabled", "wal_dir={} queue_size={}", cfg.dir, cfg.queue_size);
//! log_debug!(
//!     "scan_started",
//!     "component=scan table_id={} projection_len={}",
//!     table_id,
//!     projection_len
//! );
//! ```

/// Single logging target for Tonbo.
pub(crate) const LOG_TARGET: &str = "tonbo";

/// Optional common key/value fields appended to all logs in a scope.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LogContext {
    common_kv: &'static str,
}

impl LogContext {
    /// Build a context that appends the provided key/value pairs.
    pub(crate) const fn new(common_kv: &'static str) -> Self {
        Self { common_kv }
    }

    pub(crate) fn common_kv(&self) -> Option<&'static str> {
        if self.common_kv.is_empty() {
            None
        } else {
            Some(self.common_kv)
        }
    }
}

/// Primitive logging macro with optional `LogContext`.
macro_rules! tonbo_log {
    ($level:expr, $event:expr, $fmt:expr $(, $args:expr)* $(,)?) => {{
        if log::log_enabled!($level) {
            log::log!(
                target: crate::logging::LOG_TARGET,
                $level,
                "event={} {}",
                $event,
                format_args!($fmt $(, $args)*)
            );
        }
    }};
    ($level:expr, ctx: $ctx:expr, $event:expr, $fmt:expr $(, $args:expr)* $(,)?) => {{
        if log::log_enabled!($level) {
            if let Some(common_kv) = $ctx.common_kv() {
                log::log!(
                    target: crate::logging::LOG_TARGET,
                    $level,
                    "event={} {} {}",
                    $event,
                    common_kv,
                    format_args!($fmt $(, $args)*)
                );
            } else {
                log::log!(
                    target: crate::logging::LOG_TARGET,
                    $level,
                    "event={} {}",
                    $event,
                    format_args!($fmt $(, $args)*)
                );
            }
        }
    }};
}

/// Emit an info-level Tonbo log event.
macro_rules! log_info {
    (ctx: $ctx:expr, $event:expr, $fmt:expr $(, $args:expr)* $(,)?) => {{
        $crate::logging::tonbo_log!(log::Level::Info, ctx: $ctx, $event, $fmt $(, $args)*);
    }};
    ($event:expr, $fmt:expr $(, $args:expr)* $(,)?) => {{
        $crate::logging::tonbo_log!(log::Level::Info, $event, $fmt $(, $args)*);
    }};
}

/// Emit a debug-level Tonbo log event.
macro_rules! log_debug {
    (ctx: $ctx:expr, $event:expr, $fmt:expr $(, $args:expr)* $(,)?) => {{
        $crate::logging::tonbo_log!(log::Level::Debug, ctx: $ctx, $event, $fmt $(, $args)*);
    }};
    ($event:expr, $fmt:expr $(, $args:expr)* $(,)?) => {{
        $crate::logging::tonbo_log!(log::Level::Debug, $event, $fmt $(, $args)*);
    }};
}

/// Emit a warn-level Tonbo log event.
#[allow(unused_macros)]
macro_rules! log_warn {
    (ctx: $ctx:expr, $event:expr, $fmt:expr $(, $args:expr)* $(,)?) => {{
        $crate::logging::tonbo_log!(log::Level::Warn, ctx: $ctx, $event, $fmt $(, $args)*);
    }};
    ($event:expr, $fmt:expr $(, $args:expr)* $(,)?) => {{
        $crate::logging::tonbo_log!(log::Level::Warn, $event, $fmt $(, $args)*);
    }};
}

/// Emit an error-level Tonbo log event.
#[allow(unused_macros)]
macro_rules! log_error {
    (ctx: $ctx:expr, $event:expr, $fmt:expr $(, $args:expr)* $(,)?) => {{
        $crate::logging::tonbo_log!(log::Level::Error, ctx: $ctx, $event, $fmt $(, $args)*);
    }};
    ($event:expr, $fmt:expr $(, $args:expr)* $(,)?) => {{
        $crate::logging::tonbo_log!(log::Level::Error, $event, $fmt $(, $args)*);
    }};
}

pub(crate) use log_debug;
#[allow(unused_imports)]
pub(crate) use log_error;
pub(crate) use log_info;
#[allow(unused_imports)]
pub(crate) use log_warn;
pub(crate) use tonbo_log;
