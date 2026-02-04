//! Logging infrastructure for Tonbo observability.
//!
//! Tonbo uses `tracing` for structured logging. All events use target "tonbo"
//! and include an `event` field for filtering.
//!
//! ## Library Integration
//!
//! Tonbo never initializes a global subscriber. Applications configure
//! tracing via `tracing_subscriber` or similar.
//!
//! ## Conventions
//!
//! - `event`: snake_case event name (required)
//! - `component`: module/subsystem (e.g., "wal", "compaction")
//! - Use `%` for Display, `?` for Debug formatting
//! - Avoid high-cardinality fields without sampling

/// Target for all Tonbo log events.
pub(crate) const TONBO_TARGET: &str = "tonbo";

/// Macro for info-level log events.
///
/// # Example
/// ```ignore
/// log_info!(
///     component = "wal",
///     event = "wal_enabled",
///     wal_dir = %dir,
///     queue_size = cfg.queue_size,
/// );
/// ```
macro_rules! log_info {
    ($($field:tt)*) => {
        ::tracing::info!(target: $crate::observability::TONBO_TARGET, $($field)*)
    };
}

/// Macro for debug-level log events.
macro_rules! log_debug {
    ($($field:tt)*) => {
        ::tracing::debug!(target: $crate::observability::TONBO_TARGET, $($field)*)
    };
}

/// Macro for warn-level log events.
macro_rules! log_warn {
    ($($field:tt)*) => {
        ::tracing::warn!(target: $crate::observability::TONBO_TARGET, $($field)*)
    };
}

/// Macro for error-level log events.
#[allow(unused_macros)]
macro_rules! log_error {
    ($($field:tt)*) => {
        ::tracing::error!(target: $crate::observability::TONBO_TARGET, $($field)*)
    };
}

pub(crate) use log_debug;
#[allow(unused_imports)]
pub(crate) use log_error;
pub(crate) use log_info;
pub(crate) use log_warn;
