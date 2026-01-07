//! Internal logging helpers for structured Tonbo events.

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

pub(crate) use tonbo_log;
