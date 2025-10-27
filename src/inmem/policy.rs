//! Sealing (freezing) policy interfaces and default strategies.
//!
//! This module defines a policy-driven approach for deciding when to seal the
//! mutable in-memory state into an immutable run. Policies evaluate cheap
//! in-memory statistics and return a decision with a structured reason.
//!
//! The policy layer is mode-agnostic and uses a unified `MemStats` struct which
//! carries common counters and optional, mode-specific hints.

use std::time::Duration;

/// A unified snapshot of mutable memtable statistics used by sealing policies.
#[derive(Clone, Debug, Default)]
pub struct MemStats {
    /// Number of distinct keys currently indexed in the mutable state.
    pub entries: usize,
    /// Number of inserts observed since the last reset.
    pub inserts: u64,
    /// Number of in-place replacements (last-writer wins) observed since the last reset.
    pub replaces: u64,
    /// Approximate total heap bytes used for keys in the mutable state.
    pub approx_key_bytes: usize,
    /// Per-entry overhead in bytes assumed by the mutable structure.
    pub entry_overhead: usize,

    /// Typed mode only: number of rows currently buffered in the active memtable.
    pub typed_open_rows: Option<usize>,
    /// Dynamic mode only: number of attached `RecordBatch` chunks.
    pub dyn_batches: Option<usize>,
    /// Dynamic mode only: an approximate sum of payload bytes across attached batches (if
    /// available).
    pub dyn_approx_batch_bytes: Option<usize>,

    /// Time elapsed since the last seal decision was applied, if known.
    pub since_last_seal: Option<Duration>,
}

/// The outcome of evaluating a sealing policy.
#[derive(Clone, Debug, PartialEq)]
pub enum SealDecision {
    /// No action is required.
    NoOp,
    /// Seal the current mutable state. Includes a structured reason for observability.
    Seal(SealReason),
}

/// A structured reason for sealing. Useful for diagnostics and metrics.
#[derive(Clone, Debug, PartialEq)]
pub enum SealReason {
    /// Approximate key bytes reached or exceeded the limit.
    ApproxBytesReached { approx: usize, limit: usize },
    /// Open row buffer (typed mode) reached the limit.
    OpenRowsReached { count: usize, limit: usize },
    /// Number of attached batches (dynamic mode) reached the limit.
    BatchesReached { count: usize, limit: usize },
    /// A minimum time interval has elapsed since the last seal.
    TimeElapsed { elapsed: Duration, limit: Duration },
    /// Replaces to inserts ratio suggests clustering sooner.
    ReplaceRatio {
        replaces: u64,
        inserts: u64,
        min_ratio: f64,
    },
    /// Manual or external request to seal.
    Manual,
}

/// A pluggable sealing policy evaluated after ingest.
pub trait SealPolicy {
    /// Evaluate the current statistics and decide whether to seal.
    fn evaluate(&mut self, stats: &MemStats) -> SealDecision;
}

/// A policy that never seals.
///
/// Useful as a safe default when you prefer manual or explicit sealing, or in
/// tests where sealing is controlled by the test case.
#[derive(Clone, Debug, Default)]
#[allow(unused)]
pub struct NeverSeal;

impl SealPolicy for NeverSeal {
    fn evaluate(&mut self, _stats: &MemStats) -> SealDecision {
        SealDecision::NoOp
    }
}

/// Build a conservative, wide-use default sealing policy.
///
/// Triggers when any of these conditions are met:
/// - Approximate key bytes plus per-entry overhead exceed ~8 MiB.
/// - A minimum time interval of 30 seconds has elapsed since the last seal.
/// - Typed mode only: open row buffer reaches 16k rows.
/// - Dynamic mode only: 64 attached batches.
///
/// These values aim to provide sensible out-of-the-box behavior while avoiding
/// overly aggressive sealing for small workloads. Callers can override this
/// policy via `DB::set_seal_policy`.
pub fn default_policy() -> Box<dyn SealPolicy + Send + Sync> {
    use std::time::Duration;
    Box::new(AnyOf::new(vec![
        Box::new(BytesThreshold {
            limit: 64 * 1024 * 1024, // ~64 MiB
        }),
        Box::new(TimeElapsedPolicy {
            min_interval: Duration::from_secs(30),
        }),
        Box::new(OpenRowsThreshold { rows: 16_384 }),
        Box::new(BatchesThreshold { batches: 64 }),
    ]))
}

/// A simple bytes threshold policy: seals when `approx_key_bytes >= limit`.
#[derive(Clone, Debug)]
pub struct BytesThreshold {
    /// Threshold in bytes. Includes key sizes and per-entry overhead in stats.
    pub limit: usize,
}

impl SealPolicy for BytesThreshold {
    fn evaluate(&mut self, stats: &MemStats) -> SealDecision {
        if stats.approx_key_bytes + stats.entries * stats.entry_overhead >= self.limit {
            SealDecision::Seal(SealReason::ApproxBytesReached {
                approx: stats.approx_key_bytes + stats.entries * stats.entry_overhead,
                limit: self.limit,
            })
        } else {
            SealDecision::NoOp
        }
    }
}

/// A simple open-rows threshold for typed mode.
#[derive(Clone, Debug)]
pub struct OpenRowsThreshold {
    /// Maximum number of rows before sealing.
    pub rows: usize,
}

impl SealPolicy for OpenRowsThreshold {
    fn evaluate(&mut self, stats: &MemStats) -> SealDecision {
        match stats.typed_open_rows {
            Some(cnt) if cnt >= self.rows => SealDecision::Seal(SealReason::OpenRowsReached {
                count: cnt,
                limit: self.rows,
            }),
            _ => SealDecision::NoOp,
        }
    }
}

/// A simple batch-count threshold for dynamic mode.
#[derive(Clone, Debug)]
pub struct BatchesThreshold {
    /// Maximum number of attached batches before sealing is suggested.
    pub batches: usize,
}

impl SealPolicy for BatchesThreshold {
    fn evaluate(&mut self, stats: &MemStats) -> SealDecision {
        match stats.dyn_batches {
            Some(cnt) if cnt >= self.batches => SealDecision::Seal(SealReason::BatchesReached {
                count: cnt,
                limit: self.batches,
            }),
            _ => SealDecision::NoOp,
        }
    }
}

/// A time-based policy that seals if enough time has elapsed since the last seal.
#[derive(Clone, Debug)]
pub struct TimeElapsedPolicy {
    /// Minimum interval to elapse between seals.
    pub min_interval: Duration,
}

impl SealPolicy for TimeElapsedPolicy {
    fn evaluate(&mut self, stats: &MemStats) -> SealDecision {
        match stats.since_last_seal {
            Some(elapsed) if elapsed >= self.min_interval => {
                SealDecision::Seal(SealReason::TimeElapsed {
                    elapsed,
                    limit: self.min_interval,
                })
            }
            _ => SealDecision::NoOp,
        }
    }
}

/// A replace ratio policy that suggests sealing when churn is high.
#[derive(Clone, Debug)]
#[allow(unused)]
pub struct ReplaceRatioPolicy {
    /// Minimum replaces/inserts ratio to trigger sealing.
    pub min_ratio: f64,
    /// Minimum number of inserts observed before evaluating the ratio.
    pub min_inserts: u64,
}

impl SealPolicy for ReplaceRatioPolicy {
    fn evaluate(&mut self, stats: &MemStats) -> SealDecision {
        if stats.inserts >= self.min_inserts && stats.inserts > 0 {
            let ratio = stats.replaces as f64 / stats.inserts as f64;
            if ratio >= self.min_ratio {
                return SealDecision::Seal(SealReason::ReplaceRatio {
                    replaces: stats.replaces,
                    inserts: stats.inserts,
                    min_ratio: self.min_ratio,
                });
            }
        }
        SealDecision::NoOp
    }
}

/// Composite policy that triggers if any inner policy triggers.
#[derive(Default)]
pub struct AnyOf {
    inner: Vec<Box<dyn SealPolicy + Send + Sync>>,
}

impl AnyOf {
    /// Create a composite policy from a list of inner policies.
    pub fn new(inner: Vec<Box<dyn SealPolicy + Send + Sync>>) -> Self {
        Self { inner }
    }
}

impl SealPolicy for AnyOf {
    fn evaluate(&mut self, stats: &MemStats) -> SealDecision {
        for p in self.inner.iter_mut() {
            if let SealDecision::Seal(reason) = p.evaluate(stats) {
                return SealDecision::Seal(reason);
            }
        }
        SealDecision::NoOp
    }
}

/// Composite policy that triggers only if all inner policies trigger.
#[derive(Default)]
#[allow(unused)]
pub struct AllOf {
    inner: Vec<Box<dyn SealPolicy + Send + Sync>>,
}

impl AllOf {
    /// Create a composite policy from a list of inner policies.
    #[allow(unused)]
    pub fn new(inner: Vec<Box<dyn SealPolicy + Send + Sync>>) -> Self {
        Self { inner }
    }
}

impl SealPolicy for AllOf {
    fn evaluate(&mut self, stats: &MemStats) -> SealDecision {
        let mut last_reason: Option<SealReason> = None;
        for p in self.inner.iter_mut() {
            match p.evaluate(stats) {
                SealDecision::Seal(r) => last_reason = Some(r),
                SealDecision::NoOp => return SealDecision::NoOp,
            }
        }
        match last_reason {
            Some(r) => SealDecision::Seal(r),
            None => SealDecision::NoOp,
        }
    }
}

/// Provider trait for producing `MemStats` from a concrete mutable structure.
pub(crate) trait StatsProvider {
    /// Build a `MemStats` snapshot. The caller may pass `since_last_seal` to aid time-based
    /// policies.
    fn build_stats(&self, since_last_seal: Option<Duration>) -> MemStats;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_elapsed_policy_triggers_correctly() {
        let mut p = TimeElapsedPolicy {
            min_interval: Duration::from_millis(50),
        };
        let s_short = MemStats {
            since_last_seal: Some(Duration::from_millis(10)),
            ..Default::default()
        };
        let s_long = MemStats {
            since_last_seal: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        assert_eq!(p.evaluate(&s_short), SealDecision::NoOp);
        match p.evaluate(&s_long) {
            SealDecision::Seal(SealReason::TimeElapsed { elapsed, limit }) => {
                assert!(elapsed >= limit);
            }
            other => panic!("unexpected decision: {other:?}"),
        }
    }

    #[test]
    fn anyof_triggers_on_any_inner() {
        let mut any = AnyOf::new(vec![
            Box::new(OpenRowsThreshold { rows: 5 }),
            Box::new(BytesThreshold { limit: 10_000 }),
        ]);
        let s1 = MemStats {
            typed_open_rows: Some(5),
            ..Default::default()
        };
        assert!(matches!(
            any.evaluate(&s1),
            SealDecision::Seal(SealReason::OpenRowsReached { .. })
        ));
    }

    #[test]
    fn allof_requires_all_inners() {
        let mut all = AllOf::new(vec![
            Box::new(OpenRowsThreshold { rows: 2 }),
            Box::new(BytesThreshold { limit: 10 }),
        ]);
        // Only rows threshold met -> NoOp
        let s_only_rows = MemStats {
            typed_open_rows: Some(3),
            entries: 0,
            approx_key_bytes: 5,
            ..Default::default()
        };
        assert_eq!(all.evaluate(&s_only_rows), SealDecision::NoOp);

        // Both thresholds met -> Seal
        let s_both = MemStats {
            typed_open_rows: Some(3),
            entries: 0,
            approx_key_bytes: 20,
            ..Default::default()
        };
        assert!(matches!(all.evaluate(&s_both), SealDecision::Seal(_)));
    }
}
