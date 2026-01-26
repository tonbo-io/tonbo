//! Internal pruning configuration.

use crate::db::StorageBackend;

/// Policy for selecting pruning implementations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrunerPolicy {
    /// Select a policy based on storage backend.
    Auto,
    /// Force Aisle-based pruning.
    Aisle,
    /// Force no-op pruning (baseline).
    Noop,
}

/// Configuration for pruning behavior.
///
/// Bench/test overrides are available via `set_pruning_override` under
/// `cfg(any(test, feature = "bench"))` to force a policy without public API.
#[derive(Clone, Debug)]
pub(crate) struct PruningConfig {
    pub(crate) policy: PrunerPolicy,
    pub(crate) no_pruning: bool,
    pub(crate) enable_bloom: bool,
    pub(crate) enable_page_index: bool,
}

impl Default for PruningConfig {
    fn default() -> Self {
        Self {
            policy: PrunerPolicy::Auto,
            no_pruning: false,
            enable_bloom: true,
            enable_page_index: true,
        }
    }
}

impl PruningConfig {
    /// Resolve benchmark/test overrides and return the effective config.
    pub(crate) fn effective(&self) -> PruningConfig {
        #[cfg(any(test, feature = "bench"))]
        if let Some(override_cfg) = get_pruning_override() {
            return override_cfg;
        }
        self.clone()
    }

    /// Resolve the effective policy given a storage backend.
    pub(crate) fn resolve_policy(&self, backend: StorageBackend) -> PrunerPolicy {
        if self.no_pruning {
            return PrunerPolicy::Noop;
        }
        match self.policy {
            PrunerPolicy::Auto => match backend {
                StorageBackend::S3 => PrunerPolicy::Aisle,
                StorageBackend::Disk | StorageBackend::Memory | StorageBackend::Unknown => {
                    PrunerPolicy::Noop
                }
            },
            policy => policy,
        }
    }
}

#[cfg(any(test, feature = "bench"))]
mod overrides {
    use std::sync::OnceLock;

    use parking_lot::RwLock;

    use super::PruningConfig;

    static OVERRIDE: OnceLock<RwLock<Option<PruningConfig>>> = OnceLock::new();

    fn slot() -> &'static RwLock<Option<PruningConfig>> {
        OVERRIDE.get_or_init(|| RwLock::new(None))
    }

    pub(crate) fn set_pruning_override(config: PruningConfig) {
        *slot().write() = Some(config);
    }

    pub(crate) fn get_pruning_override() -> Option<PruningConfig> {
        slot().read().clone()
    }
}

#[cfg(any(test, feature = "bench"))]
pub(crate) use overrides::{get_pruning_override, set_pruning_override};

#[cfg(any(test, feature = "bench"))]
/// Benchmark-facing pruning modes, independent of pruning implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkPruningMode {
    /// Let the pruning layer pick based on storage backend.
    Auto,
    /// Force pruning to be enabled (uses the default implementation).
    Enabled,
    /// Disable pruning and always scan all row groups.
    Disabled,
}

#[cfg(any(test, feature = "bench"))]
/// Benchmark-only pruning overrides (no implementation details exposed).
#[derive(Clone, Debug)]
pub struct BenchmarkPruningConfig {
    /// How pruning should be selected for the benchmark run.
    pub mode: BenchmarkPruningMode,
    /// Toggle bloom-filter pruning.
    pub enable_bloom: bool,
    /// Toggle page-index pruning.
    pub enable_page_index: bool,
}

#[cfg(any(test, feature = "bench"))]
impl Default for BenchmarkPruningConfig {
    fn default() -> Self {
        Self {
            mode: BenchmarkPruningMode::Auto,
            enable_bloom: true,
            enable_page_index: true,
        }
    }
}

#[cfg(any(test, feature = "bench"))]
impl BenchmarkPruningConfig {
    pub(crate) fn to_pruning_config(&self) -> PruningConfig {
        let (policy, no_pruning) = match self.mode {
            BenchmarkPruningMode::Auto => (PrunerPolicy::Auto, false),
            BenchmarkPruningMode::Enabled => (PrunerPolicy::Aisle, false),
            BenchmarkPruningMode::Disabled => (PrunerPolicy::Noop, true),
        };

        PruningConfig {
            policy,
            no_pruning,
            enable_bloom: self.enable_bloom,
            enable_page_index: self.enable_page_index,
        }
    }
}
