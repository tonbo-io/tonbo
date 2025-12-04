//! Stateless compaction trigger helpers intended for cron/HTTP callers.
//!
//! The helpers here wrap the existing planner/executor pipeline with an optional
//! idempotency lease so repeated triggers do not duplicate work.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Mutex,
    time::{Duration, Instant},
};

use fusio::executor::{Executor, Timer};

use crate::{
    compaction::{
        executor::{CompactionError, CompactionLease, CompactionOutcome},
        planner::CompactionPlanner,
    },
    db::DB,
    manifest::IdempotencyRuntime,
    mode::Mode,
};

/// Parameters supplied by a stateless trigger (cron/HTTP/CLI).
#[derive(Clone, Debug)]
pub struct TriggerRequest {
    /// Human-readable owner for observability.
    pub owner: String,
    /// Optional idempotency key; when present the trigger will no-op if a live lease exists.
    pub idempotency_key: Option<String>,
    /// How long the lease should remain valid.
    pub lease_ttl: Duration,
}

impl TriggerRequest {
    /// Build a request with a default 30s lease.
    pub fn new(owner: impl Into<String>, idempotency_key: Option<String>) -> Self {
        Self {
            owner: owner.into(),
            idempotency_key,
            lease_ttl: Duration::from_secs(30),
        }
    }
}

/// Minimal idempotency backend used to gate duplicate triggers.
pub trait IdempotentCompactionLease: Send + Sync {
    /// Attempt to acquire a lease for `key`. Returns `true` when acquired, `false` if already held.
    fn try_acquire<'a>(
        &'a self,
        key: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, CompactionError>> + Send + 'a>>;
    /// Release a previously acquired lease.
    fn release<'a>(&'a self, key: &'a str) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// In-memory idempotency backend useful for single-process triggers.
#[derive(Debug, Default)]
pub struct InMemoryCompactionLease {
    inner: Mutex<HashMap<String, Instant>>,
}

impl InMemoryCompactionLease {
    /// Create a new in-memory lease backend.
    pub fn new() -> Self {
        Self::default()
    }

    fn prune_expired(&self, now: Instant) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.retain(|_, expiry| *expiry > now);
        }
    }
}

impl IdempotentCompactionLease for InMemoryCompactionLease {
    fn try_acquire<'a>(
        &'a self,
        key: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, CompactionError>> + Send + 'a>> {
        Box::pin(async move {
            let now = Instant::now();
            self.prune_expired(now);
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| CompactionError::LeaseMissing)?;
            if let Some(expiry) = guard.get(key)
                && *expiry > now
            {
                return Ok(false);
            }
            guard.insert(key.to_string(), now + ttl);
            Ok(true)
        })
    }

    fn release<'a>(&'a self, key: &'a str) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if let Ok(mut guard) = self.inner.lock() {
                guard.remove(key);
            }
        })
    }
}

/// Manifest-backed idempotency manager to coordinate compaction triggers across processes.
pub(crate) struct ManifestCompactionLease {
    runtime: std::sync::Arc<dyn IdempotencyRuntime>,
}

impl ManifestCompactionLease {
    pub(crate) fn new(runtime: std::sync::Arc<dyn IdempotencyRuntime>) -> Self {
        Self { runtime }
    }
}

impl IdempotentCompactionLease for ManifestCompactionLease {
    fn try_acquire<'a>(
        &'a self,
        key: &'a str,
        ttl: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, CompactionError>> + Send + 'a>> {
        Box::pin(async move {
            let acquired = self
                .runtime
                .try_acquire(key, ttl)
                .await
                .map_err(CompactionError::Manifest)?;
            Ok(acquired)
        })
    }

    fn release<'a>(&'a self, key: &'a str) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let _ = self.runtime.release(key).await;
        })
    }
}

async fn with_idempotency_guard<L, F, Fut, R>(
    manager: Option<&L>,
    request: &TriggerRequest,
    run: F,
) -> Result<Option<R>, CompactionError>
where
    L: IdempotentCompactionLease,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Option<R>, CompactionError>>,
{
    struct LeaseGuard<'a, L: IdempotentCompactionLease> {
        manager: &'a L,
        key: String,
    }

    if let (Some(manager), Some(key)) = (manager, request.idempotency_key.as_deref()) {
        if !manager.try_acquire(key, request.lease_ttl).await? {
            return Ok(None);
        }
        let guard = LeaseGuard {
            manager,
            key: key.to_string(),
        };
        let result = run().await;
        guard.manager.release(&guard.key).await;
        return result;
    }

    run().await
}

/// One-shot compaction trigger suitable for stateless callers.
///
/// This wraps the existing `run_compaction_task_with_lease` path in an optional idempotency
/// lease. When an idempotency key is supplied and a lease backend is provided, duplicate triggers
/// with the same key will return `Ok(None)` without executing compaction.
pub(crate) async fn stateless_compaction_once<M, E, CE, P, L>(
    db: &DB<M, E>,
    planner: &P,
    executor: &CE,
    request: TriggerRequest,
    idempotency: Option<&L>,
    lease: Option<CompactionLease>,
) -> Result<Option<CompactionOutcome>, CompactionError>
where
    M: Mode,
    M::Key: Eq + Clone,
    E: Executor + Timer,
    CE: crate::compaction::executor::CompactionExecutor,
    P: CompactionPlanner,
    L: IdempotentCompactionLease,
{
    with_idempotency_guard(idempotency, &request, || async {
        db.run_compaction_task_with_lease(planner, executor, lease.clone())
            .await
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::time::{Duration, sleep};

    use super::*;

    #[tokio::test]
    async fn in_memory_lease_expires() {
        let lease = InMemoryCompactionLease::new();
        let key = "k";
        assert!(
            lease
                .try_acquire(key, Duration::from_millis(50))
                .await
                .unwrap()
        );
        assert!(
            !lease
                .try_acquire(key, Duration::from_secs(1))
                .await
                .unwrap()
        );
        sleep(Duration::from_millis(60)).await;
        assert!(
            lease
                .try_acquire(key, Duration::from_secs(1))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn idempotency_guard_skips_duplicate_runs() {
        let lease = Arc::new(InMemoryCompactionLease::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let req = TriggerRequest::new("test", Some("dup".to_string()));
        let calls_clone = Arc::clone(&calls);
        let first_req = req.clone();
        let first = tokio::spawn({
            let lease = Arc::clone(&lease);
            async move {
                with_idempotency_guard(Some(lease.as_ref()), &first_req, || {
                    let calls = Arc::clone(&calls_clone);
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, CompactionError>(Some(()))
                    }
                })
                .await
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        let second = with_idempotency_guard(Some(lease.as_ref()), &req, || {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, CompactionError>(Some(()))
            }
        })
        .await
        .expect("second run");
        assert!(
            second.is_none(),
            "duplicate trigger should be skipped while first holds lease"
        );
        let _ = first.await.expect("first join");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
