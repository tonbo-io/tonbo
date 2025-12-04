//! Minimal admin HTTP helpers for stateless compaction triggers.

use std::sync::Arc;

use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
    compaction::{
        executor::CompactionExecutor, planner::CompactionPlanner, trigger::TriggerRequest,
    },
    db::DB,
    mode::Mode,
};

/// Shared state for the lightweight compaction trigger handler.
pub struct CompactionHttpState<M, E, CE, P>
where
    M: Mode,
    E: fusio::executor::Executor + fusio::executor::Timer + Send + Sync,
{
    /// Database handle used to schedule compaction work.
    pub db: Arc<DB<M, E>>,
    /// Planner selecting the next compaction task.
    pub planner: P,
    /// Executor that runs the chosen compaction job.
    pub executor: CE,
}

#[derive(Debug, Deserialize, Serialize)]
/// Request body accepted by the compaction trigger endpoint.
pub struct TriggerPayload {
    /// Optional owner label that will be attached to the trigger request.
    pub owner: Option<String>,
    /// Idempotency key to suppress duplicate triggers within the TTL window.
    pub idempotency_key: Option<String>,
    /// Lease TTL for the trigger, in milliseconds.
    pub lease_ms: Option<u64>,
}

/// Stateless trigger helper that can be wrapped by an HTTP handler.
pub async fn handle_trigger<M, E, CE, P>(
    state: &CompactionHttpState<M, E, CE, P>,
    payload: TriggerPayload,
) -> StatusCode
where
    M: Mode,
    M::Key: Eq + Clone,
    E: fusio::executor::Executor + fusio::executor::Timer,
    CE: CompactionExecutor,
    P: CompactionPlanner,
{
    let lease_ttl = payload
        .lease_ms
        .map(std::time::Duration::from_millis)
        .unwrap_or_else(|| std::time::Duration::from_secs(30));
    let request = TriggerRequest {
        owner: payload.owner.unwrap_or_else(|| "http-trigger".to_string()),
        idempotency_key: payload.idempotency_key,
        lease_ttl,
    };
    match state
        .db
        .trigger_compaction_once(&state.planner, &state.executor, request, None)
        .await
    {
        Ok(Some(_)) => StatusCode::ACCEPTED,
        Ok(None) => StatusCode::OK,
        Err(err) => {
            eprintln!("compaction trigger failed: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
