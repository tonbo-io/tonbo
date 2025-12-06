//! Persistence helpers for `wal/state.json`.

use std::{fmt, sync::Arc};

use fusio::{
    Error as FusioError,
    dynamic::{MaybeSend, MaybeSync},
    fs::{CasCondition, FsCas},
    path::{Path, PathPart},
};
#[cfg(not(target_arch = "wasm32"))]
use futures::future::BoxFuture;
#[cfg(target_arch = "wasm32")]
use futures::future::LocalBoxFuture as BoxFuture;
use serde::{Deserialize, Serialize};

use super::{WalError, WalResult};
use crate::{id::FileId, mvcc::Timestamp};

const STATE_FILE_NAME: &str = "state.json";
type WalStateLoadFuture<'a> = BoxFuture<'a, WalResult<Option<(Vec<u8>, String)>>>;
type WalStatePutFuture<'a> = BoxFuture<'a, WalResult<String>>;

/// Serialized WAL metadata persisted alongside segments in `wal/state.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct WalState {
    /// Highest fully sealed segment sequence.
    pub last_segment_seq: Option<u64>,
    /// Highest frame sequence persisted to disk.
    pub last_frame_seq: Option<u64>,
    /// Highest commit timestamp made durable.
    pub last_commit_ts: Option<u64>,
    /// Metadata for all sealed segments that still exist on disk.
    pub sealed_segments: Vec<WalSegmentBounds>,
    /// Metadata for the currently active segment (if it already contains frames).
    pub active_segment: Option<WalSegmentBounds>,
}

impl WalState {
    /// Returns the highest durable commit timestamp, if known.
    #[inline]
    pub(crate) fn commit_ts(&self) -> Option<Timestamp> {
        self.last_commit_ts.map(Timestamp::new)
    }

    /// Record the latest durable commit timestamp.
    #[inline]
    pub(crate) fn set_commit_ts(&mut self, ts: Timestamp) {
        self.last_commit_ts = Some(ts.get());
    }

    /// Update the tracked frame sequence.
    #[inline]
    pub fn set_frame_seq(&mut self, seq: u64) {
        self.last_frame_seq = Some(seq);
    }

    /// Update the tracked sealed segment sequence.
    #[inline]
    pub fn set_segment_seq(&mut self, seq: u64) {
        self.last_segment_seq = Some(seq);
    }

    /// Replace the sealed segment list with `segments`.
    pub fn replace_sealed_segments(&mut self, segments: Vec<WalSegmentBounds>) {
        self.sealed_segments = segments;
    }

    /// Insert or update metadata for a sealed segment.
    pub fn upsert_sealed_segment(&mut self, segment: WalSegmentBounds) {
        self.sealed_segments
            .retain(|existing| existing.seq != segment.seq);
        self.sealed_segments.push(segment);
    }

    /// Retain only those sealed segments that satisfy `predicate`.
    pub fn retain_sealed_segments<F>(&mut self, mut predicate: F)
    where
        F: FnMut(&WalSegmentBounds) -> bool,
    {
        self.sealed_segments.retain(|segment| predicate(segment));
    }

    /// Return metadata for all sealed segments.
    pub fn sealed_segments(&self) -> &[WalSegmentBounds] {
        &self.sealed_segments
    }

    /// Update metadata for the active segment.
    pub fn set_active_segment(&mut self, bounds: WalSegmentBounds) {
        self.active_segment = Some(bounds);
    }

    /// Clear the active segment metadata.
    pub fn clear_active_segment(&mut self) {
        self.active_segment = None;
    }

    /// Access the active segment metadata.
    pub fn active_segment(&self) -> Option<&WalSegmentBounds> {
        self.active_segment.as_ref()
    }
}

/// Handle for loading and persisting [`WalState`] using an injected store.
#[derive(Clone)]
pub struct WalStateHandle {
    store: Arc<dyn WalStateStore>,
    path: Path,
    tag: Option<String>,
    state: WalState,
}

impl fmt::Debug for WalStateHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalStateHandle")
            .field("path", &self.path)
            .field("tag_present", &self.tag.is_some())
            .field("state", &self.state)
            .finish()
    }
}

impl WalStateHandle {
    /// Load the state file within `dir`, returning a new handle.
    pub async fn load(store: Arc<dyn WalStateStore>, dir: &Path) -> WalResult<Self> {
        let path = state_path(dir)?;
        let (state, tag) = match store.load(&path).await? {
            Some((bytes, tag)) if !bytes.is_empty() => {
                let state = serde_json::from_slice(&bytes)
                    .map_err(|err| WalError::State(format!("decode wal state json: {err}")))?;
                (state, Some(tag))
            }
            Some((_bytes, tag)) => (WalState::default(), Some(tag)),
            None => (WalState::default(), None),
        };

        Ok(Self {
            store,
            path,
            tag,
            state,
        })
    }

    /// Access the current state snapshot.
    #[inline]
    pub fn state(&self) -> &WalState {
        &self.state
    }

    /// Mutably access the state snapshot.
    #[inline]
    pub fn state_mut(&mut self) -> &mut WalState {
        &mut self.state
    }

    /// Return the sealed segment metadata snapshot.
    pub fn sealed_segments(&self) -> &[WalSegmentBounds] {
        self.state.sealed_segments()
    }

    /// Return the active segment metadata snapshot, if any.
    pub fn active_segment(&self) -> Option<&WalSegmentBounds> {
        self.state.active_segment()
    }

    /// Persist the in-memory state back to `state.json` using CAS semantics.
    pub async fn persist(&mut self) -> WalResult<()> {
        let payload = serde_json::to_vec(&self.state)
            .map_err(|err| WalError::State(format!("encode wal state json: {err}")))?;
        let new_tag = self
            .store
            .put(&self.path, &payload, self.tag.as_deref())
            .await?;
        self.tag = Some(new_tag);
        Ok(())
    }
}

/// Inclusive frame bounds tracked for each WAL segment on disk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalSegmentBounds {
    /// Sequence embedded in the WAL filename.
    pub seq: u64,
    /// Deterministic file identifier derived from `seq`.
    pub file_id: FileId,
    /// First frame sequence stored in the segment.
    pub first_frame: u64,
    /// Last frame sequence stored in the segment.
    pub last_frame: u64,
}

impl WalSegmentBounds {
    /// Construct a new set of bounds for the provided segment sequence.
    pub fn new(seq: u64, file_id: FileId, first_frame: u64, last_frame: u64) -> Self {
        Self {
            seq,
            file_id,
            first_frame,
            last_frame,
        }
    }

    /// Extend the tracked frame range to include `last_frame`.
    pub fn extend_to(&mut self, last_frame: u64) {
        self.last_frame = last_frame;
    }
}

fn state_path(dir: &Path) -> WalResult<Path> {
    let part = PathPart::parse(STATE_FILE_NAME)
        .map_err(|err| WalError::State(format!("invalid state path component: {err}")))?;
    Ok(dir.child(part))
}

/// Storage interface used by [`WalStateHandle`].
pub trait WalStateStore: MaybeSend + MaybeSync {
    /// Load the payload and tag associated with the state file.
    fn load<'a>(&'a self, path: &'a Path) -> WalStateLoadFuture<'a>;

    /// Write a new payload conditionally and return the new tag.
    fn put<'a>(
        &'a self,
        path: &'a Path,
        payload: &'a [u8],
        expect: Option<&'a str>,
    ) -> WalStatePutFuture<'a>;
}

/// [`WalStateStore`] implementation backed by an `FsCas` filesystem.
#[derive(Clone)]
pub struct FsWalStateStore {
    cas: Arc<dyn FsCas>,
}

impl FsWalStateStore {
    /// Construct a new CAS-backed store.
    pub fn new(cas: Arc<dyn FsCas>) -> Self {
        Self { cas }
    }
}

impl WalStateStore for FsWalStateStore {
    fn load<'a>(&'a self, path: &'a Path) -> WalStateLoadFuture<'a> {
        Box::pin(async move {
            match self
                .cas
                .load_with_tag(path)
                .await
                .map_err(|err| map_fusio_err("load wal state", err))?
            {
                Some((bytes, tag)) => Ok(Some((bytes, tag))),
                None => Ok(None),
            }
        })
    }

    fn put<'a>(
        &'a self,
        path: &'a Path,
        payload: &'a [u8],
        expect: Option<&'a str>,
    ) -> WalStatePutFuture<'a> {
        Box::pin(async move {
            let condition = match expect {
                Some(tag) => CasCondition::IfMatch(tag.to_string()),
                None => CasCondition::IfNotExists,
            };
            self.cas
                .put_conditional(path, payload, Some("application/json"), None, condition)
                .await
                .map_err(|err| map_fusio_err("persist wal state", err))
        })
    }
}

fn map_fusio_err(action: &str, err: FusioError) -> WalError {
    WalError::State(format!("failed to {action}: {err}"))
}
