use std::{marker::PhantomPinned, pin::Pin};

use arrow_array::RecordBatch;

use crate::key::{KeyView, KeyViewRaw};

/// Record batch held in a pinned box so raw key views can borrow its buffers safely.
///
/// The struct itself carries a `PhantomPinned` marker so callers cannot move it after creation.
/// Arrow buffers referenced by `KeyViewRaw` remain valid as long as the originating batch stays
/// alive, so we only need to ensure the batch outlives any derived `KeyView`.
#[allow(dead_code)]
pub struct PinnedBatch {
    batch: RecordBatch,
    _pin: PhantomPinned,
}

impl PinnedBatch {
    /// Pin a record batch on the heap and return the pinned handle.
    #[allow(dead_code)]
    pub fn new(batch: RecordBatch) -> Pin<Box<Self>> {
        Box::pin(Self {
            batch,
            _pin: PhantomPinned,
        })
    }

    /// Access the underlying record batch.
    #[allow(dead_code)]
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// # Safety
    ///
    /// Callers must guarantee that `raw` only contains pointers into this batch's buffers.
    /// The returned `KeyView<'_>` borrows the batch for its lifetime.
    #[allow(dead_code)]
    pub unsafe fn view_from_raw<'batch>(&'batch self, raw: KeyViewRaw) -> KeyView<'batch> {
        KeyView::from_raw(raw)
    }
}
