//! MVCC core utilities (timestamps, read views, and commit clock helpers).

use std::fmt;

/// Logical commit timestamp assigned to mutations and read views.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Least possible timestamp (used for uninitialised clocks).
    pub const MIN: Self = Self(0);
    /// Greatest possible timestamp (used for open-ended visibility).
    pub const MAX: Self = Self(u64::MAX);

    /// Construct a timestamp from a raw `u64`.
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw `u64` value backing this timestamp.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next timestamp after `self`, saturating on overflow.
    #[inline]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Add `delta` while saturating on overflow.
    #[inline]
    pub const fn saturating_add(self, delta: u64) -> Self {
        Self(self.0.saturating_add(delta))
    }
}

impl From<u64> for Timestamp {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for u64 {
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Timestamp").field(&self.0).finish()
    }
}

/// Tracks the next commit timestamp to allocate.
#[derive(Debug, Clone, Copy)]
pub struct CommitClock {
    next: Timestamp,
}

impl CommitClock {
    /// Create a new clock that will hand out timestamps starting from `start`.
    #[inline]
    pub const fn new(start: Timestamp) -> Self {
        Self { next: start }
    }

    /// Allocate and return the next commit timestamp.
    #[inline]
    pub fn next(&mut self) -> Timestamp {
        let current = self.next;
        self.next = current.next();
        current
    }

    /// Return the timestamp that will be handed out next.
    #[inline]
    pub const fn peek(&self) -> Timestamp {
        self.next
    }

    /// Advance the clock so that it will hand out at least `candidate`.
    ///
    /// Useful after recovery where the highest observed commit is already known.
    #[inline]
    pub fn advance_to_at_least(&mut self, candidate: Timestamp) {
        if candidate > self.next {
            self.next = candidate;
        }
    }
}

impl Default for CommitClock {
    fn default() -> Self {
        Self::new(Timestamp::MIN)
    }
}

/// Immutable view acquired by readers to evaluate MVCC visibility.
#[derive(Debug, Clone, Copy)]
pub struct ReadView {
    read_ts: Timestamp,
}

impl ReadView {
    /// Build a read view pinned at `read_ts`.
    #[inline]
    pub const fn new(read_ts: Timestamp) -> Self {
        Self { read_ts }
    }

    /// Commit timestamp visible to the view (inclusive).
    #[inline]
    pub const fn read_ts(&self) -> Timestamp {
        self.read_ts
    }
}
