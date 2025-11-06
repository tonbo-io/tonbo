use std::sync::Mutex;

use ulid::{Generator, Ulid};

/// Identifier used for files and other persisted artifacts.
pub type FileId = Ulid;

/// Thread-safe ULID generator scoped to a single database instance.
pub struct FileIdGenerator {
    inner: Mutex<Generator>,
}

impl FileIdGenerator {
    /// Create a new generator seeded with the current time.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Generator::new()),
        }
    }

    /// Produce the next [`FileId`] in a monotonic, time-ordered sequence.
    pub fn generate(&self) -> FileId {
        let mut guard = self
            .inner
            .lock()
            .expect("file id generator mutex should not be poisoned");
        guard
            .generate()
            .expect("file id generator should advance without error")
    }
}

impl Default for FileIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}
