use std::sync::{atomic::AtomicU32, Arc};

use tonbo::record::{Record, Schema};
use ulid::Ulid;

pub(crate) const MAX_LEVEL: usize = 7;
pub type FileId = Ulid;

#[allow(unused)]
struct Metadata<R>
where
    R: Record,
{
    // Latest committed timestamp for S3 metadata
    latest: Arc<AtomicU32>,
    level_slice: [Vec<Scope<<R::Schema as Schema>::Key>>; MAX_LEVEL],
}

impl<R> Metadata<R>
where
    R: Record,
{
    #[allow(unused)]
    pub fn new(timestamp: Arc<AtomicU32>) -> Self {
        Metadata {
            latest: timestamp,
            level_slice: [const { vec![] }; MAX_LEVEL],
        }
    }
}

// Scope used for TonboCloud. FileID
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Scope<K> {
    pub(crate) min: K,
    pub(crate) max: K,
    pub(crate) gen: FileId,
    pub(crate) file_size: u64,
}

#[allow(unused)]
enum MetadataChanges {}
