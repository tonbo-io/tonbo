use std::{collections::BTreeMap, io, sync::Arc};

use crate::{executor::Executor, oracle::Timestamp, Record, DB};

pub struct Transaction<R, E>
where
    R: Record,
    E: Executor,
{
    db: Arc<DB<R, E>>,
    read_at: Timestamp,
    local: BTreeMap<R::Key, Option<R>>,
}

impl<R, E> Transaction<R, E>
where
    R: Record,
    E: Executor,
{
    pub(crate) fn new(db: Arc<DB<R, E>>, read_at: Timestamp) -> Self {
        Self {
            db,
            read_at,
            local: BTreeMap::new(),
        }
    }

    pub async fn get(&self, key: &R::Key) -> io::Result<Option<&R>> {
        // match self.local.get(key).and_then(|v| v.as_ref()) {
        //     Some(v) => Ok(Some(v)),
        //     None => self.db.get(key, self.read_at).await,
        // }
        todo!()
    }
}
