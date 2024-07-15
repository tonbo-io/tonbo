use std::{collections::BTreeMap, io, sync::Arc};

use crate::{oracle::Timestamp, Record, DB};

pub struct Transaction<R>
where
    R: Record,
{
    db: Arc<DB<R>>,
    read_at: Timestamp,
    local: BTreeMap<R::Key, Option<R>>,
}

impl<R> Transaction<R>
where
    R: Record,
{
    pub(crate) fn new(db: Arc<DB<R>>, read_at: Timestamp) -> Self {
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
