use std::{
    collections::{btree_map::Entry, BTreeMap},
    io,
    mem::transmute,
};

use async_lock::RwLockReadGuard;
use parquet::errors::ParquetError;
use thiserror::Error;

use crate::{
    fs::FileProvider,
    oracle::{Oracle, Timestamp, WriteConflict},
    record::KeyRef,
    stream, Record, Schema,
};

pub struct Transaction<'txn, R, FP>
where
    R: Record,
{
    read_at: Timestamp,
    local: BTreeMap<R::Key, Option<R>>,
    share: RwLockReadGuard<'txn, Schema<R, FP>>,
    oracle: &'txn Oracle<R::Key>,
}

impl<'txn, R, FP> Transaction<'txn, R, FP>
where
    R: Record + Send,
    FP: FileProvider,
{
    pub(crate) fn new(
        oracle: &'txn Oracle<R::Key>,
        share: RwLockReadGuard<'txn, Schema<R, FP>>,
    ) -> Self {
        Self {
            read_at: oracle.start_read(),
            local: BTreeMap::new(),
            share,
            oracle,
        }
    }

    pub async fn get<'get>(
        &'get self,
        key: &'get R::Key,
    ) -> Result<Option<TransactionEntry<'get, R>>, ParquetError> {
        Ok(match self.local.get(key).and_then(|v| v.as_ref()) {
            Some(v) => Some(TransactionEntry::Local(v.as_record_ref())),
            None => self
                .share
                .get(key, self.read_at)
                .await?
                .map(TransactionEntry::Stream),
        })
    }

    pub fn set(&mut self, value: R) {
        self.entry(value.key().to_key(), Some(value))
    }

    pub fn remove(&mut self, key: R::Key) {
        self.entry(key, None)
    }

    fn entry(&mut self, key: R::Key, value: Option<R>) {
        match self.local.entry(key) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(mut o) => *o.get_mut() = value,
        }
    }

    pub async fn commit(self) -> Result<(), CommitError<R>> {
        self.oracle.read_commit(self.read_at);
        if self.local.is_empty() {
            return Ok(());
        }
        let write_at = self.oracle.start_write();
        self.oracle
            .write_commit(self.read_at, write_at, self.local.keys().cloned().collect())?;

        for (key, record) in self.local {
            match record {
                Some(record) => self.share.write(record, write_at).await?,
                None => self.share.remove(key, write_at).await?,
            }
        }
        Ok(())
    }
}

pub enum TransactionEntry<'entry, R>
where
    R: Record,
{
    Stream(stream::Entry<'entry, R>),
    Local(R::Ref<'entry>),
}

impl<'entry, R> TransactionEntry<'entry, R>
where
    R: Record,
{
    pub fn get(&self) -> R::Ref<'_> {
        match self {
            TransactionEntry::Stream(entry) => entry.value(),
            TransactionEntry::Local(value) => {
                // Safety: shorter lifetime must be safe
                unsafe { transmute::<R::Ref<'entry>, R::Ref<'_>>(*value) }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum CommitError<R>
where
    R: Record,
{
    #[error("commit transaction error {:?}", .0)]
    Io(#[from] io::Error),
    #[error(transparent)]
    WriteConflict(#[from] WriteConflict<R::Key>),
}

#[cfg(test)]
mod tests {
    use crate::{executor::tokio::TokioExecutor, DB};

    #[tokio::test]
    async fn transaction_read_write() {
        let db = DB::<String, TokioExecutor>::default();
        {
            let mut txn1 = db.transaction().await;
            txn1.set("foo".to_string());

            let txn2 = db.transaction().await;
            dbg!(txn2.get(&"foo".to_string()).await.unwrap().is_none());

            txn1.commit().await.unwrap();
            txn2.commit().await.unwrap();
        }

        {
            let txn3 = db.transaction().await;
            dbg!(txn3.get(&"foo".to_string()).await.unwrap().is_none());
            txn3.commit().await.unwrap();
        }
    }
}
