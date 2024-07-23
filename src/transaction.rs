use std::{
    collections::{btree_map::Entry, BTreeMap},
    io,
    mem::transmute,
};

use async_lock::RwLockReadGuard;
use lockable::SyncLimit;
use parquet::{arrow::ProjectionMask, errors::ParquetError};
use thiserror::Error;

use crate::{
    fs::FileProvider,
    record::KeyRef,
    stream,
    timestamp::Timestamp,
    version::{set::transaction_ts, VersionRef},
    LockMap, Record, Schema,
};

pub struct Transaction<'txn, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    ts: Timestamp,
    local: BTreeMap<R::Key, Option<R>>,
    share: RwLockReadGuard<'txn, Schema<R, FP>>,
    version: VersionRef<R, FP>,
    lock_map: LockMap<R::Key>,
}

impl<'txn, R, FP> Transaction<'txn, R, FP>
where
    R: Record + Send,
    FP: FileProvider,
{
    pub(crate) fn new(
        version: VersionRef<R, FP>,
        share: RwLockReadGuard<'txn, Schema<R, FP>>,
        lock_map: LockMap<R::Key>,
    ) -> Self {
        Self {
            ts: transaction_ts(),
            local: BTreeMap::new(),
            share,
            version,
            lock_map,
        }
    }

    pub async fn get<'get>(
        &'get self,
        key: &'get R::Key,
        projection_mask: ProjectionMask,
    ) -> Result<Option<TransactionEntry<'get, R>>, ParquetError> {
        Ok(match self.local.get(key).and_then(|v| v.as_ref()) {
            Some(v) => Some(TransactionEntry::Local(v.as_record_ref())),
            None => self
                .share
                .get(key, self.ts, projection_mask)
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
        let mut _key_guards = Vec::new();

        for (key, _) in self.local.iter() {
            // SAFETY: Error is Never
            _key_guards.push(
                self.lock_map
                    .blocking_lock(key.clone(), SyncLimit::no_limit())
                    .unwrap(),
            );
        }
        for (key, _) in self.local.iter() {
            if self.share.check_conflict(key, self.ts) {
                return Err(CommitError::WriteConflict(key.clone()));
            }
        }
        for (key, record) in self.local {
            let new_ts = transaction_ts();
            match record {
                Some(record) => self.share.write(record, new_ts).await?,
                None => self.share.remove(key, new_ts).await?,
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
    #[error("transaction io error {:?}", .0)]
    Io(#[from] io::Error),
    #[error("transaction parquet error {:?}", .0)]
    Parquet(#[from] ParquetError),
    #[error("transaction write conflict: {:?}", .0)]
    WriteConflict(R::Key),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::arrow::{arrow_to_parquet_schema, ProjectionMask};
    use tempfile::TempDir;

    use crate::{
        executor::tokio::TokioExecutor, record::Record, tests::Test, transaction::CommitError,
        DbOption, DB,
    };

    #[tokio::test]
    async fn transaction_read_write() {
        let temp_dir = TempDir::new().unwrap();

        let db = DB::<String, TokioExecutor>::new(
            Arc::new(DbOption::new(temp_dir.path())),
            TokioExecutor::new(),
        )
        .await
        .unwrap();
        {
            let mut txn1 = db.transaction().await;
            txn1.set("foo".to_string());

            let txn2 = db.transaction().await;
            dbg!(txn2
                .get(&"foo".to_string(), ProjectionMask::all())
                .await
                .unwrap()
                .is_none());

            txn1.commit().await.unwrap();
            txn2.commit().await.unwrap();
        }

        {
            let txn3 = db.transaction().await;
            dbg!(txn3
                .get(&"foo".to_string(), ProjectionMask::all())
                .await
                .unwrap()
                .is_none());
            txn3.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn write_conflicts() {
        let temp_dir = TempDir::new().unwrap();

        let db = DB::<String, TokioExecutor>::new(
            Arc::new(DbOption::new(temp_dir.path())),
            TokioExecutor::new(),
        )
        .await
        .unwrap();

        let mut txn = db.transaction().await;
        txn.set(0.to_string());
        txn.set(1.to_string());
        txn.commit().await.unwrap();

        let mut txn_0 = db.transaction().await;
        let mut txn_1 = db.transaction().await;
        let mut txn_2 = db.transaction().await;

        txn_0.set(1.to_string());
        txn_1.set(1.to_string());
        txn_1.set(2.to_string());
        txn_2.set(2.to_string());

        txn_0.commit().await.unwrap();

        if let Err(CommitError::WriteConflict(conflict_key)) = txn_1.commit().await {
            assert_eq!(conflict_key, 1.to_string());
            txn_2.commit().await.unwrap();
            return;
        }
        unreachable!();
    }

    #[tokio::test]
    async fn transaction_projection() {
        let temp_dir = TempDir::new().unwrap();

        let db = DB::<Test, TokioExecutor>::new(
            Arc::new(DbOption::new(temp_dir.path())),
            TokioExecutor::new(),
        )
        .await
        .unwrap();

        let mut txn1 = db.transaction().await;
        txn1.set(Test {
            vstring: 0.to_string(),
            vu32: 0,
            vobool: Some(true),
        });

        let key = 0.to_string();
        let entry = txn1
            .get(
                &key,
                ProjectionMask::roots(
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2],
                ),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(entry.get().vstring, 0.to_string());
        assert_eq!(entry.get().vu32, Some(0));
        assert_eq!(entry.get().vbool, Some(true));
        drop(entry);

        txn1.commit().await.unwrap();
    }
}
