use std::{collections::Bound, sync::Arc};

use async_lock::RwLockReadGuard;
use parquet::arrow::ProjectionMask;

use crate::{
    fs::manager::StoreManager,
    record::Record,
    stream,
    stream::ScanStream,
    timestamp::Timestamp,
    version::{TransactionTs, VersionRef},
    DbError, ParquetLru, Projection, Scan, Schema,
};

pub struct Snapshot<'s, R>
where
    R: Record,
{
    ts: Timestamp,
    share: RwLockReadGuard<'s, Schema<R>>,
    version: VersionRef<R>,
    manager: Arc<StoreManager>,
    parquet_lru: ParquetLru,
}

impl<'s, R> Snapshot<'s, R>
where
    R: Record,
{
    pub async fn get<'get>(
        &'get self,
        key: &'get R::Key,
        projection: Projection,
    ) -> Result<Option<stream::Entry<'get, R>>, DbError<R>> {
        Ok(self
            .share
            .get(
                &self.version,
                &self.manager,
                key,
                self.ts,
                projection,
                self.parquet_lru.clone(),
            )
            .await?
            .and_then(|entry| {
                if entry.value().is_none() {
                    None
                } else {
                    Some(entry)
                }
            }))
    }

    pub fn scan<'scan, 'range>(
        &'scan self,
        range: (Bound<&'range R::Key>, Bound<&'range R::Key>),
    ) -> Scan<'scan, 'range, R> {
        Scan::new(
            &self.share,
            &self.manager,
            range,
            self.ts,
            &self.version,
            Box::new(move |_: Option<ProjectionMask>| None),
            self.parquet_lru.clone(),
        )
    }

    pub(crate) fn new(
        share: RwLockReadGuard<'s, Schema<R>>,
        version: VersionRef<R>,
        manager: Arc<StoreManager>,
        parquet_lru: ParquetLru,
    ) -> Self {
        Self {
            ts: version.load_ts(),
            share,
            version,
            manager,
            parquet_lru,
        }
    }

    pub(crate) fn ts(&self) -> Timestamp {
        self.ts
    }

    pub(crate) fn increase_ts(&self) -> Timestamp {
        self.version.increase_ts()
    }

    pub(crate) fn schema(&self) -> &Schema<R> {
        &self.share
    }

    pub(crate) fn _scan<'scan, 'range>(
        &'scan self,
        range: (Bound<&'range R::Key>, Bound<&'range R::Key>),
        fn_pre_stream: Box<
            dyn FnOnce(Option<ProjectionMask>) -> Option<ScanStream<'scan, R>> + Send + 'scan,
        >,
    ) -> Scan<'scan, 'range, R> {
        Scan::new(
            &self.share,
            &self.manager,
            range,
            self.ts,
            &self.version,
            fn_pre_stream,
            self.parquet_lru.clone(),
        )
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::{collections::Bound, sync::Arc};

    use fusio::path::Path;
    use fusio_dispatch::FsOptions;
    use futures_util::StreamExt;
    use tempfile::TempDir;

    use crate::{
        compaction::tests::build_version,
        executor::tokio::TokioExecutor,
        fs::manager::StoreManager,
        tests::{build_db, build_schema},
        version::TransactionTs,
        DbOption,
    };

    #[tokio::test]
    async fn snapshot_scan() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(StoreManager::new(FsOptions::Local, vec![]).unwrap());
        let option = Arc::new(DbOption::from(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
        ));

        manager
            .base_fs()
            .create_dir_all(&option.version_log_dir_path())
            .await
            .unwrap();
        manager
            .base_fs()
            .create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let (_, version) = build_version(&option, &manager).await;
        let (schema, compaction_rx) = build_schema(option.clone(), manager.base_fs())
            .await
            .unwrap();
        let db = build_db(
            option,
            compaction_rx,
            TokioExecutor::new(),
            schema,
            version,
            manager,
        )
        .await
        .unwrap();

        {
            // to increase timestamps to 1 because the data ts built in advance is 1
            db.version_set.increase_ts();
        }
        let snapshot = db.snapshot().await;

        let mut stream = snapshot
            .scan((Bound::Unbounded, Bound::Unbounded))
            .projection(vec![1])
            .take()
            .await
            .unwrap();

        let entry_0 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_0.key().value, "1");
        assert!(entry_0.value().unwrap().vbool.is_none());
        let entry_1 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_1.key().value, "2");
        assert!(entry_1.value().unwrap().vbool.is_none());
        let entry_2 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_2.key().value, "3");
        assert!(entry_2.value().unwrap().vbool.is_none());
        let entry_3 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_3.key().value, "4");
        assert!(entry_3.value().unwrap().vbool.is_none());
        let entry_4 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_4.key().value, "5");
        assert!(entry_4.value().unwrap().vbool.is_none());
        let entry_5 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_5.key().value, "6");
        assert!(entry_5.value().unwrap().vbool.is_none());
        let entry_6 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_6.key().value, "7");
        assert!(entry_6.value().unwrap().vbool.is_none());
        let entry_7 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_7.key().value, "8");
        assert!(entry_7.value().unwrap().vbool.is_none());
        let entry_8 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_8.key().value, "9");
        assert!(entry_8.value().unwrap().vbool.is_none());
        let entry_9 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_9.key().value, "alice");
        let entry_10 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_10.key().value, "ben");
        let entry_11 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_11.key().value, "carl");
        let entry_12 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_12.key().value, "dice");
        let entry_13 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_13.key().value, "erika");
        let entry_14 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_14.key().value, "funk");
    }
}
