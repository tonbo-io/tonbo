use std::marker::PhantomData;

use async_trait::async_trait;
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Transaction};

use crate::{
    manifest::{ManifestStorage, ManifestStorageError},
    ondisk::sstable::SsTableID,
    record::{Record, Schema},
    version::{edit::VersionEdit, timestamp::Timestamp, TransactionTs, VersionRef},
};

pub(crate) struct MetadataManifestStorage<R>
where
    R: Record,
{
    _marker: PhantomData<R>,
    pool: PgPool,
}

impl<R> MetadataManifestStorage<R>
where
    R: Record,
{
    pub async fn new(conn_url: impl Into<String>) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&conn_url.into())
            .await?;
        
        Ok(Self {
            pool,
            _marker: PhantomData,
        })
    }
    
    /// Execute a database transaction with the provided closure.
    /// The transaction will be automatically committed on success or rolled back on error.
    pub async fn execute_transaction<F, Fut, T>(&self, f: F) -> Result<T, sqlx::Error>
    where
        F: FnOnce(Transaction<'_, Postgres>) -> Fut,
        Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
    {
        let mut tx = self.pool.begin().await?;
        
        match f(tx).await {
            Ok(result) => Ok(result),
            Err(e) => Err(e),
        }
    }
    
    /// Get a reference to the underlying connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl<R> TransactionTs for MetadataManifestStorage<R>
where
    R: Record,
{
    fn load_ts(&self) -> Timestamp {
        todo!()
    }

    fn increase_ts(&self) -> Timestamp {
        todo!()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl<R> ManifestStorage<R> for MetadataManifestStorage<R>
where
    R: Record,
{
    /// Return reference to the current version of the LSM-tree.
    async fn current(&self) -> VersionRef<R> {
        todo!()
    }

    /// Recover manifest state while applying version edits. Use to rebuild
    /// the in-memory manifest state from persisted log.
    async fn recover(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
    ) -> Result<(), ManifestStorageError> {
        todo!()
    }

    /// Apply version edits and update the manifest.
    async fn update(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
    ) -> Result<(), ManifestStorageError> {
        todo!()
    }

    /// Perform a rewrite of manifest log. Can be used to perform log
    /// compaction.
    async fn rewrite(&self) -> Result<(), ManifestStorageError> {
        todo!()
    }

    /// Completely destroy all manifest data.
    async fn destroy(&mut self) -> Result<(), ManifestStorageError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;
    use postgresql_embedded::{PostgreSQL, Settings};
    use tonbo_macros::Record;

    #[derive(Record, Debug, PartialEq, Eq, Clone)]
    pub struct TestRecord {
        #[record(primary_key)]
        pub vstring: String,
        pub vu32: u32,
        pub vbool: Option<bool>,
    }

    #[tokio::test]
    async fn test_metadata_manifest_storage_transaction() {
        // Create an embedded PostgreSQL instance
        let settings = Settings::default()
            .temporary(true)
            .installation_dir(tempfile::tempdir().unwrap().path())
            .password("test_password")
            .username("test_user");
        
        let mut postgresql = PostgreSQL::new(settings);
        postgresql.setup().await.expect("Failed to setup PostgreSQL");
        postgresql.start().await.expect("Failed to start PostgreSQL");
        
        // Get the connection URL from the embedded instance
        let conn_url = postgresql.database_url("test_db");
        
        // Create the database
        postgresql.create_database("test_db").await.expect("Failed to create database");
        
        // Create the storage instance
        let storage = MetadataManifestStorage::<TestRecord>::new(&conn_url)
            .await
            .expect("Failed to create storage instance");

        // Create a test table for the transaction test
        let create_table_result = storage.execute_transaction(|tx| async move {
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS test_transactions (
                    id SERIAL PRIMARY KEY,
                    value TEXT NOT NULL
                )"
            )
            .execute(&mut *tx)
            .await?;
            Ok(())
        }).await;

        assert!(create_table_result.is_ok(), "Failed to create test table");

        // Test successful transaction
        let insert_result = storage.execute_transaction(|tx| async move {
            let result = sqlx::query("INSERT INTO test_transactions (value) VALUES ($1) RETURNING id")
                .bind("test_value")
                .fetch_one(&mut *tx)
                .await?;
            
            let id: i32 = result.get(0);
            Ok(id)
        }).await;

        assert!(insert_result.is_ok(), "Failed to insert test data");
        let inserted_id = insert_result.unwrap();
        assert!(inserted_id > 0, "Invalid inserted ID");

        // Verify the data was committed
        let verify_result = storage.execute_transaction(|tx| async move {
            let result = sqlx::query("SELECT value FROM test_transactions WHERE id = $1")
                .bind(inserted_id)
                .fetch_one(&mut *tx)
                .await?;
            
            let value: String = result.get(0);
            Ok(value)
        }).await;

        assert!(verify_result.is_ok(), "Failed to verify inserted data");
        assert_eq!(verify_result.unwrap(), "test_value");

        // Test transaction rollback on error
        let rollback_result = storage.execute_transaction(|tx| async move {
            sqlx::query("INSERT INTO test_transactions (value) VALUES ($1)")
                .bind("should_rollback")
                .execute(&mut *tx)
                .await?;
            
            // Force an error to trigger rollback
            Err(sqlx::Error::RowNotFound)
        }).await;

        assert!(rollback_result.is_err(), "Transaction should have failed");

        // Verify the failed transaction was rolled back
        let count_result = storage.execute_transaction(|tx| async move {
            let result = sqlx::query("SELECT COUNT(*) FROM test_transactions WHERE value = $1")
                .bind("should_rollback")
                .fetch_one(&mut *tx)
                .await?;
            
            let count: i64 = result.get(0);
            Ok(count)
        }).await;

        assert!(count_result.is_ok(), "Failed to count rows");
        assert_eq!(count_result.unwrap(), 0, "Rollback failed - data was committed");

        // Clean up test table
        let _ = storage.execute_transaction(|tx| async move {
            sqlx::query("DROP TABLE IF EXISTS test_transactions")
                .execute(&mut *tx)
                .await?;
            Ok(())
        }).await;
        
        // Stop the embedded PostgreSQL instance
        postgresql.stop().await.expect("Failed to stop PostgreSQL");
    }
}
