#![allow(unused)]

use std::{
    collections::HashMap,
    io::Cursor,
    marker::PhantomData,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use flume::Sender;
use fusio_log::{Decode, Encode};
use itertools::Itertools;
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, QueryBuilder, Row, Transaction};
use ulid::Ulid;

use crate::{
    fs::FileId,
    manifest::{ManifestStorage, ManifestStorageError},
    metadata::{
        error::MetadataServiceError,
        model::{VersionSnapshot, VersionSnapshotLeveledScope},
    },
    ondisk::sstable::SsTableID,
    record::{Record, Schema},
    scope::Scope,
    version::{
        cleaner::CleanTag, edit::VersionEdit, timestamp::Timestamp, TransactionTs, Version,
        VersionRef, MAX_LEVEL,
    },
    DbOption,
};

pub(crate) struct MetadataManifestStorage<R>
where
    R: Record,
{
    _marker: PhantomData<R>,
    // Postgres connection pool
    pool: PgPool,
    // Channel sender for deleting WAL/SST
    pub clean_sender: Sender<CleanTag>,
    // In memory counter for version change
    pub timestamp: Arc<AtomicU32>,
    pub option: Arc<DbOption>,
}

async fn write_version_snapshot<R: Record>(
    version_snapshot: VersionRef<R>,
    mut tx: Transaction<'static, Postgres>,
) -> Result<(), MetadataServiceError> {
    let version_snapshot_id = sqlx::query(
        r#"
        INSERT INTO version_snapshots (timestamp, log_length) VALUES ($1, $2) RETURNING id
    "#,
    )
    .bind(version_snapshot.ts.inner() as i32)
    .bind(version_snapshot.log_length as i32)
    .fetch_one(&mut *tx)
    .await?
    .get::<i32, _>(0);

    for (row_idx, level) in version_snapshot.level_slice.iter().enumerate() {
        for (col_idx, scope) in level.iter().enumerate() {
            let mut min_key_buf: Vec<u8> = Vec::new();
            let mut min_key_cursor = Cursor::new(&mut min_key_buf);
            scope.min.encode(&mut min_key_cursor).await?;

            let mut max_key_buf: Vec<u8> = Vec::new();
            let mut max_key_cursor = Cursor::new(&mut max_key_buf);
            scope.min.encode(&mut max_key_cursor).await?;

            let scope_id = sqlx::query(
                r#"
                INSERT INTO scopes (file_id, size, min_key, max_key)
                VALUES ($1, $2, $3, $4) RETURNING id
            "#,
            )
            .bind(scope.gen().to_string())
            .bind(scope.file_size as i64)
            .bind(min_key_buf)
            .bind(max_key_buf)
            .fetch_one(&mut *tx)
            .await?
            .get::<i32, _>(0);

            if let Some(wal_files) = &scope.wal_ids {
                let mut query_builder =
                    QueryBuilder::new("INSERT INTO scope_wal_files (file_id, scope_id)");
                query_builder.push_values(wal_files.iter(), |mut b, wal_file| {
                    b.push_bind(wal_file.to_string()).push_bind(scope_id);
                });
                let _ = query_builder.build().execute(&mut *tx).await?;
            }

            sqlx::query(
                r#"
                INSERT INTO version_snapshot_leveled_scopes 
                (version_snapshot_id, row_index, col_index, scope_id)
                VALUES ($1, $2, $3, $4)
            "#,
            )
            .bind(version_snapshot_id)
            .bind(row_idx as i32)
            .bind(col_idx as i32)
            .bind(scope_id)
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await?;

    Ok(())
}

impl<R> MetadataManifestStorage<R>
where
    R: Record,
{
    pub async fn new(
        conn_url: impl Into<String>,
        clean_sender: Sender<CleanTag>,
        option: Arc<DbOption>,
    ) -> Result<Self, MetadataServiceError> {
        let pool = PgPoolOptions::new()
            .idle_timeout(Some(Duration::from_secs(10 * 60)))
            .max_connections(10)
            .connect(&conn_url.into())
            .await?;

        let mut tx = pool.begin().await?;
        let version_snapshot = sqlx::query_as::<_, VersionSnapshot>(
            r#"
                SELECT id, timestamp, log_length
                FROM version_snapshots 
                ORDER BY timestamp DESC 
                LIMIT 1
            "#,
        )
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit();

        if let Some(_version_snapshot) = version_snapshot {
            // There is already a persisted version ref. MetadataManifestStorage is safe to
            // initialize
        } else {
            let new_version = Arc::new(Version::<R>::new_full(
                Timestamp::from(0),
                0,
                [const { Vec::new() }; MAX_LEVEL],
                option.clone(),
                clean_sender.clone(),
                Arc::new(AtomicU32::new(0)),
            ));
            let mut tx = pool.begin().await?;
            write_version_snapshot(new_version, tx).await?;
        }

        Ok(Self {
            pool,
            clean_sender,
            timestamp: Arc::new(AtomicU32::default()),
            option,
            _marker: PhantomData,
        })
    }

    async fn read_current(&self) -> Result<Option<VersionRef<R>>, MetadataServiceError> {
        let mut tx = self.pool().begin().await?;

        let version_snapshot = sqlx::query_as::<_, VersionSnapshot>(
            r#"
                SELECT id, timestamp, log_length
                FROM version_snapshots 
                ORDER BY timestamp DESC 
                LIMIT 1
            "#,
        )
        .fetch_optional(&mut *tx)
        .await?;

        if version_snapshot.is_none() {
            return Ok(None);
        }
        let version_snapshot = version_snapshot.unwrap();

        let version_snapshot_indexed_scopes = sqlx::query_as::<_, VersionSnapshotLeveledScope>(
            r#"
                SELECT 
                    vsls.row_index,
                    vsls.col_index,
                    s.file_id,
                    s.size,
                    s.min_key,
                    s.max_key,
                    s.id as scope_id
                FROM version_snapshot_leveled_scopes vsls
                JOIN scopes s ON vsls.scope_id = s.id
                WHERE vsls.version_snapshot_id = $1
                ORDER BY vsls.row_index, vsls.col_index
            "#,
        )
        .bind(version_snapshot.id)
        .fetch_all(&mut *tx)
        .await?;

        let mut leved_scopes = [const { Vec::new() }; crate::version::MAX_LEVEL];
        for mut indexed_scope in version_snapshot_indexed_scopes {
            let mut min_key_buf = Cursor::new(&mut indexed_scope.min_key);
            let mut max_key_buf = Cursor::new(&mut indexed_scope.max_key);

            let scope_wal_files = sqlx::query(
                r#"
                    SELECT file_id FROM scope_wal_files WHERE scope_id = $1
                "#,
            )
            .bind(indexed_scope.scope_id)
            .fetch_all(&mut *tx)
            .await?
            .iter()
            .map(|pg_row| pg_row.get::<String, _>(0))
            .map(|file_id| Ulid::from_string(&file_id))
            .collect::<Result<Vec<_>, _>>()?;

            let scope = Scope {
                min: <<R::Schema as Schema>::Key>::decode(&mut min_key_buf).await?,
                max: <<R::Schema as Schema>::Key>::decode(&mut max_key_buf).await?,
                gen: Ulid::from_string(&indexed_scope.file_id)?,
                wal_ids: (!scope_wal_files.is_empty()).then_some(scope_wal_files),
                file_size: indexed_scope.size as u64,
            };
            // Query is ordered by col index so vector push results in the right scope order
            leved_scopes[indexed_scope.row_index as usize].push(scope);
        }

        tx.commit().await?;

        Ok(Some(
            Version::new_full(
                Timestamp::from(version_snapshot.timestamp as u32),
                version_snapshot.log_length as u32,
                leved_scopes,
                self.option.clone(),
                self.clean_sender.clone(),
                AtomicU32::new(version_snapshot.timestamp as u32).into(),
            )
            .into(),
        ))
    }

    async fn apply_edits(
        &self,
        mut version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
        is_recover: bool,
    ) -> Result<(), MetadataServiceError> {
        let timestamp = &self.timestamp;
        let mut new_version_snapshot =
            (*self
                .read_current()
                .await?
                .ok_or(MetadataServiceError::Unexpected(
                    "Version snapshot not found".to_owned(),
                ))?)
            .clone();
        let edit_len = new_version_snapshot.log_length + version_edits.len() as u32;

        let mut batch_add: HashMap<u8, Vec<Scope<<R::Schema as Schema>::Key>>> = HashMap::new();
        let mut deleted_wals: Vec<FileId> = vec![];
        let mut deleted_ssts: Vec<SsTableID> = vec![];

        // Non recovery mode will invoke
        if !is_recover {
            version_edits.push(VersionEdit::NewLogLength { len: edit_len });
        }

        for version_edit in version_edits {
            match version_edit {
                VersionEdit::Add { level, mut scope } => {
                    // TODO: remove after apply
                    if let Some(wal_ids) = scope.wal_ids.take() {
                        deleted_wals.extend(wal_ids);
                    }

                    if level == 0 {
                        new_version_snapshot.level_slice[0].push(scope);
                    } else {
                        batch_add.entry(level).or_default().push(scope);
                    }
                }
                VersionEdit::Remove { level, gen } => {
                    if let Some(i) = new_version_snapshot.level_slice[level as usize]
                        .iter()
                        .position(|scope| scope.gen == gen)
                    {
                        new_version_snapshot.level_slice[level as usize].remove(i);
                    }
                    if is_recover {
                        deleted_ssts.push(SsTableID::new(gen, level as usize));
                    }
                }
                VersionEdit::LatestTimeStamp { ts } => {
                    if is_recover {
                        timestamp.store(u32::from(ts), Ordering::Release);
                    }
                    new_version_snapshot.ts = ts;
                }
                VersionEdit::NewLogLength { len } => {
                    new_version_snapshot.log_length = len;
                }
            }
        }

        // Due to many compaction add operations being consecutive, this checks if the
        // SSTs can be splice inserted instead of inserting each one individually
        if !batch_add.is_empty() {
            for (level, mut scopes) in batch_add.into_iter() {
                scopes.sort_unstable_by_key(|scope| scope.min.clone());
                let sort_runs = &mut new_version_snapshot.level_slice[level as usize];

                let merged: Vec<_> = scopes
                    .iter()
                    .cloned()
                    .merge_by(sort_runs.iter().cloned(), |a, b| a.min <= b.min)
                    .collect();
                *sort_runs = merged;
            }
        }

        if let Some(delete_gens) = delete_gens {
            deleted_ssts.extend(delete_gens);
        }

        let mut tx = self.pool().begin().await?;
        write_version_snapshot(new_version_snapshot.into(), tx).await?;

        // TODO @liguoso: Insert into delete wals and ssts

        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl<R> TransactionTs for MetadataManifestStorage<R>
where
    R: Record,
{
    fn load_ts(&self) -> Timestamp {
        self.timestamp.load(Ordering::Acquire).into()
    }

    fn increase_ts(&self) -> Timestamp {
        (self.timestamp.fetch_add(1, Ordering::Release) + 1).into()
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
        self.read_current()
            .await
            .expect("Failed to read VersionRef from MetadataManifestStorage")
            .expect("No VersionRef is found in MetadataManifestStorage")
    }

    /// Recover manifest state while applying version edits. Use to rebuild
    /// the in-memory manifest state from persisted log.
    async fn recover(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
    ) -> Result<(), ManifestStorageError> {
        Ok(self.apply_edits(version_edits, delete_gens, true).await?)
    }

    /// Apply version edits and update the manifest.
    async fn update(
        &self,
        version_edits: Vec<VersionEdit<<R::Schema as Schema>::Key>>,
        delete_gens: Option<Vec<SsTableID>>,
    ) -> Result<(), ManifestStorageError> {
        Ok(self.apply_edits(version_edits, delete_gens, false).await?)
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
    use flume::bounded;
    use fusio_log::{FsOptions, Path};
    use postgresql_embedded::{PostgreSQL, Settings};
    use rand::Rng;
    use rstest::{fixture, rstest};
    use sqlx::Row;
    use tempfile::TempDir;

    use super::*;
    use crate::{fs::generate_file_id, record::test::StringSchema, version::MAX_LEVEL};

    #[fixture]
    async fn local_postgres() -> String {
        let mut settings = Settings::default();
        settings.temporary = true;
        settings.port = rand::rng().random_range(5000..=15000);
        settings.installation_dir = tempfile::tempdir().unwrap().path().to_path_buf();

        let conn_url = settings.url("test_db");

        // Create an embedded PostgreSQL instance
        let mut postgresql = PostgreSQL::new(settings);
        postgresql
            .setup()
            .await
            .expect("Failed to setup PostgreSQL");
        postgresql
            .start()
            .await
            .expect("Failed to start PostgreSQL");
        postgresql
            .create_database("test_db")
            .await
            .expect("Failed to create database");

        conn_url
    }

    #[fixture]
    async fn string_metadata_service(
        #[future] local_postgres: String,
    ) -> MetadataManifestStorage<String> {
        let temp_dir = TempDir::new().unwrap();
        let (sender, _) = bounded(1);
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        ));

        MetadataManifestStorage::<String>::new(
            &local_postgres.await,
            sender.clone(),
            option.clone(),
        )
        .await
        .expect("Failed to create metadata service")
    }

    #[rstest]
    #[tokio::test]
    async fn mds_initialize_will_create_a_new_version(
        #[future] string_metadata_service: MetadataManifestStorage<String>,
    ) {
        let mut settings = Settings::default();
        settings.temporary = true;
        settings.port = rand::rng().random_range(5000..=15000);
        settings.installation_dir = tempfile::tempdir().unwrap().path().to_path_buf();

        let conn_url = settings.url("test_db");
        eprintln!("{}", conn_url);

        // Create an embedded PostgreSQL instance
        let mut postgresql = PostgreSQL::new(settings);
        postgresql
            .setup()
            .await
            .expect("Failed to setup PostgreSQL");
        postgresql
            .start()
            .await
            .expect("Failed to start PostgreSQL");
        postgresql
            .create_database("test_db")
            .await
            .expect("Failed to create database");

        let temp_dir = TempDir::new().unwrap();
        let (sender, _) = bounded(1);
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        ));

        let mds =MetadataManifestStorage::<String>::new(
            &conn_url,
            sender.clone(),
            option.clone(),
        )
        .await
        .expect("Failed to create metadata service");

        let new_version = mds.read_current().await.unwrap().unwrap();
        assert_eq!(new_version.log_length, 0);
        assert_eq!(new_version.ts.inner(), 0);
        assert_eq!(new_version.level_slice[0].len(), 0);

        postgresql.stop().await.expect("Failed to stop PostgreSQL");
    }

    #[tokio::test]
    async fn test_metadata_manifest_storage_transaction() {
        // Create an embedded PostgreSQL instance
        let mut settings = Settings::default();
        settings.temporary = true;
        settings.port = 5433;
        settings.installation_dir = tempfile::tempdir().unwrap().path().to_path_buf();
        let conn_url = settings.url("test_db");
        eprintln!("{}", conn_url);

        // Create the database
        let mut postgresql = PostgreSQL::new(settings);
        postgresql
            .setup()
            .await
            .expect("Failed to setup PostgreSQL");
        postgresql
            .start()
            .await
            .expect("Failed to start PostgreSQL");
        postgresql
            .create_database("test_db")
            .await
            .expect("Failed to create database");

        let temp_dir = TempDir::new().unwrap();
        let (sender, _) = bounded(1);
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        ));

        // Create the storage instance
        let storage =
            MetadataManifestStorage::<String>::new(&conn_url, sender.clone(), option.clone())
                .await
                .expect("Failed to create storage instance");

        // Read and execute bootstrap script
        let bootstrap_sql = std::fs::read_to_string("src/metadata/bootstrap.sql")
            .expect("Failed to read bootstrap.sql");
        let _result = sqlx::raw_sql(&bootstrap_sql)
            .execute(storage.pool())
            .await
            .expect("Failed to execute bootstrap script");

        // Create a test table for the transaction test
        let mut tx = storage.pool.begin().await.unwrap();
        let create_table_result = sqlx::query(
            "CREATE TABLE IF NOT EXISTS test_transactions (
                id SERIAL PRIMARY KEY,
                value TEXT NOT NULL
            )",
        )
        .execute(&mut *tx)
        .await
        .unwrap()
        .rows_affected();
        assert_eq!(create_table_result, 0);

        let insert_result =
            sqlx::query("INSERT INTO test_transactions (value) VALUES ($1) RETURNING id")
                .bind("test_value")
                .fetch_one(&mut *tx)
                .await;
        assert!(insert_result.is_ok(), "Failed to insert test data");
        assert_eq!(insert_result.unwrap().get::<i32, _>(0), 1);

        // Verify the data was committed
        let verify_result = sqlx::query("SELECT value FROM test_transactions WHERE id = $1")
            .bind(1)
            .fetch_one(&mut *tx)
            .await;
        assert!(verify_result.is_ok(), "Failed to verify inserted data");
        assert_eq!(verify_result.unwrap().get::<String, _>(0), "test_value");

        // Clean up test table
        let drop_table_result = sqlx::query("DROP TABLE IF EXISTS test_transactions")
            .execute(&mut *tx)
            .await
            .unwrap()
            .rows_affected();
        assert_eq!(drop_table_result, 0);

        let sstables_exists = sqlx::query("SELECT COUNT(*) FROM sstables")
            .fetch_one(&mut *tx)
            .await;
        assert!(sstables_exists.is_ok(), "sstables table should exist");
        assert_eq!(sstables_exists.unwrap().get::<i64, _>("count"), 0);

        let _ = tx.commit().await;

        // Stop the embedded PostgreSQL instance
        postgresql.stop().await.expect("Failed to stop PostgreSQL");
    }
}
