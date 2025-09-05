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
            scope.max.encode(&mut max_key_cursor).await?;

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
        tx.commit().await?;

        let mut timestamp = Arc::new(AtomicU32::default());
        if let Some(version_snapshot) = version_snapshot {
            timestamp = Arc::new((version_snapshot.timestamp as u32).into());
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
            timestamp,
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
                ORDER BY created_at DESC 
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

        // Non recovery mode will persist the log length
        if !is_recover {
            version_edits.push(VersionEdit::NewLogLength { len: edit_len });
            eprint!("I'm here to push: {edit_len}")
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
                    eprint!("I'm here to persist: {len}")
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

// NOTE:
// 1. The tests here are actually integration test that runs against postgres backend. We should
//    run this test in our CI/CD mechanism
// 2. To run test you need to have docker daemon installed on your machine. Then you can run `cargo test --
//    metadata::mds::tests --ignored require-docker`
#[cfg(test)]
mod tests {
    use flume::bounded;
    use fusio_log::Path;
    use rstest::{fixture, rstest};
    use tempfile::TempDir;
    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        ContainerAsync, GenericImage, ImageExt,
    };

    use super::*;
    use crate::{fs::generate_file_id, record::test::StringSchema};

    #[fixture]
    async fn postgres_container() -> ContainerAsync<GenericImage> {
        GenericImage::new("postgres", "14")
            .with_exposed_port(5432.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_PASSWORD", "password")
            .with_env_var("POSTGRES_USER", "test_user")
            .with_env_var("POSTGRES_DB", "test_db")
            .start()
            .await
            .unwrap()
    }

    async fn string_metadata_service(
        container: &ContainerAsync<GenericImage>,
    ) -> MetadataManifestStorage<String> {
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let connection_url = format!("postgresql://test_user:password@{}:{}/test_db", host, port);

        // Bootstrap script is idempotent so it's safe to execute multiple times
        let pool = sqlx::PgPool::connect(&connection_url).await.unwrap();
        let bootstrap_sql = include_str!("bootstrap.sql");
        sqlx::raw_sql(bootstrap_sql)
            .execute(&pool)
            .await
            .expect("Failed to execute bootstrap script");
        pool.close().await;

        let temp_dir = TempDir::new().unwrap();
        let (sender, _) = bounded(1);
        let option = Arc::new(DbOption::new(
            Path::from_filesystem_path(temp_dir.path()).unwrap(),
            &StringSchema,
        ));

        MetadataManifestStorage::<String>::new(&connection_url, sender.clone(), option.clone())
            .await
            .expect("Failed to create metadata service")
    }

    #[rstest]
    #[ignore = "require-docker"]
    #[tokio::test(flavor = "multi_thread")]
    async fn mds_can_initialize_freshly(
        #[future] postgres_container: ContainerAsync<GenericImage>,
    ) {
        let container = postgres_container.await;
        let mds = string_metadata_service(&container).await;

        let version = mds.read_current().await.unwrap().unwrap();
        assert_eq!(version.log_length, 0);
        assert_eq!(version.ts.inner(), 0);
        assert_eq!(version.level_slice[0].len(), 0);
    }

    #[rstest]
    #[ignore = "require-docker"]
    #[tokio::test(flavor = "multi_thread")]
    async fn mds_can_persist_timestamp_when_reinitialized(
        #[future] postgres_container: ContainerAsync<GenericImage>,
    ) {
        let container = postgres_container.await;
        let mds = string_metadata_service(&container).await;

        mds.apply_edits(
            vec![VersionEdit::LatestTimeStamp { ts: 20_u32.into() }],
            None,
            false,
        )
        .await
        .unwrap();
        drop(mds);

        let mds = string_metadata_service(&container).await;
        let version = mds.read_current().await.unwrap().unwrap();
        assert_eq!(version.ts, 20_u32.into());
        assert_eq!(mds.load_ts(), 20_u32.into());
    }

    #[rstest]
    #[ignore = "require-docker"]
    #[tokio::test(flavor = "multi_thread")]
    async fn mds_can_apply_edits_in_sequential(
        #[future] postgres_container: ContainerAsync<GenericImage>,
    ) {
        let container = postgres_container.await;
        let mds = string_metadata_service(&container).await;

        let scope_0 = Scope {
            min: "0".to_string(),
            max: "1".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 7,
        };
        let scope_1 = Scope {
            min: "2".to_string(),
            max: "3".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 7,
        };
        let scope_2 = Scope {
            min: "4".to_string(),
            max: "5".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 7,
        };

        mds.apply_edits(
            vec![VersionEdit::Add {
                level: 0,
                scope: scope_0.clone(),
            }],
            None,
            false,
        )
        .await
        .unwrap();

        mds.apply_edits(
            vec![VersionEdit::Add {
                level: 0,
                scope: scope_1.clone(),
            }],
            None,
            false,
        )
        .await
        .unwrap();
        mds.apply_edits(
            vec![VersionEdit::Add {
                level: 0,
                scope: scope_2.clone(),
            }],
            None,
            false,
        )
        .await
        .unwrap();

        let version = mds.read_current().await.unwrap().unwrap();
        assert_eq!(version.log_length, 3);
        assert_eq!(version.level_slice[1].len(), 0);
        assert_eq!(version.level_slice[0], vec![scope_0, scope_1, scope_2])
    }

    #[rstest]
    #[ignore = "require-docker"]
    #[tokio::test(flavor = "multi_thread")]
    async fn mds_can_apply_edits_in_batch(
        #[future] postgres_container: ContainerAsync<GenericImage>,
    ) {
        let container = postgres_container.await;
        let mds = string_metadata_service(&container).await;

        let scope_0 = Scope {
            min: "2".to_string(),
            max: "2".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 7,
        };
        let scope_1 = Scope {
            min: "1".to_string(),
            max: "1".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 7,
        };
        let scope_2 = Scope {
            min: "4".to_string(),
            max: "4".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 7,
        };

        mds.apply_edits(
            vec![
                VersionEdit::Add {
                    level: 1,
                    scope: scope_0.clone(),
                },
                VersionEdit::Add {
                    level: 1,
                    scope: scope_1.clone(),
                },
                VersionEdit::Add {
                    level: 1,
                    scope: scope_2.clone(),
                },
            ],
            None,
            false,
        )
        .await
        .unwrap();

        let version = mds.read_current().await.unwrap().unwrap();
        assert_eq!(version.log_length, 3);
        assert_eq!(version.level_slice[0].len(), 0);
        assert_eq!(version.level_slice[1], vec![scope_1, scope_0, scope_2]);
    }

    #[rstest]
    #[ignore = "require-docker"]
    #[tokio::test(flavor = "multi_thread")]
    async fn mds_can_apply_edits_in_batch_and_sort_level(
        #[future] postgres_container: ContainerAsync<GenericImage>,
    ) {
        let container = postgres_container.await;
        let mds = string_metadata_service(&container).await;

        let scope_0 = Scope {
            min: "4".to_string(),
            max: "6".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 7,
        };
        let scope_1 = Scope {
            min: "1".to_string(),
            max: "3".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 7,
        };
        let scope_2 = Scope {
            min: "7".to_string(),
            max: "9".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 7,
        };
        let scope_3 = Scope {
            min: "0".to_string(),
            max: "0".to_string(),
            gen: generate_file_id(),
            wal_ids: None,
            file_size: 7,
        };

        mds.apply_edits(
            vec![VersionEdit::Add {
                level: 1,
                scope: scope_0.clone(),
            }],
            None,
            false,
        )
        .await
        .unwrap();

        mds.apply_edits(
            vec![
                VersionEdit::Add {
                    level: 1,
                    scope: scope_1.clone(),
                },
                VersionEdit::Add {
                    level: 1,
                    scope: scope_2.clone(),
                },
                VersionEdit::Add {
                    level: 1,
                    scope: scope_3.clone(),
                },
            ],
            None,
            false,
        )
        .await
        .unwrap();

        let version = mds.read_current().await.unwrap().unwrap();
        assert_eq!(version.log_length, 4);
        assert_eq!(version.level_slice[0].len(), 0);
        let min_key_slice = version.level_slice[1]
            .iter()
            .map(|scope| scope.min.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            min_key_slice,
            vec![
                "0".to_string(),
                "1".to_string(),
                "4".to_string(),
                "7".to_string()
            ]
        );
    }
}
