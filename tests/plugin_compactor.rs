#[cfg(not(target_arch = "wasm32"))]
mod test {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tempfile::TempDir;
    use tonbo::{
        compaction::{error::CompactionError, Compactor},
        context::Context,
        executor::tokio::TokioExecutor,
        fs::FileId,
        inmem::immutable::ImmutableMemTable,
        record,
        record::Record,
        version::{edit::VersionEdit, TransactionTs},
        CompactionExecutor, DbOption, Path, DB,
    };
    use tonbo_macros::Record;

    #[derive(Record, Debug, PartialEq, Eq, Clone)]
    pub struct TestRecord {
        #[record(primary_key)]
        pub vstring: String,
        pub vu32: u32,
        pub vbool: Option<bool>,
    }

    pub struct NoCompactor<R: Record> {
        db_option: Arc<DbOption>,
        record_schema: Arc<R::Schema>,
        ctx: Arc<Context<R>>,
    }

    impl<R: Record> NoCompactor<R> {
        pub fn new(
            db_option: Arc<DbOption>,
            record_schema: Arc<R::Schema>,
            ctx: Arc<Context<R>>,
        ) -> Self {
            Self {
                db_option,
                record_schema,
                ctx,
            }
        }
    }

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    impl<R> Compactor<R> for NoCompactor<R>
    where
        R: Record,
        <<R as Record>::Schema as tonbo::record::Schema>::Columns: Send + Sync,
    {
        async fn check_then_compaction(
            &self,
            batches: Option<
                &[(
                    Option<FileId>,
                    ImmutableMemTable<<R::Schema as record::Schema>::Columns>,
                )],
            >,
            recover_wal_ids: Option<Vec<FileId>>,
            _is_manual: bool,
        ) -> Result<(), CompactionError<R>> {
            if let Some(batches) = batches {
                if let Some(scope) = Self::minor_compaction(
                    &self.db_option,
                    recover_wal_ids,
                    batches,
                    &self.record_schema,
                    self.ctx.manager(),
                )
                .await?
                {
                    // Update manifest with new L0 SST
                    let version_ref = self.ctx.current_manifest().await;
                    let mut version_edits = vec![VersionEdit::Add { level: 0, scope }];
                    version_edits.push(VersionEdit::LatestTimeStamp {
                        ts: version_ref.as_ref().increase_ts(),
                    });

                    self.ctx
                        .update_manifest(version_edits, None)
                        .await
                        .map_err(|e| CompactionError::Manifest(e))?;
                }
            }
            // TODO: Also add the major part, and change the publicity based on that.
            Ok(())
        }
    }

    // Since we can't access the private Compactor trait from external tests,
    // we'll implement CompactionExecutor directly with a no-op implementation
    impl<R> CompactionExecutor<R> for NoCompactor<R>
    where
        R: Record,
        <<R as Record>::Schema as tonbo::record::Schema>::Columns: Send + Sync,
    {
        fn check_then_compaction<'a>(
            &'a self,
            batches: Option<
                &'a [(
                    Option<FileId>,
                    ImmutableMemTable<<R::Schema as tonbo::record::Schema>::Columns>,
                )],
            >,
            recover_wal_ids: Option<Vec<FileId>>,
            is_manual: bool,
        ) -> impl std::future::Future<Output = Result<(), CompactionError<R>>> + Send + 'a {
            <Self as Compactor<R>>::check_then_compaction(self, batches, recover_wal_ids, is_manual)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_no_compactor() {
        let temp_dir = TempDir::new().unwrap();

        let schema = TestRecordSchema;

        // Create DB with NoCompactor (plugin)
        let option = DbOption::new(Path::new(temp_dir.path()).unwrap(), &schema);

        // Use DB::new_with_compactor_factory with factory closure - this solves the circular
        // dependency!
        let db_no_compact: DB<TestRecord, TokioExecutor> = DB::new_with_compactor_factory(
            option,
            TokioExecutor::default(),
            schema,
            |db_option, record_schema, ctx| NoCompactor::new(db_option, record_schema, ctx),
        )
        .await
        .unwrap();

        // Insert the data to databases
        for i in 0..1001 {
            let record = TestRecord {
                vstring: format!("test_key_{:03}", i),
                vu32: i,
                vbool: Some(i % 3 == 0),
            };

            db_no_compact.insert(record.clone()).await.unwrap();

            if i % 100 == 0 {
                db_no_compact.flush().await.unwrap();
            }
        }

        let current = db_no_compact.current_manifest().await;
        let slice = &current.level_slice[0];
        // Should have 11 SSTs here
        assert_eq!(slice.len(), 11);
        for file in slice.iter() {
            println!("NoCompactor L0 file: {:?}", file);
        }
    }
}
