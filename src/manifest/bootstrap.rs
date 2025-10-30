use std::sync::Arc;

use fusio::mem::fs::InMemoryFs;
use fusio_manifest::{
    BackoffPolicy, BlockingExecutor, CheckpointStoreImpl, HeadStoreImpl, LeaseStoreImpl,
    ManifestContext, SegmentStoreImpl,
};
use futures::executor::block_on;

use super::{
    domain::{TableHead, TableId},
    driver::{Manifest, ManifestResult, Stores},
};

/// In-memory manifest type wired against `fusio`'s memory FS for dev/test flows.
pub(crate) type InMemoryManifest = Manifest<
    HeadStoreImpl<InMemoryFs>,
    SegmentStoreImpl<InMemoryFs>,
    CheckpointStoreImpl<InMemoryFs>,
    LeaseStoreImpl<InMemoryFs, BlockingExecutor>,
>;

/// Construct an in-memory manifest and prime its head with the provided schema version.
pub(crate) fn init_in_memory_manifest(
    schema_version: u32,
) -> ManifestResult<(InMemoryManifest, TableId)> {
    let fs = InMemoryFs::new();
    let head = HeadStoreImpl::new(fs.clone(), "head.json");
    let segment = SegmentStoreImpl::new(fs.clone(), "segments");
    let checkpoint = CheckpointStoreImpl::new(fs.clone(), "");
    let lease = LeaseStoreImpl::new(fs, "", BackoffPolicy::default(), BlockingExecutor);
    let ctx = Arc::new(ManifestContext::new(BlockingExecutor));
    let manifest = Manifest::open(Stores::new(head, segment, checkpoint, lease), ctx);
    let table_id = TableId::new();
    block_on(async {
        manifest
            .init_table_head(
                table_id,
                TableHead {
                    table_id,
                    schema_version,
                    wal_floor: None,
                    last_manifest_txn: None,
                },
            )
            .await
    })?;
    Ok((manifest, table_id))
}
