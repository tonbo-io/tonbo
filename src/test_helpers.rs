//! Test-only helpers exposed under the `test-helpers` feature.

use std::sync::Arc;

use arrow_array::RecordBatch;
use fusio::executor::{Executor, Timer};

use crate::{
    db::DB,
    key::KeyOwned,
    mode::Mode,
    ondisk::sstable::{SsTable, SsTableConfig, SsTableDescriptor, SsTableError},
};

/// Extension trait exposing internal DB hooks for integration tests.
pub trait DbTestExt<M, E>
where
    M: Mode,
    M::Key: Eq + std::hash::Hash + Clone,
    E: Executor + Timer,
{
    /// Force-flush immutables into an SST with the provided descriptor.
    fn flush_immutables_with_descriptor_for_tests(
        &mut self,
        config: Arc<SsTableConfig>,
        descriptor: SsTableDescriptor,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SsTable<M>, SsTableError>> + '_>>
    where
        M: Mode<ImmLayout = RecordBatch, Key = KeyOwned>;

    /// Prune WAL segments below the manifest floor.
    fn prune_wal_segments_below_floor_for_tests(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>>;
}

impl<M, E> DbTestExt<M, E> for DB<M, E>
where
    M: Mode,
    M::Key: Eq + std::hash::Hash + Clone,
    E: Executor + Timer,
{
    fn flush_immutables_with_descriptor_for_tests(
        &mut self,
        config: Arc<SsTableConfig>,
        descriptor: SsTableDescriptor,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SsTable<M>, SsTableError>> + '_>>
    where
        M: Mode<ImmLayout = RecordBatch, Key = KeyOwned>,
    {
        Box::pin(self.flush_immutables_with_descriptor(config, descriptor))
    }

    fn prune_wal_segments_below_floor_for_tests(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + '_>> {
        Box::pin(self.prune_wal_segments_below_floor())
    }
}
