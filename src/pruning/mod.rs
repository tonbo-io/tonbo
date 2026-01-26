#![allow(dead_code)]
//! Pruning abstraction boundary for Tonbo’s read path.
//!
//! This module defines the library-agnostic pruning interface. Implementations
//! live behind the [`Pruner`] trait so Tonbo core can remain independent of
//! any specific pruning library.

pub(crate) mod adapter;
pub(crate) mod aisle;
pub(crate) mod config;
pub(crate) mod noop;

use std::{collections::HashMap, pin::Pin};

use arrow_schema::SchemaRef;
use fusio::dynamic::MaybeSendFuture;
use parquet::{
    arrow::{
        arrow_reader::RowSelection,
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    bloom_filter::Sbbf,
    file::metadata::ParquetMetaData,
};
use thiserror::Error;

use self::{
    aisle::AislePruner,
    config::{PrunerPolicy, PruningConfig},
    noop::NoopPruner,
};
use crate::{db::StorageBackend, query::Predicate};

/// Input bundle for pruning decisions.
pub(crate) struct PruneInput<'a> {
    /// Parquet metadata for the file being scanned.
    pub(crate) metadata: &'a ParquetMetaData,
    /// Arrow schema for the file.
    pub(crate) schema: &'a SchemaRef,
    /// Optional predicate provided by the caller.
    pub(crate) predicate: Option<&'a Predicate>,
    /// Storage classification for backend-aware decisions.
    pub(crate) storage_kind: StorageBackend,
    /// Optional async bloom filter provider for remote pruning.
    pub(crate) bloom_provider: Option<&'a mut dyn PruneBloomProvider>,
}

/// Result of a pruning pass.
#[derive(Clone, Debug, Default)]
pub(crate) struct PruneOutput {
    /// Row groups selected for scanning.
    pub(crate) row_groups: Vec<usize>,
    /// Optional row-level selection within row groups.
    pub(crate) row_selection: Option<RowSelection>,
}

/// Errors raised by pruning implementations.
#[derive(Debug, Error)]
pub(crate) enum PruneError {
    /// Generic pruning failure.
    #[error("pruning failed: {0}")]
    Message(String),
}

/// Future returned by [`PruneBloomProvider::bloom_filter`].
pub(crate) type BloomFuture<'a> = Pin<Box<dyn MaybeSendFuture<Output = Option<Sbbf>> + 'a>>;

/// Future returned by [`PruneBloomProvider::bloom_filters_batch`].
pub(crate) type BloomBatchFuture<'a> =
    Pin<Box<dyn MaybeSendFuture<Output = HashMap<(usize, usize), Sbbf>> + 'a>>;

/// Async bloom filter provider used by pruning implementations.
pub(crate) trait PruneBloomProvider: Send {
    /// Load a bloom filter for a specific row group/column pair.
    fn bloom_filter<'a>(&'a mut self, row_group_idx: usize, column_idx: usize) -> BloomFuture<'a>;

    /// Batch bloom filter lookup for multiple pairs.
    fn bloom_filters_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> BloomBatchFuture<'a> {
        Box::pin(async move {
            let mut result = HashMap::new();
            for &(row_group_idx, column_idx) in requests {
                if let Some(filter) = self.bloom_filter(row_group_idx, column_idx).await {
                    result.insert((row_group_idx, column_idx), filter);
                }
            }
            result
        })
    }
}

impl<T> PruneBloomProvider for ParquetRecordBatchStreamBuilder<T>
where
    T: AsyncFileReader + Send + 'static,
{
    fn bloom_filter<'a>(&'a mut self, row_group_idx: usize, column_idx: usize) -> BloomFuture<'a> {
        Box::pin(async move {
            match self
                .get_row_group_column_bloom_filter(row_group_idx, column_idx)
                .await
            {
                Ok(Some(filter)) => Some(filter),
                _ => None,
            }
        })
    }
}

impl PruneError {
    /// Construct a pruning error from a message.
    pub(crate) fn message(msg: impl Into<String>) -> Self {
        Self::Message(msg.into())
    }
}

/// Future returned by [`Pruner::prune`].
pub(crate) type PruneFuture<'a> =
    Pin<Box<dyn MaybeSendFuture<Output = Result<PruneOutput, PruneError>> + 'a>>;

/// Pruning abstraction boundary for Tonbo’s read path.
pub(crate) trait Pruner {
    /// Compute row-group and optional row-level selection for a Parquet file.
    fn prune<'a>(&self, input: PruneInput<'a>) -> PruneFuture<'a>;
}

/// Registry/factory for pruning implementations.
#[derive(Clone, Debug)]
pub(crate) struct PrunerRegistry {
    config: PruningConfig,
}

impl PrunerRegistry {
    /// Create a registry with an explicit configuration.
    pub(crate) fn new(config: PruningConfig) -> Self {
        Self { config }
    }

    /// Run pruning using the selected policy, falling back to a full scan on errors.
    pub(crate) fn prune_or_all<'a>(
        &self,
        input: PruneInput<'a>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = PruneOutput> + 'a>> {
        let fallback = read_all(input.metadata);
        let config = self.config.effective();
        let policy = config.resolve_policy(input.storage_kind);
        match policy {
            PrunerPolicy::Aisle => {
                let pruner = AislePruner::new(config);
                Box::pin(async move { pruner.prune(input).await.unwrap_or(fallback) })
            }
            PrunerPolicy::Noop => {
                let pruner = NoopPruner;
                Box::pin(async move { pruner.prune(input).await.unwrap_or(fallback) })
            }
            PrunerPolicy::Auto => {
                let pruner = NoopPruner;
                Box::pin(async move { pruner.prune(input).await.unwrap_or(fallback) })
            }
        }
    }
}

impl Default for PrunerRegistry {
    fn default() -> Self {
        Self::new(PruningConfig::default())
    }
}

fn read_all(metadata: &ParquetMetaData) -> PruneOutput {
    let row_groups = (0..metadata.num_row_groups()).collect();
    PruneOutput {
        row_groups,
        row_selection: None,
    }
}

/// Run pruning using the default policy selection, falling back to a full scan on errors.
pub(crate) fn prune_or_all<'a>(
    input: PruneInput<'a>,
) -> Pin<Box<dyn MaybeSendFuture<Output = PruneOutput> + 'a>> {
    // NOTE: we intentionally build the registry per-call to keep the pruning boundary
    // isolated from the rest of the read path. Cache here if we decide to reuse it.
    PrunerRegistry::default().prune_or_all(input)
}
