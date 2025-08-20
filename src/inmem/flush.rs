use std::{mem, sync::Arc};

use fusio::DynFs;

use crate::{
    fs::FileId,
    inmem::{immutable::ImmutableMemTable, mutable::MutableMemTable},
    record::{Record, Schema as RecordSchema},
    DbError,
};

/// Flush mutable memtable to immutable and return owned batches ready for compaction.
///
/// This function performs only the minimal critical section work while holding
/// the `DbStorage` write lock: it converts the current mutable into an
/// immutable batch if needed, determines how many immutables to flush, then
/// drains those immutables from storage and returns ownership to the caller.
/// Heavy I/O and merging should happen after releasing the lock.
pub(crate) async fn minor_flush<R>(
    db_storage: &mut crate::DbStorage<R>,
    base_fs: Arc<dyn DynFs>,
    immutable_chunk_num: usize,
    immutable_chunk_max_num: usize,
    is_manual: bool,
) -> Result<
    Option<(
        Vec<(
            Option<FileId>,
            ImmutableMemTable<<R::Schema as RecordSchema>::Columns>,
        )>,
        Option<Vec<FileId>>,
    )>,
    DbError,
>
where
    R: Record,
    <R::Schema as RecordSchema>::Columns: Send + Sync,
{
    // Reset trigger
    db_storage.trigger.reset();

    // Add the mutable memtable into the immutable memtable
    if !db_storage.mutable.is_empty() {
        let trigger_clone = db_storage.trigger.clone();

        // Replace mutable memtable with new memtable
        let old_mutable = mem::replace(
            &mut db_storage.mutable,
            MutableMemTable::new(
                &db_storage.option,
                trigger_clone,
                base_fs,
                db_storage.record_schema.clone(),
            )
            .await?,
        );
        let (file_id, immutable) = old_mutable.into_immutable().await?;
        db_storage.immutables.push((file_id, immutable));
    } else if !is_manual {
        return Ok(None);
    }

    // If manual, we always flush if there are any immutables
    // If not manual, we flush only if the number of immutables exceeds the limit
    if (is_manual && !db_storage.immutables.is_empty())
        || db_storage.immutables.len() > immutable_chunk_max_num
    {
        let recovered_wal_ids = db_storage.recover_wal_ids.take();

        let chunk_num = if is_manual {
            db_storage.immutables.len()
        } else {
            immutable_chunk_num.min(db_storage.immutables.len())
        };

        if chunk_num > 0 {
            // Drain owned immutables to be processed outside the lock
            let drained = db_storage.immutables.drain(..chunk_num).collect();
            return Ok(Some((drained, recovered_wal_ids)));
        }
    }

    Ok(None)
}
