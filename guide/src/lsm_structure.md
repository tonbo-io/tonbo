# LSM Architecture

## Interfaces

Exposes `txn`/`basic`/`MVCC` read-write interfaces to external

* Supports `get/scan` operations.
* Supports `insert/remove` operations.

## Path
Some part of the lsm tree component is persist to `Local Disk/S3`.
- `WAL`:`base_path/wal`
- `SsTable`: `SsTable` can be stored at either `Local Disk` of `S3`. This can be set by `.level_path`
    - `.level_path(level, Path, fs_option, cached)` can set level's `Path/cached/fs_option` mentioned below.
    - each level of `Sstable` can be stored at seperated path set by `.level_path` and `Local Disk/S3`, under `/{Path}` provided by `.level_path`.
    - if a level is set as `S3`, then `cache` can be set for this level and cached at `Local Disk`, under `/{base_path}`
- `Manifest`:`Manifest` is stored at `{base_path}/version`
    - `Manifest` will be further stored at a Distributed Coordination Service.


## DbStorage

* `DbOptions::base_path/base_fs`: Configures storage backend type/path, currently supports `local/S3`.

## Memtable

```rust
pub(crate) struct MutableMemTable<R>
where
    R: Record,
{
    data: SkipMap<Ts<<R::Schema as Schema>::Key>, Option<R>>,
    wal: Option<Mutex<WalFile<R>>>,
    trigger: Arc<dyn FreezeTrigger<R>>,
    schema: Arc<R::Schema>,
}
```

* `trigger_type`: Triggers `freeze` based on `SizeOfMem/Length` of memtable.

The memtable stores full data in a skipmap, so `get/scan` directly call skipmap APIs with `range` parameters.

* Current memtable interface provides `insert` for single `key/value` pair.

## WAL

```rust
pub(crate) struct Log<R>
where
    R: Record,
{
    pub(crate) key: Ts<<R::Schema as Schema>::Key>,
    pub(crate) value: Option<R>,
    pub(crate) log_type: Option<LogType>,
}
```

* `use_wal`: WAL is configurable, which means data integrity not guaranteed when disabled.
* `wal_buffer_size`: Controls buffer size for WAL in the logger.
* WAL handles transaction writes through `batch` or single writes via `log_type`:

  * `Full`: Single write operations.
  * `First/Middle/Last`: Batch writes marked with sequence `First->Middle...Middle->Last`.

The data structure for the WAL is provided by `fusio` and is composed of three layers.

1. `buffer`: Memory-staged batch writes.
2. `local_fs`: Local storage on service node.
3. `fs`: Remote S3 storage.

The dataflow of WAL is `buffer->local_fs->fs`.

`key/value` pairs are first written to `buffer`, and then immediately inserted into the memtable. The subsequent flush of `key/value` pairs from `local_fs` to `fs` is performed by a background process, but can also be triggered manually.

## Transaction

Transaction includes the following components:

* `local`: Stores writes operation in a in-memory datastructure `BTreeMap`.
* `snapshot`: Consistent view at transaction start.

Tonbo uses optimistic transaction:

1. Obtain a consistent view `snapshot` at transaction start.
2. Write operations in transcation are first stored in a in-memory data structure BTreeMap `local`.
3. The `commit` process is similar to a two-phase commit. For each key, it checks for timestamp conflicts in the snapshot. If all keys reach consensus without errors, then `key/value` pairs are written into WAL by batch and are inserted into memtable one by one.
4. The read operation consists of two steps:

    * Search in the in-memory data structure BTreeMap `local`
    * Search in the `snapshot`

## Immutable Memtable

```rust
pub(crate) struct Immutable<A>
where
    A: ArrowArrays,
{
    data: A,
    index: BTreeMap<Ts<<<A::Record as Record>::Schema as Schema>::Key>, u32>,
}
```

* Converts to `Arrow` format when memtable becomes immutable
* `index`: `BTreeMap` mapping `<key, offset>`, each query should get offset from index first, and then query `value` in `data`.
* `data`: `ArrowArray` consist of `value`.

The query process of immutable memtable is different from memtable:

```rust
let range = self.index.range::<TsRef<<<A::Record as Record>::Schema as Schema>::Key>, _>((lower, upper));
let boxed_range = if order == Some(Order::Desc) {
    Box::new(range.rev())
} else {
    Box::new(range)
};
ImmutableScan::<A::Record>::new(boxed_range, self.data.as_record_batch(), projection_mask)
```

For `get(key)`, it can be transformed into `scan(key, key)`. The final result returned by `scan` is then converted into `RecordBatchEntry` using Arrow's `.as_record_batch()` method.

## SsTable

```rust
pub(crate) struct SsTable<R>
where
    R: Record,
{
    reader: BoxedFileReader,
    _marker: PhantomData<R>,
}
```

`reader` reads the `SsTable` from the storage backend into memory and provides iterator-like functionality.

Similar to `immutable memtable`, the core interface provided by `SsTable` is `get/scan`. The `get(key)` operation is also implemented via `scan(key, key)`.

```rust
Ok(SsTableScan::new(
            builder.with_row_filter(filter).build()?,
            projection_mask,
            full_schema,
)
```

The process within `scan` first obtains a `reader` in `ParquetResult` format via `.into_parquet_builder()`. Then, it retrieves the corresponding columns using the `prohection_mask` and filters for the relevant key range using `filter`, finally returning an`SsTableScan` type.

Currently, `SsTable` has two storage backend methods:

* `FsOptions::Local`
* `FsOptions::S3`

For the upper layer, `fusio` abstracts this using `DynFs`, which provides file reading interfaces for these different storage methods.

`SsTable` uses `Parquet` for its underlying storage.

* `DbOption::max_sst_file_size`: Controls the default `SsTable` file size.
* `DbOption::WriterProperties`: Controls Parquet-level parameters. The following refer to `Parquet` configurations:

  * `set_compression`: File compression method, supporting algorithms like `LZ4` and `ZSTD` and etc.
  * `set_column_statistics_enabled`: Whether to generate column statistics (used for predicate pushdown) and the granularity of this information.
  * `set_column_bloom_filter_enabled`: Whether to enable Bloom filters for columns.
  * `set_sorting_columns`: Specifies that metadata is to be sorted by the primary key.

## Compaction

For `compaction`, a manual interface `flush()` is also provided. This can be used to manually flush the `WAL` to disk and trigger `compaction`.

The specific process involves placing the `readers` of the tables to be merged into `streams`, and then `build_table` is used to form a new `SsTable`.

`compaction_option`: This controls the `compaction` strategy. Currently, only the `leveled compaction` strategy is available.

### minor compaction

When `memtable` is `freezed`, a `minor compaction` check is triggered based on the following parameters:

* `immutable_chunk_num`: The threshold for the number of immutable chunks that triggers `minor compaction`.
* `immutable_chunk_max_num`: The maximum number of immutable chunks allowed.

### major compaction

When a new `SsTable` is flushed, a `major compaction` check is triggered and is controlled by the following parameters:

* `major_threshold_with_sst_size`: The base threshold for `major compaction` (default: 4).
* `level_sst_magnification`: The magnification factor between levels for `SsTables` (default: 10).

## Manifest

Manifest of tonbo uses a similar datastructure as WAL provided by `fusio`, and is finally persisted to `fs`.

### Version Edit

```rust
pub(crate) enum VersionEdit<K> {
    Add { level: u8, scope: Scope<K> },
    Remove { level: u8, gen: FileId },
    LatestTimeStamp { ts: Timestamp },
    NewLogLength { len: u32 },
}
```

There are 4 types of "Version Edit" logs:

* `Add`: Specifies an SST (Sorted String Table) file to be added at a designated level.
* `Remove`: Specifies an SST file to be removed from a designated level.
* `LatestTimeStamp`: Updates the newest timestamp (this could refer to the overall version's timestamp or a specific dataset's).
* `NewLogLength`: Updates the length of the version log (likely referring to the MANIFEST log or a similar metadata log).

```rust
pub(crate) struct VersionSet<R>
where
    R: Record,
{
    inner: Arc<RwLock<VersionSetInner<R>>>,
    clean_sender: Sender<CleanTag>,
    timestamp: Arc<AtomicU32>,
    option: Arc<DbOption>,
    manager: Arc<StoreManager>,
}
```

VersionSet maintains the file information source data, using `apply_edits` for replay.

`inner` contains the latest snapshot of manifest:

```rust
pub(crate) struct VersionSetInner<R>
where
    R: Record,
{
    current: VersionRef<R>,
    log_id: FileId,
    deleted_wal: Vec<FileId>,
    deleted_sst: Vec<(FileId, usize)>,
}
```

* `current`: Maintains the latest Version.
* `deleted_wal/ssts`: Maintains the WAL and SST files that are pending deletion.

Regarding the logic for acquiring a VersionSet, error handling needs to be considered. If multiple VersionSets exist, the latest one is considered incomplete. Therefore, the second to last VersionSet is used for recovery.

* `DbOption::version_log_threshold`: Since the manifest (which is partially stored in memory) continuously grows, a threshold can be configured. When it grows to `DbOption::version_log_threshold`, `.rewrite` is called to force a flush to disk, reducing memory consumption. And `deleted_wal/ssts` are called to be cleaned.

### Cleaner

```rust
pub(crate) struct Cleaner {
    tag_recv: Receiver<CleanTag>,
    gens_map: BTreeMap<Timestamp, (Vec<(FileId, usize)>, bool)>,
    option: Arc<DbOption>,
    manager: Arc<StoreManager>,
}
```

The `Cleaner` receives different CleanTag operations `(Add/Clean/RecoverClean)` in the background and performs corresponding operations on the file system. After the corresponding manifest is flushed to disk, it deletes unnecessary `wal/ssts`.