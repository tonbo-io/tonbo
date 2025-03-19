
```rust
pub struct DbOption {
    pub(crate) clean_channel_buffer: usize,
    pub(crate) base_path: Path,
    pub(crate) base_fs: FsOptions,
    pub(crate) level_paths: Vec<Option<(Path, FsOptions)>>,
    pub(crate) immutable_chunk_num: usize,
    pub(crate) immutable_chunk_max_num: usize,
    pub(crate) level_sst_magnification: usize,
    pub(crate) major_default_oldest_table_num: usize,
    pub(crate) major_l_selection_table_max_num: usize,
    pub(crate) major_threshold_with_sst_size: usize,
    pub(crate) max_sst_file_size: usize,
    pub(crate) version_log_snapshot_threshold: u32,
    pub(crate) trigger_type: TriggerType,
    pub(crate) use_wal: bool,
    pub(crate) wal_buffer_size: usize,
    pub(crate) write_parquet_properties: WriterProperties,
}
```
