use fusio::path::Path;
use pyo3::{pyclass, pymethods};
use tonbo::record::DynRecord;

/// configure the operating parameters of each component in the `DB`
#[pyclass]
#[derive(Debug, Clone)]
#[allow(unused)]
pub struct DbOption {
    /// cached message size in parquet cleaner
    #[pyo3(get, set)]
    clean_channel_buffer: usize,
    /// len threshold of `immutables` when minor compaction is triggered
    #[pyo3(get, set)]
    immutable_chunk_num: usize,
    immutable_chunk_max_num: usize,
    /// magnification that triggers major compaction between different levels
    #[pyo3(get, set)]
    level_sst_magnification: usize,
    #[pyo3(get, set)]
    major_default_oldest_table_num: usize,
    major_l_selection_table_max_num: usize,
    /// threshold for the number of `parquet` when major compaction is triggered
    #[pyo3(get, set)]
    major_threshold_with_sst_size: usize,
    /// Maximum size of each parquet
    #[pyo3(get, set)]
    max_sst_file_size: usize,
    #[pyo3(get, set)]
    version_log_snapshot_threshold: u32,
    #[pyo3(get, set)]
    use_wal: bool,
    /// build the `DB` storage directory based on the passed path
    #[pyo3(get, set)]
    path: String,
}

#[pymethods]
impl DbOption {
    #[new]
    pub(crate) fn new(path: String) -> Self {
        Self {
            clean_channel_buffer: 0,
            immutable_chunk_num: 0,
            immutable_chunk_max_num: 0,
            level_sst_magnification: 0,
            major_default_oldest_table_num: 0,
            major_l_selection_table_max_num: 0,
            major_threshold_with_sst_size: 0,
            max_sst_file_size: 0,
            version_log_snapshot_threshold: 0,
            use_wal: false,
            path,
        }
    }
}

impl DbOption {
    pub(crate) fn into_option(
        self,
        primary_key_index: usize,
        primary_key_name: String,
    ) -> tonbo::DbOption<DynRecord> {
        std::fs::create_dir_all(self.path.clone()).unwrap();
        tonbo::DbOption::with_path(
            Path::from_filesystem_path(self.path).unwrap(),
            primary_key_name,
            primary_key_index,
        )
    }
}
