use pyo3::{pyclass, pymethods, PyResult};
use tonbo::option::Path;

use crate::{ExceedsMaxLevelError, FsOptions};

pub(crate) const MAX_LEVEL: usize = 7;

/// configure the operating parameters of each component in the `DB`
#[pyclass]
#[derive(Debug, Clone)]
pub struct DbOption {
    /// cached message size in parquet cleaner
    #[pyo3(get, set)]
    clean_channel_buffer: usize,
    /// len threshold of `immutables` when minor compaction is triggered
    #[pyo3(get, set)]
    immutable_chunk_num: usize,
    /// magnification that triggers major compaction between different levels
    #[pyo3(get, set)]
    level_sst_magnification: usize,
    #[pyo3(get, set)]
    major_default_oldest_table_num: usize,
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
    /// Maximum size of WAL buffer size
    #[pyo3(get, set)]
    wal_buffer_size: usize,
    /// build the `DB` storage directory based on the passed path
    #[pyo3(get, set)]
    path: String,
    #[pyo3(get, set)]
    base_fs: FsOptions,
    level_paths: Vec<Option<(String, FsOptions)>>,
}

#[pymethods]
impl DbOption {
    #[new]
    fn new(path: String) -> Self {
        Self {
            clean_channel_buffer: 10,
            immutable_chunk_num: 3,
            level_sst_magnification: 10,
            major_default_oldest_table_num: 3,
            major_threshold_with_sst_size: 4,
            max_sst_file_size: 256 * 1024 * 1024,
            version_log_snapshot_threshold: 200,
            use_wal: true,
            wal_buffer_size: 4 * 1024,
            path,
            base_fs: FsOptions::Local {},
            level_paths: vec![None; MAX_LEVEL],
        }
    }

    fn level_path(&mut self, level: usize, path: String, fs_options: FsOptions) -> PyResult<()> {
        if level >= MAX_LEVEL {
            ExceedsMaxLevelError::new_err("Exceeds max level");
        }
        self.level_paths[level] = Some((path, fs_options));
        Ok(())
    }
}

impl DbOption {
    pub(crate) fn into_option(self) -> tonbo::DbOption {
        let mut opt = tonbo::DbOption::new(Path::from(self.path))
            .clean_channel_buffer(self.clean_channel_buffer)
            .immutable_chunk_num(self.immutable_chunk_num)
            .level_sst_magnification(self.level_sst_magnification)
            .major_default_oldest_table_num(self.major_default_oldest_table_num)
            .major_threshold_with_sst_size(self.major_threshold_with_sst_size)
            .max_sst_file_size(self.max_sst_file_size)
            .version_log_snapshot_threshold(self.version_log_snapshot_threshold)
            .base_fs(tonbo::option::FsOptions::from(self.base_fs));
        for (level, path) in self.level_paths.into_iter().enumerate() {
            if let Some((path, fs_options)) = path {
                opt = opt
                    .level_path(
                        level,
                        Path::from(path),
                        tonbo::option::FsOptions::from(fs_options),
                    )
                    .unwrap();
            }
        }
        if !self.use_wal {
            opt = opt.disable_wal()
        }
        opt
    }
}
