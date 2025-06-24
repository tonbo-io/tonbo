use tonbo::option::Path;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

use crate::FsOptions;

pub(crate) const MAX_LEVEL: usize = 7;

#[wasm_bindgen]
#[derive(Debug, Clone)]
pub struct DbOption {
    /// cached message size in parquet cleaner
    clean_channel_buffer: usize,
    /// len threshold of `immutables` when minor compaction is triggered
    immutable_chunk_num: usize,
    /// magnification that triggers major compaction between different levels
    level_sst_magnification: usize,
    major_default_oldest_table_num: usize,
    /// threshold for the number of `parquet` when major compaction is triggered
    major_threshold_with_sst_size: usize,
    /// Maximum size of each parquet
    max_sst_file_size: usize,
    version_log_snapshot_threshold: u32,
    use_wal: bool,
    /// Maximum size of WAL buffer size
    wal_buffer_size: usize,
    /// build the `DB` storage directory based on the passed path
    path: String,
    base_fs: FsOptions,
    level_paths: Vec<Option<(String, FsOptions)>>,
}

#[wasm_bindgen]
impl DbOption {
    #[wasm_bindgen(constructor)]
    pub fn new(path: String) -> Result<Self, JsValue> {
        let path = Path::from_opfs_path(path)
            .map_err(|err| JsValue::from(err.to_string()))?
            .to_string();
        Ok(Self {
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
            base_fs: FsOptions::local(),
            level_paths: vec![None; MAX_LEVEL],
        })
    }

    pub fn level_path(
        mut self,
        level: usize,
        path: String,
        fs_options: FsOptions,
    ) -> Result<Self, JsValue> {
        self.level_paths[level] = Some((path.to_string(), fs_options));
        Ok(self)
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
            .wal_buffer_size(self.wal_buffer_size)
            .base_fs(self.base_fs.into_fs_options());

        for (level, path) in self.level_paths.into_iter().enumerate() {
            if let Some((path, fs_options)) = path {
                let path = fs_options.path(path).unwrap();
                opt = opt
                    .level_path(level, path, fs_options.into_fs_options())
                    .unwrap();
            }
        }
        if !self.use_wal {
            opt = opt.disable_wal()
        }
        opt
    }
}
