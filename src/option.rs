use std::path::PathBuf;

use parquet::file::properties::WriterProperties;

use crate::{
    fs::{FileId, FileProvider, FileType},
    record::Record,
    version::Version,
};

/// configure the operating parameters of each component in the [`DB`](../struct.DB.html)
#[derive(Debug, Clone)]
pub struct DbOption {
    pub(crate) path: PathBuf,
    pub(crate) max_mutable_len: usize,
    pub(crate) immutable_chunk_num: usize,
    pub(crate) immutable_chunk_max_num: usize,
    pub(crate) major_threshold_with_sst_size: usize,
    pub(crate) level_sst_magnification: usize,
    pub(crate) max_sst_file_size: usize,
    pub(crate) clean_channel_buffer: usize,
    pub(crate) write_parquet_option: Option<WriterProperties>,

    #[allow(unused)]
    pub(crate) use_wal: bool,
    pub(crate) major_default_oldest_table_num: usize,
    pub(crate) major_l_selection_table_max_num: usize,
}

impl<P> From<P> for DbOption
where
    P: Into<PathBuf>,
{
    /// build the default configured [`DbOption`](struct.DbOption.html) based on the passed path
    fn from(path: P) -> Self {
        DbOption {
            path: path.into(),
            max_mutable_len: 3000,
            immutable_chunk_num: 3,
            immutable_chunk_max_num: 5,
            major_threshold_with_sst_size: 4,
            level_sst_magnification: 10,
            max_sst_file_size: 24 * 1024 * 1024,
            clean_channel_buffer: 10,
            write_parquet_option: None,

            use_wal: true,
            major_default_oldest_table_num: 3,
            major_l_selection_table_max_num: 4,
        }
    }
}

impl DbOption {
    /// build the [`DB`](../struct.DB.html) storage directory based on the passed path
    pub fn path(self, path: impl Into<PathBuf>) -> Self {
        DbOption {
            path: path.into(),
            ..self
        }
    }

    /// len threshold of `mutable` when freeze is triggered
    pub fn max_mutable_len(self, max_mutable_len: usize) -> Self {
        DbOption {
            max_mutable_len,
            ..self
        }
    }

    /// len threshold of `immutables` when minor compaction is triggered
    pub fn immutable_chunk_num(self, immutable_chunk_num: usize) -> Self {
        DbOption {
            immutable_chunk_num,
            ..self
        }
    }

    /// threshold for the number of `parquet` when major compaction is triggered
    pub fn major_threshold_with_sst_size(self, major_threshold_with_sst_size: usize) -> Self {
        DbOption {
            major_threshold_with_sst_size,
            ..self
        }
    }

    /// magnification that triggers major compaction between different levels
    pub fn level_sst_magnification(self, level_sst_magnification: usize) -> Self {
        DbOption {
            level_sst_magnification,
            ..self
        }
    }

    /// Maximum size of each parquet
    pub fn max_sst_file_size(self, max_sst_file_size: usize) -> Self {
        DbOption {
            max_sst_file_size,
            ..self
        }
    }

    /// cached message size in parquet cleaner
    pub fn clean_channel_buffer(self, clean_channel_buffer: usize) -> Self {
        DbOption {
            clean_channel_buffer,
            ..self
        }
    }

    /// specific settings for Parquet
    pub fn write_parquet_option(self, write_parquet_option: WriterProperties) -> Self {
        DbOption {
            write_parquet_option: Some(write_parquet_option),
            ..self
        }
    }

    /// disable WAL
    ///
    /// tips: risk of data loss during downtime
    pub fn disable_wal(self) -> Self {
        DbOption {
            use_wal: false,
            ..self
        }
    }

    /// When selecting the compaction level during major compaction, if there are no sstables with
    /// intersecting targets, the oldest sstables will be selected by default.
    pub fn major_default_oldest_table_num(self, major_default_oldest_table_num: usize) -> Self {
        DbOption {
            major_default_oldest_table_num,
            ..self
        }
    }
}

impl DbOption {
    pub(crate) fn table_path(&self, gen: &FileId) -> PathBuf {
        self.path.join(format!("{}.{}", gen, FileType::Parquet))
    }

    pub(crate) fn wal_dir_path(&self) -> PathBuf {
        self.path.join("wal")
    }

    pub(crate) fn wal_path(&self, gen: &FileId) -> PathBuf {
        self.wal_dir_path()
            .join(format!("{}.{}", gen, FileType::Wal))
    }

    pub(crate) fn version_path(&self) -> PathBuf {
        self.path.join(format!("version.{}", FileType::Log))
    }

    pub(crate) fn is_threshold_exceeded_major<R, E>(
        &self,
        version: &Version<R, E>,
        level: usize,
    ) -> bool
    where
        R: Record,
        E: FileProvider,
    {
        Version::<R, E>::tables_len(version, level)
            >= (self.major_threshold_with_sst_size * self.level_sst_magnification.pow(level as u32))
    }
}
