use std::path::PathBuf;

use parquet::file::properties::WriterProperties;

use crate::{
    fs::{FileId, FileProvider, FileType},
    record::Record,
    version::Version,
};

#[derive(Debug, Clone)]
pub struct DbOption {
    pub(crate) path: PathBuf,
    pub(crate) max_mem_table_size: usize,
    pub(crate) immutable_chunk_num: usize,
    pub(crate) major_threshold_with_sst_size: usize,
    pub(crate) level_sst_magnification: usize,
    pub(crate) max_sst_file_size: usize,
    pub(crate) clean_channel_buffer: usize,
    pub(crate) write_parquet_option: Option<WriterProperties>,

    #[allow(unused)]
    pub(crate) use_wal: bool,
}

impl<P> From<P> for DbOption
where
    P: Into<PathBuf>,
{
    fn from(path: P) -> Self {
        DbOption {
            path: path.into(),
            max_mem_table_size: 3000,
            immutable_chunk_num: 3,
            major_threshold_with_sst_size: 10,
            level_sst_magnification: 10,
            max_sst_file_size: 24 * 1024 * 1024,
            clean_channel_buffer: 10,
            write_parquet_option: None,

            use_wal: true,
        }
    }
}

impl DbOption {
    pub fn path(self, path: impl Into<PathBuf>) -> Self {
        DbOption {
            path: path.into(),
            ..self
        }
    }

    pub fn max_mem_table_size(self, max_mem_table_size: usize) -> Self {
        DbOption {
            max_mem_table_size,
            ..self
        }
    }

    pub fn immutable_chunk_num(self, immutable_chunk_num: usize) -> Self {
        DbOption {
            immutable_chunk_num,
            ..self
        }
    }

    pub fn major_threshold_with_sst_size(self, major_threshold_with_sst_size: usize) -> Self {
        DbOption {
            major_threshold_with_sst_size,
            ..self
        }
    }

    pub fn level_sst_magnification(self, level_sst_magnification: usize) -> Self {
        DbOption {
            level_sst_magnification,
            ..self
        }
    }

    pub fn max_sst_file_size(self, max_sst_file_size: usize) -> Self {
        DbOption {
            max_sst_file_size,
            ..self
        }
    }

    pub fn clean_channel_buffer(self, clean_channel_buffer: usize) -> Self {
        DbOption {
            clean_channel_buffer,
            ..self
        }
    }

    pub fn write_parquet_option(self, write_parquet_option: WriterProperties) -> Self {
        DbOption {
            write_parquet_option: Some(write_parquet_option),
            ..self
        }
    }

    pub fn use_wal(self, use_wal: bool) -> Self {
        DbOption { use_wal, ..self }
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
