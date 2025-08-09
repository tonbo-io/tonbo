use std::fmt::{Debug, Formatter};

pub use fusio::path::Path;
#[cfg(feature = "aws")]
pub use fusio::remotes::aws::AwsCredential;
pub use fusio_dispatch::FsOptions;
use parquet::{
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};
use thiserror::Error;

use crate::{
    fs::{FileId, FileType},
    record::Schema,
    trigger::TriggerType,
    version::MAX_LEVEL,
};

const DEFAULT_WAL_BUFFER_SIZE: usize = 4 * 1024;

/// Specifies the ordering direction for scans and other operations
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub enum Order {
    /// Ascending order (default)
    #[default]
    Asc,
    /// Descending order
    Desc,
}

#[derive(Clone)]
pub enum CompactionOption {
    Leveled,
}

/// Configure the operating parameters of each component in the [`DB`](crate::DB)
#[derive(Clone)]
pub struct DbOption {
    /// Number of entries to buffer before cleaning operations
    pub(crate) clean_channel_buffer: usize,

    /// Base directory for database files
    pub(crate) base_path: Path,

    /// Filesystem options for the base path
    pub(crate) base_fs: FsOptions,

    /// Detailed options governing compaction behavior
    pub(crate) compaction_option: CompactionOption,

    /// Optional custom paths and filesystem options for each level
    pub(crate) level_paths: Vec<Option<(Path, FsOptions)>>,

    /// Number of immutable chunks to accumulate before triggering a flush
    pub(crate) immutable_chunk_num: usize,

    /// Maximum allowed number of immutable chunks in memory
    pub(crate) immutable_chunk_max_num: usize,

    /// Magnification factor controlling SST file count per level
    pub(crate) level_sst_magnification: usize,

    /// Default number of oldest tables to include in a major compaction
    pub(crate) major_default_oldest_table_num: usize,

    /// Maximum number of tables to select for major compaction at level L
    pub(crate) major_l_selection_table_max_num: usize,

    /// Size threshold (in bytes) to trigger major compaction relative to SST size
    pub(crate) major_threshold_with_sst_size: usize,

    /// Maximum allowed size (in bytes) for a single SST file
    pub(crate) max_sst_file_size: usize,

    /// Version count after which a snapshot is taken of the version log
    pub(crate) version_log_snapshot_threshold: u32,

    /// Trigger type used for individual memtables before being flushed
    pub(crate) trigger_type: TriggerType,

    /// Flag for deciding wehther to use a write-ahead log for durability
    pub(crate) use_wal: bool,

    /// Buffer size (in bytes) for the write-ahead log
    pub(crate) wal_buffer_size: usize,

    /// Parquet writer properties for on-disk SST files
    pub(crate) write_parquet_properties: WriterProperties,
}

impl DbOption {
    /// build the default configured [`DbOption`] with base path and primary key
    pub fn new<S: Schema>(base_path: Path, schema: &S) -> Self {
        let (column_paths, sorting_columns) = schema.primary_key_paths_and_sorting();

        // Configure Parquet writer properties for all PK columns and sorting order.
        let mut writer_builder = WriterProperties::builder()
            .set_compression(Compression::LZ4)
            .set_sorting_columns(Some(sorting_columns.to_vec()))
            .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned());

        for path in column_paths.iter().cloned() {
            writer_builder = writer_builder
                .set_column_statistics_enabled(path.clone(), EnabledStatistics::Page)
                .set_column_bloom_filter_enabled(path, true);
        }

        DbOption {
            immutable_chunk_num: 3,
            immutable_chunk_max_num: 5,
            major_threshold_with_sst_size: 4,
            level_sst_magnification: 10,
            max_sst_file_size: 256 * 1024 * 1024,
            clean_channel_buffer: 10,
            base_path,
            write_parquet_properties: writer_builder.build(),

            use_wal: true,
            wal_buffer_size: DEFAULT_WAL_BUFFER_SIZE,
            major_default_oldest_table_num: 3,
            major_l_selection_table_max_num: 4,
            trigger_type: TriggerType::SizeOfMem(64 * 1024 * 1024),
            version_log_snapshot_threshold: 200,
            level_paths: vec![None; MAX_LEVEL],
            base_fs: FsOptions::Local,
            compaction_option: CompactionOption::Leveled,
        }
    }
}

impl DbOption {
    /// build the [`DB`](crate::DB) storage directory based on the passed path
    pub fn path(self, path: impl Into<Path>) -> Self {
        DbOption {
            base_path: path.into(),
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
    pub fn write_parquet_option(self, write_parquet_properties: WriterProperties) -> Self {
        DbOption {
            write_parquet_properties,
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

    /// Maximum size of WAL buffer, default value is 4KB
    ///
    /// Set to 0 to disable WAL buffer
    pub fn wal_buffer_size(self, wal_buffer_size: usize) -> Self {
        DbOption {
            wal_buffer_size,
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

    /// VersionLog will use version_log_snapshot_threshold as the cycle to SnapShot to reduce the
    /// size.
    pub fn version_log_snapshot_threshold(self, version_log_snapshot_threshold: u32) -> Self {
        DbOption {
            version_log_snapshot_threshold,
            ..self
        }
    }
    /// set the path where files will be stored in the level.
    pub fn level_path(
        mut self,
        level: usize,
        path: Path,
        fs_options: FsOptions,
    ) -> Result<Self, ExceedsMaxLevel> {
        if level >= MAX_LEVEL {
            return Err(ExceedsMaxLevel);
        }
        self.level_paths[level] = Some((path, fs_options));
        Ok(self)
    }

    /// set the base path option.
    ///
    /// This will be the default option for all wal, manifest and SSTables. Use
    /// [`DbOption::level_path`] to set the option for SStables.
    pub fn base_fs(mut self, base_fs: FsOptions) -> Self {
        self.base_fs = base_fs;
        self
    }

    pub fn compaction_option(self, compaction_option: CompactionOption) -> Self {
        Self {
            compaction_option,
            ..self
        }
    }
}

#[derive(Debug, Error)]
#[error("exceeds max level, max level is {}", MAX_LEVEL)]
pub struct ExceedsMaxLevel;

impl DbOption {
    pub(crate) fn table_path(&self, gen: FileId, level: usize) -> Path {
        self.level_paths[level]
            .as_ref()
            .map(|(path, _)| path)
            .unwrap_or(&self.base_path)
            .child(format!("{}.{}", gen, FileType::Parquet))
    }

    pub(crate) fn wal_dir_path(&self) -> Path {
        self.base_path.child("wal")
    }

    pub(crate) fn wal_path(&self, gen: FileId) -> Path {
        self.wal_dir_path()
            .child(format!("{}.{}", gen, FileType::Wal))
    }

    pub(crate) fn version_log_dir_path(&self) -> Path {
        self.base_path.child("version")
    }

    pub(crate) fn version_log_path(&self, gen: FileId) -> Path {
        self.version_log_dir_path()
            .child(format!("{}.{}", gen, FileType::Log))
    }

    pub(crate) fn level_fs_path(&self, level: usize) -> Option<&Path> {
        self.level_paths[level].as_ref().map(|(path, _)| path)
    }
}

impl Debug for DbOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbOption")
            .field("clean_channel_buffer", &self.clean_channel_buffer)
            .field("base_path", &self.base_path)
            // TODO
            // .field("level_paths", &self.level_paths)
            .field("immutable_chunk_num", &self.immutable_chunk_num)
            .field("immutable_chunk_max_num", &self.immutable_chunk_max_num)
            .field("level_sst_magnification", &self.level_sst_magnification)
            .field(
                "major_default_oldest_table_num",
                &self.major_default_oldest_table_num,
            )
            .field(
                "major_l_selection_table_max_num",
                &self.major_l_selection_table_max_num,
            )
            .field(
                "major_threshold_with_sst_size",
                &self.major_threshold_with_sst_size,
            )
            .field("max_sst_file_size", &self.max_sst_file_size)
            .field(
                "version_log_snapshot_threshold",
                &self.version_log_snapshot_threshold,
            )
            .field("trigger_type", &self.trigger_type)
            .field("use_wal", &self.use_wal)
            .field("write_parquet_properties", &self.write_parquet_properties)
            .finish()
    }
}
