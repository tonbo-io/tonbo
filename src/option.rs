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
    compaction::{
        lazyleveled::LazyLeveledOptions,
        leveled::LeveledOptions, 
        tiered::TieredOptions,
    },
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

pub enum CompactionOption {
    Leveled(LeveledOptions),
    Tiered(TieredOptions),
    LazyLeveled(LazyLeveledOptions),
}

impl std::fmt::Debug for CompactionOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactionOption::Leveled(opts) => f.debug_tuple("Leveled").field(opts).finish(),
            CompactionOption::Tiered(opts) => f.debug_tuple("Tiered").field(opts).finish(),
            CompactionOption::LazyLeveled(opts) => f.debug_tuple("LazyLeveled").field(opts).finish(),
        }
    }
}

impl Clone for CompactionOption {
    fn clone(&self) -> Self {
        match self {
            CompactionOption::Leveled(opts) => CompactionOption::Leveled(opts.clone()),
            CompactionOption::Tiered(opts) => CompactionOption::Tiered(opts.clone()),
            CompactionOption::LazyLeveled(opts) => CompactionOption::LazyLeveled(opts.clone()),
        }
    }
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

    /// Optional custom paths and filesystem options for each level
    pub(crate) level_paths: Vec<Option<(Path, FsOptions)>>,

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

    /// Detailed options governing compaction behavior
    pub(crate) compaction_option: CompactionOption,
}


impl DbOption {
    /// build the default configured [`DbOption`] with base path and primary key
    pub fn new<S: Schema>(base_path: Path, schema: &S) -> Self {
        let (column_paths, sorting_columns) = schema.primary_key_path();

        DbOption {
            clean_channel_buffer: 10,
            base_path,
            max_sst_file_size: 256 * 1024 * 1024,
            write_parquet_properties: WriterProperties::builder()
                .set_compression(Compression::LZ4)
                .set_column_statistics_enabled(column_paths.clone(), EnabledStatistics::Page)
                .set_column_bloom_filter_enabled(column_paths.clone(), true)
                .set_sorting_columns(Some(sorting_columns))
                .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned())
                .build(),

            use_wal: true,
            wal_buffer_size: DEFAULT_WAL_BUFFER_SIZE,
            trigger_type: TriggerType::SizeOfMem(64 * 1024 * 1024),
            version_log_snapshot_threshold: 200,
            level_paths: vec![None; MAX_LEVEL],
            base_fs: FsOptions::Local,
            compaction_option: CompactionOption::Leveled(LeveledOptions::default()),
        }
    }

    /// build configured [`DbOption`] with base path, primary key, and custom compaction options
    pub fn new_with_options<S: Schema>(base_path: Path, schema: &S, compaction_option: CompactionOption) -> Self {
        let (column_paths, sorting_columns) = schema.primary_key_path();

        DbOption {
            clean_channel_buffer: 10,
            base_path,
            max_sst_file_size: 256 * 1024 * 1024,
            write_parquet_properties: WriterProperties::builder()
                .set_compression(Compression::LZ4)
                .set_column_statistics_enabled(column_paths.clone(), EnabledStatistics::Page)
                .set_column_bloom_filter_enabled(column_paths.clone(), true)
                .set_sorting_columns(Some(sorting_columns))
                .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned())
                .build(),

            use_wal: true,
            wal_buffer_size: DEFAULT_WAL_BUFFER_SIZE,
            trigger_type: TriggerType::SizeOfMem(64 * 1024 * 1024),
            version_log_snapshot_threshold: 200,
            level_paths: vec![None; MAX_LEVEL],
            base_fs: FsOptions::Local,
            compaction_option,
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

    /// Configure leveled compaction with custom options
    pub fn leveled_compaction(mut self, options: LeveledOptions) -> Self {
        self.compaction_option = CompactionOption::Leveled(options);
        self
    }

    /// Configure tiered compaction with custom options
    pub fn tiered_compaction(mut self, options: TieredOptions) -> Self {
        self.compaction_option = CompactionOption::Tiered(options);
        self
    }

    /// Configure lazy leveled compaction with custom options
    pub fn lazy_leveled_compaction(mut self, options: LazyLeveledOptions) -> Self {
        self.compaction_option = CompactionOption::LazyLeveled(options);
        self
    }

    /// Set major threshold with SST size (for leveled and lazy leveled compaction)
    pub fn major_threshold_with_sst_size(mut self, value: usize) -> Self {
        match &mut self.compaction_option {
            CompactionOption::Leveled(opts) => {
                opts.major_threshold_with_sst_size = value;
            },
            CompactionOption::LazyLeveled(opts) => {
                opts.major_threshold_with_sst_size = value;
            },
            _ => {} // No-op for other compaction types
        }
        self
    }

    /// Set level SST magnification (for leveled and lazy leveled compaction)
    pub fn level_sst_magnification(mut self, value: usize) -> Self {
        match &mut self.compaction_option {
            CompactionOption::Leveled(opts) => {
                opts.level_sst_magnification = value;
            },
            CompactionOption::LazyLeveled(opts) => {
                opts.level_sst_magnification = value;
            },
            _ => {} // No-op for other compaction types
        }
        self
    }

    /// Set maximum SST file size
    pub fn max_sst_file_size(mut self, value: usize) -> Self {
        self.max_sst_file_size = value;
        self
    }

    /// Set immutable chunk number
    pub fn immutable_chunk_num(mut self, value: usize) -> Self {
        match &mut self.compaction_option {
            CompactionOption::Leveled(opts) => opts.immutable_chunk_num = value,
            CompactionOption::Tiered(opts) => opts.immutable_chunk_num = value,
            CompactionOption::LazyLeveled(opts) => opts.immutable_chunk_num = value,
        }
        self
    }

    /// Set maximum immutable chunk number
    pub fn immutable_chunk_max_num(mut self, value: usize) -> Self {
        match &mut self.compaction_option {
            CompactionOption::Leveled(opts) => opts.immutable_chunk_max_num = value,
            CompactionOption::Tiered(opts) => opts.immutable_chunk_max_num = value,
            CompactionOption::LazyLeveled(opts) => opts.immutable_chunk_max_num = value,
        }
        self
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

    /// Set major default oldest table number (for leveled and lazy leveled compaction)
    pub fn major_default_oldest_table_num(mut self, value: usize) -> Self {
        match &mut self.compaction_option {
            CompactionOption::Leveled(opts) => {
                opts.major_default_oldest_table_num = value;
            },
            CompactionOption::LazyLeveled(opts) => {
                opts.major_default_oldest_table_num = value;
            },
            _ => panic!("major_default_oldest_table_num only applies to leveled-based compaction"),
        }
        self
    }

    /// Set maximum number of tiers (for tiered compaction only)
    pub fn max_tiers(mut self, value: usize) -> Self {
        match &mut self.compaction_option {
            CompactionOption::Tiered(opts) => opts.max_tiers = value,
            _ => panic!("max_tiers only applies to tiered compaction"),
        }
        self
    }

    /// Set tier base capacity (for tiered compaction only)
    pub fn tier_base_capacity(mut self, value: usize) -> Self {
        match &mut self.compaction_option {
            CompactionOption::Tiered(opts) => opts.tier_base_capacity = value,
            _ => panic!("tier_base_capacity only applies to tiered compaction"),
        }
        self
    }

    /// Set tier growth factor (for tiered compaction only)
    pub fn tier_growth_factor(mut self, value: usize) -> Self {
        match &mut self.compaction_option {
            CompactionOption::Tiered(opts) => opts.tier_growth_factor = value,
            _ => panic!("tier_growth_factor only applies to tiered compaction"),
        }
        self
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
            .field(
                "version_log_snapshot_threshold",
                &self.version_log_snapshot_threshold,
            )
            .field("trigger_type", &self.trigger_type)
            .field("use_wal", &self.use_wal)
            .field("max_sst_file_size", &self.max_sst_file_size)
            .field("wal_buffer_size", &self.wal_buffer_size)
            .field("write_parquet_properties", &self.write_parquet_properties)
            .field("compaction_option", &self.compaction_option)
            .finish()
    }
}
