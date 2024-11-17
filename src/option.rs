use std::{
    fmt::{Debug, Formatter},
    marker::PhantomData,
};

use fusio::path::Path;
use fusio_dispatch::FsOptions;
use parquet::{
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
    format::SortingColumn,
    schema::types::ColumnPath,
};

use crate::{
    fs::{FileId, FileType},
    record::Record,
    trigger::TriggerType,
    version::{Version, MAX_LEVEL},
    DbError,
};

const DEFAULT_WAL_BUFFER_SIZE: usize = 4 * 1024;

/// configure the operating parameters of each component in the [`DB`](crate::DB)
#[derive(Clone)]
pub struct DbOption<R> {
    pub(crate) clean_channel_buffer: usize,
    pub(crate) base_path: Path,
    pub(crate) base_fs: FsOptions,
    // TODO: DEBUG
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
    _p: PhantomData<R>,
}

impl<R> DbOption<R>
where
    R: Record,
{
    /// build the default configured [`DbOption`] with base path and primary key
    pub fn with_path(base_path: Path, primary_key_name: String, primary_key_index: usize) -> Self {
        let (column_paths, sorting_columns) =
            Self::primary_key_path(primary_key_name, primary_key_index);

        DbOption {
            immutable_chunk_num: 3,
            immutable_chunk_max_num: 5,
            major_threshold_with_sst_size: 4,
            level_sst_magnification: 10,
            max_sst_file_size: 256 * 1024 * 1024,
            clean_channel_buffer: 10,
            base_path,
            write_parquet_properties: WriterProperties::builder()
                .set_compression(Compression::LZ4)
                .set_column_statistics_enabled(column_paths.clone(), EnabledStatistics::Page)
                .set_column_bloom_filter_enabled(column_paths.clone(), true)
                .set_sorting_columns(Some(sorting_columns))
                .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned())
                .build(),

            use_wal: true,
            wal_buffer_size: DEFAULT_WAL_BUFFER_SIZE,
            major_default_oldest_table_num: 3,
            major_l_selection_table_max_num: 4,
            trigger_type: TriggerType::SizeOfMem(64 * 1024 * 1024),
            _p: Default::default(),
            version_log_snapshot_threshold: 200,
            level_paths: vec![None; MAX_LEVEL],
            base_fs: FsOptions::Local,
        }
    }

    fn primary_key_path(
        primary_key_name: String,
        primary_key_index: usize,
    ) -> (ColumnPath, Vec<SortingColumn>) {
        (
            ColumnPath::new(vec!["_ts".to_string(), primary_key_name]),
            vec![
                SortingColumn::new(1_i32, true, true),
                SortingColumn::new(primary_key_index as i32, false, true),
            ],
        )
    }
}

impl<R> From<Path> for DbOption<R>
where
    R: Record,
{
    /// build the default configured [`DbOption`] based on the passed path
    fn from(base_path: Path) -> Self {
        let (column_paths, sorting_columns) = R::primary_key_path();
        DbOption {
            immutable_chunk_num: 3,
            immutable_chunk_max_num: 5,
            major_threshold_with_sst_size: 4,
            level_sst_magnification: 10,
            max_sst_file_size: 256 * 1024 * 1024,
            clean_channel_buffer: 10,
            base_path,
            base_fs: FsOptions::Local,
            write_parquet_properties: WriterProperties::builder()
                .set_compression(Compression::LZ4)
                .set_column_statistics_enabled(column_paths.clone(), EnabledStatistics::Page)
                .set_column_bloom_filter_enabled(column_paths.clone(), true)
                .set_sorting_columns(Some(sorting_columns))
                .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned())
                .build(),

            use_wal: true,
            wal_buffer_size: DEFAULT_WAL_BUFFER_SIZE,
            major_default_oldest_table_num: 3,
            major_l_selection_table_max_num: 4,
            trigger_type: TriggerType::SizeOfMem(64 * 1024 * 1024),
            _p: Default::default(),
            version_log_snapshot_threshold: 200,
            level_paths: vec![None; MAX_LEVEL],
        }
    }
}

impl<R> DbOption<R>
where
    R: Record,
{
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

    pub fn level_path(
        mut self,
        level: usize,
        path: Path,
        fs_options: FsOptions,
    ) -> Result<Self, DbError<R>> {
        if level >= MAX_LEVEL {
            Err(DbError::ExceedsMaxLevel)?;
        }
        self.level_paths[level] = Some((path, fs_options));
        Ok(self)
    }

    pub fn base_fs(mut self, base_fs: FsOptions) -> Self {
        self.base_fs = base_fs;
        self
    }
}

impl<R> DbOption<R>
where
    R: Record,
{
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

    pub(crate) fn is_threshold_exceeded_major(&self, version: &Version<R>, level: usize) -> bool {
        Version::<R>::tables_len(version, level)
            >= (self.major_threshold_with_sst_size * self.level_sst_magnification.pow(level as u32))
    }
}

impl<R> Debug for DbOption<R> {
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
