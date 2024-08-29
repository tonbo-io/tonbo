use std::{marker::PhantomData, path::PathBuf, sync::Arc};

use object_store::ObjectStore;
use parquet::{
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};
use url::Url;

use crate::{
    fs::{
        store_manager::{StoreManager, StoreManagerError},
        FileId, FileProvider, FileType,
    },
    record::Record,
    trigger::TriggerType,
    version::{Version, MAX_LEVEL},
    DbError,
};

/// configure the operating parameters of each component in the [`DB`](crate::DB)
#[derive(Debug, Clone)]
pub struct DbOption<R> {
    pub(crate) wal_path: PathBuf,
    pub(crate) table_urls: Vec<(Url, Option<Arc<dyn ObjectStore>>)>,
    pub(crate) immutable_chunk_num: usize,
    pub(crate) immutable_chunk_max_num: usize,
    pub(crate) major_threshold_with_sst_size: usize,
    pub(crate) level_sst_magnification: usize,
    pub(crate) max_sst_file_size: usize,
    pub(crate) clean_channel_buffer: usize,
    pub(crate) write_parquet_properties: WriterProperties,
    pub(crate) use_wal: bool,
    pub(crate) major_default_oldest_table_num: usize,
    pub(crate) major_l_selection_table_max_num: usize,
    pub(crate) trigger_type: TriggerType,
    _p: PhantomData<R>,
}

impl<R> TryFrom<PathBuf> for DbOption<R>
where
    R: Record,
{
    type Error = DbError<R>;

    /// build the default configured [`DbOption`] based on the passed path
    fn try_from(base_path: PathBuf) -> Result<Self, Self::Error> {
        let (column_paths, sorting_columns) = R::primary_key_path();
        let table_url = Url::from_directory_path(&base_path)
            .map_err(|_| DbError::UrlParse(base_path.to_string_lossy().to_string()))?;

        Ok(DbOption {
            wal_path: base_path,
            table_urls: vec![(table_url, None); MAX_LEVEL],
            immutable_chunk_num: 3,
            immutable_chunk_max_num: 5,
            major_threshold_with_sst_size: 4,
            level_sst_magnification: 10,
            max_sst_file_size: 256 * 1024 * 1024,
            clean_channel_buffer: 10,
            write_parquet_properties: WriterProperties::builder()
                .set_compression(Compression::LZ4)
                .set_column_statistics_enabled(column_paths.clone(), EnabledStatistics::Page)
                .set_column_bloom_filter_enabled(column_paths.clone(), true)
                .set_sorting_columns(Some(sorting_columns))
                .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned())
                .build(),

            use_wal: true,
            major_default_oldest_table_num: 3,
            major_l_selection_table_max_num: 4,
            trigger_type: TriggerType::SizeOfMem(64 * 1024 * 1024),
            _p: Default::default(),
        })
    }
}

impl<R> DbOption<R>
where
    R: Record,
{
    /// build the [`DB`](crate::DB) storage directory based on the passed path
    pub fn path(self, path: impl Into<PathBuf>) -> Self {
        DbOption {
            wal_path: path.into(),
            ..self
        }
    }

    pub fn level_url(
        mut self,
        level: usize,
        url: Url,
        store: Option<Arc<dyn ObjectStore>>,
    ) -> Result<Self, DbError<R>> {
        if level >= MAX_LEVEL {
            Err(StoreManagerError::ExceedsMaxLevel)?;
        }
        self.table_urls[level] = (url, store);
        Ok(self)
    }

    pub fn all_level_url(mut self, url: Url, store: Option<Arc<dyn ObjectStore>>) -> Self {
        for table_url in self.table_urls.iter_mut() {
            *table_url = (url.clone(), store.clone());
        }
        self
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

    /// When selecting the compaction level during major compaction, if there are no sstables with
    /// intersecting targets, the oldest sstables will be selected by default.
    pub fn major_default_oldest_table_num(self, major_default_oldest_table_num: usize) -> Self {
        DbOption {
            major_default_oldest_table_num,
            ..self
        }
    }
}

impl<R> DbOption<R>
where
    R: Record,
{
    pub(crate) fn level_store<'a>(
        &'a self,
        level: usize,
        store_manager: &'a StoreManager,
    ) -> Result<&'a Arc<dyn ObjectStore>, StoreManagerError> {
        if level >= MAX_LEVEL {
            return Err(StoreManagerError::ExceedsMaxLevel);
        }
        let (url, default_store) = &self.table_urls[level];

        if let Some(store) = default_store {
            return Ok(store);
        }
        store_manager
            .get(url)
            .ok_or(StoreManagerError::StoreNotFound(level))
    }

    pub(crate) fn table_path(&self, gen: &FileId) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}.{}", gen, FileType::Parquet))
    }

    pub(crate) fn wal_dir_path(&self) -> PathBuf {
        self.wal_path.join("wal")
    }

    pub(crate) fn wal_path(&self, gen: &FileId) -> PathBuf {
        self.wal_dir_path()
            .join(format!("{}.{}", gen, FileType::Wal))
    }

    pub(crate) fn version_path(&self) -> PathBuf {
        self.wal_path.join(format!("version.{}", FileType::Log))
    }

    pub(crate) fn is_threshold_exceeded_major<E>(
        &self,
        version: &Version<R, E>,
        level: usize,
    ) -> bool
    where
        E: FileProvider,
    {
        Version::<R, E>::tables_len(version, level)
            >= (self.major_threshold_with_sst_size * self.level_sst_magnification.pow(level as u32))
    }
}
