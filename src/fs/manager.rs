use std::{collections::HashMap, sync::Arc};

use fusio::{disk::LocalFs, dynamic::DynFs, fs::FileSystemTag, path::Path, Error};
use fusio_dispatch::FsOptions;

pub struct StoreManager {
    base_fs: Arc<dyn DynFs>,
    local_fs: Arc<dyn DynFs>,
    fs_map: HashMap<Path, Arc<dyn DynFs>>,
    cache_map: HashMap<Path, Arc<dyn DynFs>>,
}

impl StoreManager {
    pub fn new(
        base_options: FsOptions,
        levels_fs: Vec<Option<(Path, FsOptions, bool)>>,
    ) -> Result<Self, Error> {
        let mut fs_map = HashMap::with_capacity(levels_fs.len());
        let mut cache_map: HashMap<Path, Arc<dyn DynFs>> = HashMap::with_capacity(levels_fs.len());
        for (path, fs_options, enable_local_cache) in levels_fs.into_iter().flatten() {
            let fs = fs_options.parse()?;
            if enable_local_cache && fs.file_system() == FileSystemTag::S3 {
                cache_map
                    .entry(path.clone())
                    .or_insert(Arc::new(LocalFs {}));
            }
            fs_map.entry(path).or_insert(fs);
        }
        let base_fs = base_options.parse()?;
        Ok(StoreManager {
            base_fs,
            fs_map,
            cache_map,
            local_fs: Arc::new(LocalFs {}),
        })
    }

    pub fn base_fs(&self) -> &Arc<dyn DynFs> {
        &self.base_fs
    }

    pub fn local_fs(&self) -> &Arc<dyn DynFs> {
        &self.local_fs
    }

    pub fn get_fs(&self, path: &Path) -> &Arc<dyn DynFs> {
        self.fs_map.get(path).unwrap_or(&self.base_fs)
    }

    pub fn get_cache(&self, path: &Path) -> &Arc<dyn DynFs> {
        self.cache_map
            .get(path)
            .unwrap_or(self.fs_map.get(path).unwrap_or(&self.base_fs))
    }
}

// TODO: TestCases
