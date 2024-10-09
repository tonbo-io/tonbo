use std::{collections::HashMap, sync::Arc};

use fusio::{dynamic::DynFs, options::FsOptions, path::Path, Error};

pub struct StoreManager {
    base_fs: Arc<dyn DynFs>,
    fs_map: HashMap<Path, Arc<dyn DynFs>>,
}

impl StoreManager {
    pub fn new(
        base_options: FsOptions,
        levels_fs: Vec<Option<(Path, FsOptions)>>,
    ) -> Result<Self, Error> {
        let mut fs_map = HashMap::with_capacity(levels_fs.len());

        for (path, fs_options) in levels_fs.into_iter().flatten() {
            fs_map.entry(path).or_insert(fs_options.parse()?);
        }
        let base_fs = base_options.parse()?;

        Ok(StoreManager { base_fs, fs_map })
    }

    pub async fn create_dir_all(&self, path: &Path) -> Result<(), Error> {
        self.base_fs.create_dir_all(path).await?;
        for (_, fs) in self.fs_map.iter() {
            fs.create_dir_all(path).await?;
        }

        Ok(())
    }

    pub fn base_fs(&self) -> &Arc<dyn DynFs> {
        &self.base_fs
    }

    pub fn get_fs(&self, path: &Path) -> &Arc<dyn DynFs> {
        self.fs_map.get(path).unwrap_or(&self.base_fs)
    }
}

// TODO: TestCases
