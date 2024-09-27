use std::{collections::HashMap, sync::Arc};

use fusio::{dynamic::DynFs, path::Path, Error};

pub struct StoreManager {
    base_fs: Arc<dyn DynFs>,
    fs_map: HashMap<Path, Option<Arc<dyn DynFs>>>,
}

impl StoreManager {
    pub fn new(base_fs: Arc<dyn DynFs>, levels_fs: Vec<(Path, Option<Arc<dyn DynFs>>)>) -> Self {
        let mut fs_map = HashMap::with_capacity(levels_fs.len());

        for (path, fs) in levels_fs {
            fs_map.entry(path).or_insert(fs);
        }

        StoreManager { base_fs, fs_map }
    }

    pub async fn create_dir_all(&self, path: &Path) -> Result<(), Error> {
        self.base_fs.create_dir_all(path).await?;
        for (_, fs) in self.fs_map.iter() {
            if let Some(fs) = fs {
                fs.create_dir_all(path).await?;
            }
        }

        Ok(())
    }

    pub fn base_fs(&self) -> &Arc<dyn DynFs> {
        &self.base_fs
    }

    pub fn get_fs(&self, path: &Path) -> &Arc<dyn DynFs> {
        self.fs_map
            .get(path)
            .and_then(Option::as_ref)
            .unwrap_or(&self.base_fs)
    }
}

// TODO: TestCases
