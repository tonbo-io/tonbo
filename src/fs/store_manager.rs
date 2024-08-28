use std::{collections::HashMap, sync::Arc};

use object_store::{parse_url, path::Path, ObjectStore};
use thiserror::Error;
use url::Url;

pub struct StoreManager {
    stores: HashMap<Url, (Arc<dyn ObjectStore>, Path)>,
}

impl StoreManager {
    pub fn new(urls: &[Url]) -> Result<Self, object_store::Error> {
        let mut stores = HashMap::with_capacity(urls.len());

        for url in urls {
            if !stores.contains_key(url) {
                let (store, path) = parse_url(url)?;

                stores.insert(url.clone(), (Arc::from(store), path));
            }
        }

        Ok(StoreManager { stores })
    }

    pub fn get(&self, url: &Url) -> Option<&Arc<dyn ObjectStore>> {
        self.stores.get(url).map(|(store, _)| store)
    }
}

#[derive(Debug, Error)]
pub enum StoreManagerError {
    #[error("exceeds the maximum level(0-6)")]
    ExceedsMaxLevel,
    #[error("object store not found on level {0}")]
    StoreNotFound(usize),
}
