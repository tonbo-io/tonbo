use std::{collections::HashMap, sync::Arc};

use object_store::{parse_url, ObjectStore};
use thiserror::Error;
use url::Url;

pub struct StoreManager {
    stores: HashMap<Url, Arc<dyn ObjectStore>>,
}

impl StoreManager {
    pub fn new(urls: &[(Url, Option<Arc<dyn ObjectStore>>)]) -> Result<Self, object_store::Error> {
        let mut stores = HashMap::with_capacity(urls.len());

        for (url, default_store) in urls {
            if !stores.contains_key(url) {
                let store = if let Some(store) = default_store.clone() {
                    store
                } else {
                    parse_url(url).map(|(store, _)| Arc::from(store))?
                };

                stores.insert(url.clone(), store);
            }
        }

        Ok(StoreManager { stores })
    }

    pub fn get(&self, url: &Url) -> Option<&Arc<dyn ObjectStore>> {
        self.stores.get(url)
    }
}

#[derive(Debug, Error)]
pub enum StoreManagerError {
    #[error("exceeds the maximum level(0-6)")]
    ExceedsMaxLevel,
    #[error("object store not found on level {0}")]
    StoreNotFound(usize),
}
