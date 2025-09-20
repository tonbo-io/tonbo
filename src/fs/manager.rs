use std::{collections::HashMap, sync::Arc};

use fusio::{disk::LocalFs, dynamic::DynFs, path::Path, Error};
use fusio_dispatch::FsOptions;


/// Manages which filesystem to use for a given logical path.
///
/// - `base_fs` is the default FS used when no per-level override exists.
/// - `local_fs` is intended for temp/local operations (always a `LocalFs`).
/// - `fs_map` holds explicit per-path overrides. **Exact match only**; this is
///   not a prefix tree and won’t match parent directories.
///
/// # Examples
/// ```no_run
/// # use std::sync::Arc;
/// # use fusio::{path::Path};
/// # use fusio_dispatch::FsOptions;
/// # use tonbo::StoreManager;
/// 
/// let base = FsOptions::from_env("BASE")?;
/// let l0   = FsOptions::from_env("L0")?;
///
/// let mgr = StoreManager::new(
///     base,
///     vec![Some((Path::from("/level-0"), l0))],
/// )?;
///
/// // Exact-match lookup; falls back to base FS for other paths
/// let fs = mgr.get_fs(&Path::from("/level-0"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct StoreManager {
    base_fs: Arc<dyn DynFs>,
    local_fs: Arc<dyn DynFs>,
    fs_map: HashMap<Path, Arc<dyn DynFs>>,
}

impl StoreManager {
    /// Create a new manager.
    ///
    /// - `base_options` builds the default filesystem.
    /// - `levels_fs` is a sparse list of optional `(Path, FsOptions)` overrides.
    ///
    /// Returns an error if any `FsOptions::parse()` fails.
    pub fn new(
        base_options: FsOptions,
        levels_fs: Vec<Option<(Path, FsOptions)>>,
    ) -> Result<Self, Error> {
        let mut fs_map = HashMap::with_capacity(levels_fs.len());

        for (path, fs_options) in levels_fs.into_iter().flatten() {
            fs_map.entry(path).or_insert(fs_options.parse()?);
        }
        let base_fs = base_options.parse()?;

        Ok(StoreManager {
            base_fs,
            fs_map,
            local_fs: Arc::new(LocalFs {}),
        })
    }

    /// Returns the base/default filesystem (used as fallback).
    #[inline]
    pub fn base_fs(&self) -> &Arc<dyn DynFs> {
        &self.base_fs
    }

    /// Returns the local filesystem helper (typically a `LocalFs`).
    #[inline]
    pub fn local_fs(&self) -> &Arc<dyn DynFs> {
        &self.local_fs
    }

    /// Returns the filesystem for an exact path match,
    /// or the base filesystem if no override exists.
    #[inline]
    pub fn get_fs(&self, path: &Path) -> &Arc<dyn DynFs> {
        self.fs_map.get(path).unwrap_or(&self.base_fs)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    /// Helper to create a manager without calling external `FsOptions::parse()`.
    /// Fine for white-box tests inside this module.
    fn make_manager_with_map(
        base_fs: Arc<dyn DynFs>,
        local_fs: Arc<dyn DynFs>,
        fs_map: HashMap<Path, Arc<dyn DynFs>>,
    ) -> StoreManager {
        StoreManager { base_fs, local_fs, fs_map }
    }

    #[test]
    fn get_fs_returns_override_on_exact_match() {
        let base_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let local_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let override_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});

        let key = Path::from("/level-0");
        let mut map = HashMap::new();
        map.insert(key.clone(), override_fs.clone());

        let mgr = make_manager_with_map(base_fs.clone(), local_fs.clone(), map);

        // Exact key hits override
        let got = mgr.get_fs(&key);
        assert!(Arc::ptr_eq(got, &override_fs));

        // Non-matching key falls back to base
        let other = Path::from("/other");
        let got_other = mgr.get_fs(&other);
        assert!(Arc::ptr_eq(got_other, &base_fs));
    }

    #[test]
    fn base_and_local_accessors_return_same_arc() {
        let base_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let local_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let mgr = make_manager_with_map(base_fs.clone(), local_fs.clone(), HashMap::new());

        assert!(Arc::ptr_eq(mgr.base_fs(), &base_fs));
        assert!(Arc::ptr_eq(mgr.local_fs(), &local_fs));
    }

    #[test]
    fn override_does_not_prefix_match() {
        // Shows the lookup is an exact key, not a directory-prefix match.
        let base_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let local_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
        let override_fs: Arc<dyn DynFs> = Arc::new(LocalFs {});

        let key = Path::from("/data");
        let mut map = HashMap::new();
        map.insert(key.clone(), override_fs.clone());

        let mgr = make_manager_with_map(base_fs.clone(), local_fs.clone(), map);

        // "/data" hits override
        assert!(Arc::ptr_eq(mgr.get_fs(&Path::from("/data")), &override_fs));

        // "/data/part-0000" does NOT hit unless the exact key exists
        assert!(Arc::ptr_eq(mgr.get_fs(&Path::from("/data/part-0000")), &base_fs));
    }
}