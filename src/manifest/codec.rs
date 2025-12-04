use std::hash::Hash;

use serde::{Serialize, de::DeserializeOwned};

use crate::manifest::{
    ManifestError,
    domain::{CatalogKey, CatalogValue, GcPlanKey, GcPlanValue, VersionKey, VersionValue},
};

/// Trait describing the key/value serialization used for a manifest instance.
pub(crate) trait ManifestCodec {
    /// Key type stored inside `fusio-manifest`.
    type Key: Clone + Ord + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static;
    /// Value payload paired with each key.
    type Value: Clone + Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Ensure the provided key/value pair is well-formed for the codec.
    fn validate_key_value(key: &Self::Key, value: &Self::Value) -> Result<(), ManifestError>;
}

/// Marker codec binding the catalog key/value types together.

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CatalogCodec;

impl ManifestCodec for CatalogCodec {
    type Key = CatalogKey;
    type Value = CatalogValue;

    fn validate_key_value(_key: &Self::Key, _value: &Self::Value) -> Result<(), ManifestError> {
        Ok(())
    }
}

/// Marker codec binding the version key/value types together.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct VersionCodec;

impl ManifestCodec for VersionCodec {
    type Key = VersionKey;
    type Value = VersionValue;

    fn validate_key_value(key: &Self::Key, value: &Self::Value) -> Result<(), ManifestError> {
        match (key, value) {
            (VersionKey::TableHead { .. }, VersionValue::TableHead(_)) => Ok(()),
            (VersionKey::TableVersion { .. }, VersionValue::TableVersion(_)) => Ok(()),
            (VersionKey::WalFloor { .. }, VersionValue::WalFloor(_)) => Ok(()),
            _ => Err(ManifestError::Invariant("manifest key/value type mismatch")),
        }
    }
}

/// Marker codec binding the GC-plan key/value types together.

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct GcPlanCodec;

impl ManifestCodec for GcPlanCodec {
    type Key = GcPlanKey;
    type Value = GcPlanValue;

    fn validate_key_value(_key: &Self::Key, _value: &Self::Value) -> Result<(), ManifestError> {
        Ok(())
    }
}
