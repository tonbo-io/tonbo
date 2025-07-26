use tonbo::record::{DynamicField, Value};
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

use crate::utils::parse_key;

#[wasm_bindgen]
pub struct Bound {
    inner: BoundInner,
}

enum BoundInner {
    Included(JsValue),
    Exculuded(JsValue),
    Unbounded,
}

#[wasm_bindgen]
impl Bound {
    /// represent including bound of range, null or undefined are identical to [`Bound::unbounded`]
    pub fn included(key: JsValue) -> Self {
        if key.is_null() || key.is_undefined() {
            return Self {
                inner: BoundInner::Unbounded,
            };
        }
        Self {
            inner: BoundInner::Included(key),
        }
    }

    /// represent exclusive bound of range, null or undefined are identical to [`Bound::unbounded`]
    pub fn excluded(key: JsValue) -> Self {
        if key.is_null() || key.is_undefined() {
            return Self {
                inner: BoundInner::Unbounded,
            };
        }
        Self {
            inner: BoundInner::Exculuded(key),
        }
    }

    pub fn unbounded() -> Self {
        Self {
            inner: BoundInner::Unbounded,
        }
    }
}

impl Bound {
    pub(crate) fn into_bound(self, desc: &DynamicField) -> Result<std::ops::Bound<Value>, JsValue> {
        Ok(match self.inner {
            BoundInner::Included(key) => std::ops::Bound::Included(parse_key(desc, key, true)?),
            BoundInner::Exculuded(key) => std::ops::Bound::Excluded(parse_key(desc, key, true)?),
            BoundInner::Unbounded => std::ops::Bound::Unbounded,
        })
    }
}
