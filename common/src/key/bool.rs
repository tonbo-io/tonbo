use std::{any::Any, sync::Arc};

use arrow::array::BooleanArray;

use super::{Key, KeyRef, PrimaryKey, Value};
use crate::datatype::DataType;

impl Key for bool {
    type Ref<'r> = bool;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        *self
    }

    fn as_value(&self) -> &dyn Value {
        self
    }
}

impl<'r> KeyRef<'r> for bool {
    type Key = bool;

    fn to_key(self) -> Self::Key {
        self
    }
}

impl Value for bool {
    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_none(&self) -> bool {
        false
    }

    fn is_some(&self) -> bool {
        false
    }

    fn clone_arc(&self) -> super::ValueRef {
        Arc::new(*self)
    }

    fn to_arrow_datum(&self) -> Option<Arc<dyn arrow::array::Datum>> {
        Some(Arc::new(BooleanArray::new_scalar(*self)))
    }
}

impl Value for Option<bool> {
    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_none(&self) -> bool {
        self.is_none()
    }

    fn is_some(&self) -> bool {
        self.is_some()
    }

    fn clone_arc(&self) -> super::ValueRef {
        Arc::new(*self)
    }

    fn to_arrow_datum(&self) -> Option<Arc<dyn arrow::array::Datum>> {
        None
    }
}

impl From<bool> for PrimaryKey {
    fn from(value: bool) -> Self {
        PrimaryKey::new(vec![Arc::new(value)])
    }
}
