use std::{any::Any, sync::Arc};

use arrow::array::StringArray;

use super::{Key, KeyRef, PrimaryKey, Value, ValueRef};
use crate::datatype::DataType;

pub type LargeString = String;

impl Key for String {
    type Ref<'r> = &'r str;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        self
    }

    fn as_value(&self) -> &dyn Value {
        self
    }
}

impl<'r> KeyRef<'r> for &'r str {
    type Key = String;

    fn to_key(self) -> Self::Key {
        self.to_string()
    }
}

impl Value for String {
    fn data_type(&self) -> DataType {
        DataType::String
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

    fn clone_arc(&self) -> ValueRef {
        Arc::new(self.clone())
    }

    fn to_arrow_datum(&self) -> Option<Arc<dyn arrow::array::Datum>> {
        Some(Arc::new(StringArray::new_scalar(self.clone())))
    }
}

impl Value for Option<String> {
    fn data_type(&self) -> DataType {
        DataType::String
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

    fn clone_arc(&self) -> ValueRef {
        Arc::new(self.clone())
    }

    fn to_arrow_datum(&self) -> Option<Arc<dyn arrow::array::Datum>> {
        None
    }
}

impl From<String> for PrimaryKey {
    fn from(value: String) -> Self {
        PrimaryKey::new(vec![Arc::new(value)])
    }
}

impl From<&str> for PrimaryKey {
    fn from(value: &str) -> Self {
        PrimaryKey::new(vec![Arc::new(value.to_string())])
    }
}
