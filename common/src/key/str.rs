use std::{any::Any, sync::Arc};

use arrow::array::{Datum, StringArray};

use super::{Key, KeyRef, Value};
use crate::datatype::DataType;

pub type LargeString = String;

impl Key for String {
    type Ref<'r> = &'r str;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        self
    }

    fn to_arrow_datum(&self) -> Arc<dyn Datum> {
        Arc::new(StringArray::new_scalar(self))
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

    fn size_of(&self) -> usize {
        self.len()
    }

    fn to_arrow_datum(&self) -> Arc<dyn Datum> {
        Arc::new(StringArray::new_scalar(self))
    }

    fn is_none(&self) -> bool {
        false
    }

    fn is_some(&self) -> bool {
        false
    }
}
