use std::{any::Any, sync::Arc};

use super::{Key, KeyRef, Value};
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

    fn size_of(&self) -> usize {
        1
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
}

impl Value for Option<bool> {
    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn size_of(&self) -> usize {
        2
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
}
