use std::sync::Arc;

use super::Value;
use crate::datatype::DataType;

pub type LargeBinary = Vec<u8>;

impl Value for Vec<u8> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> crate::datatype::DataType {
        DataType::Bytes
    }

    fn size_of(&self) -> usize {
        self.len()
    }

    fn is_none(&self) -> bool {
        false
    }

    fn is_some(&self) -> bool {
        false
    }

    fn clone_arc(&self) -> super::ValueRef {
        Arc::new(self.clone())
    }
}

impl Value for Option<Vec<u8>> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Bytes
    }

    fn size_of(&self) -> usize {
        match self {
            Some(v) => 1 + v.len(),
            None => 1,
        }
    }

    fn is_none(&self) -> bool {
        self.is_none()
    }

    fn is_some(&self) -> bool {
        self.is_some()
    }

    fn clone_arc(&self) -> super::ValueRef {
        Arc::new(self.clone())
    }
}
