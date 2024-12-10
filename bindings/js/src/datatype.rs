use tonbo::record::Datatype;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum DataType {
    UInt8 = 0,
    UInt16 = 1,
    UInt32 = 2,
    UInt64 = 3,
    Int8 = 4,
    Int16 = 5,
    Int32 = 6,
    Int64 = 7,
    String = 8,
    Boolean = 9,
    Bytes = 10,
}

impl From<DataType> for Datatype {
    fn from(datatype: DataType) -> Self {
        match datatype {
            DataType::UInt8 => Datatype::UInt8,
            DataType::UInt16 => Datatype::UInt16,
            DataType::UInt32 => Datatype::UInt32,
            DataType::UInt64 => Datatype::UInt64,
            DataType::Int8 => Datatype::Int8,
            DataType::Int16 => Datatype::Int16,
            DataType::Int32 => Datatype::Int32,
            DataType::Int64 => Datatype::Int64,
            DataType::String => Datatype::String,
            DataType::Boolean => Datatype::Boolean,
            _ => todo!(),
        }
    }
}

pub(crate) fn to_datatype(datatype: &str) -> Datatype {
    match datatype {
        "UInt8" => Datatype::UInt8,
        "UInt16" => Datatype::UInt16,
        "UInt32" => Datatype::UInt32,
        "UInt64" => Datatype::UInt64,
        "Int8" => Datatype::Int8,
        "Int16" => Datatype::Int16,
        "Int32" => Datatype::Int32,
        "Int64" => Datatype::Int64,
        "String" => Datatype::String,
        "Boolean" => Datatype::Boolean,
        _ => todo!(),
    }
}
