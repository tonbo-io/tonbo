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
    Float32 = 11,
    Float64 = 12,
}

impl From<DataType> for tonbo::datatype::DataType {
    fn from(datatype: DataType) -> Self {
        match datatype {
            DataType::UInt8 => tonbo::datatype::DataType::UInt8,
            DataType::UInt16 => tonbo::datatype::DataType::UInt16,
            DataType::UInt32 => tonbo::datatype::DataType::UInt32,
            DataType::UInt64 => tonbo::datatype::DataType::UInt64,
            DataType::Int8 => tonbo::datatype::DataType::Int8,
            DataType::Int16 => tonbo::datatype::DataType::Int16,
            DataType::Int32 => tonbo::datatype::DataType::Int32,
            DataType::Int64 => tonbo::datatype::DataType::Int64,
            DataType::String => tonbo::datatype::DataType::String,
            DataType::Boolean => tonbo::datatype::DataType::Boolean,
            DataType::Float32 => tonbo::datatype::DataType::Float32,
            DataType::Float64 => tonbo::datatype::DataType::Float64,
            _ => todo!(),
        }
    }
}

pub(crate) fn to_datatype(datatype: &str) -> tonbo::datatype::DataType {
    match datatype {
        "UInt8" => tonbo::datatype::DataType::UInt8,
        "UInt16" => tonbo::datatype::DataType::UInt16,
        "UInt32" => tonbo::datatype::DataType::UInt32,
        "UInt64" => tonbo::datatype::DataType::UInt64,
        "Int8" => tonbo::datatype::DataType::Int8,
        "Int16" => tonbo::datatype::DataType::Int16,
        "Int32" => tonbo::datatype::DataType::Int32,
        "Int64" => tonbo::datatype::DataType::Int64,
        "String" => tonbo::datatype::DataType::String,
        "Boolean" => tonbo::datatype::DataType::Boolean,
        "Float32" => tonbo::datatype::DataType::Float32,
        "Float64" => tonbo::datatype::DataType::Float64,
        _ => todo!(),
    }
}
