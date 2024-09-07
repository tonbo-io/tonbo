pub(crate) enum DataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    Boolean,
}

impl DataType {
    pub(crate) fn from_path(path: &syn::Path) -> Self {
        if path.is_ident("u8") {
            DataType::UInt8
        } else if path.is_ident("u16") {
            DataType::UInt16
        } else if path.is_ident("u32") {
            DataType::UInt32
        } else if path.is_ident("u64") {
            DataType::UInt64
        } else if path.is_ident("i8") {
            DataType::Int8
        } else if path.is_ident("i16") {
            DataType::Int16
        } else if path.is_ident("i32") {
            DataType::Int32
        } else if path.is_ident("i64") {
            DataType::Int64
        } else if path.is_ident("String") {
            DataType::String
        } else if path.is_ident("bool") {
            DataType::Boolean
        } else {
            todo!()
        }
    }
}