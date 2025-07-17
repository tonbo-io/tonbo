use proc_macro2::Ident;
use quote::quote;

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
    Bytes,
    Float32,
    Float64,
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
        } else if path.is_ident("Bytes") {
            DataType::Bytes
        } else if path.is_ident("F32") {
            DataType::Float32
        } else if path.is_ident("F64") {
            DataType::Float64
        } else {
            todo!()
        }
    }

    pub(crate) fn to_field_ty(&self) -> proc_macro2::TokenStream {
        match self {
            DataType::UInt8 => {
                quote!(u8)
            }
            DataType::UInt16 => {
                quote!(u16)
            }
            DataType::UInt32 => {
                quote!(u32)
            }
            DataType::UInt64 => {
                quote!(u64)
            }
            DataType::Int8 => {
                quote!(i8)
            }
            DataType::Int16 => {
                quote!(i16)
            }
            DataType::Int32 => {
                quote!(i32)
            }
            DataType::Int64 => {
                quote!(i64)
            }
            DataType::String => {
                quote!(String)
            }
            DataType::Boolean => {
                quote!(bool)
            }
            DataType::Bytes => {
                quote!(bytes::Bytes)
            }
            DataType::Float32 => {
                quote!(::tonbo::F32)
            }
            DataType::Float64 => {
                quote!(::tonbo::F64)
            }
        }
    }

    pub(crate) fn to_mapped_type(&self) -> proc_macro2::TokenStream {
        match self {
            DataType::UInt8 => {
                quote!(::tonbo::arrow::datatypes::DataType::UInt8)
            }
            DataType::UInt16 => {
                quote!(::tonbo::arrow::datatypes::DataType::UInt16)
            }
            DataType::UInt32 => {
                quote!(::tonbo::arrow::datatypes::DataType::UInt32)
            }
            DataType::UInt64 => {
                quote!(::tonbo::arrow::datatypes::DataType::UInt64)
            }
            DataType::Int8 => {
                quote!(::tonbo::arrow::datatypes::DataType::Int8)
            }
            DataType::Int16 => {
                quote!(::tonbo::arrow::datatypes::DataType::Int16)
            }
            DataType::Int32 => {
                quote!(::tonbo::arrow::datatypes::DataType::Int32)
            }
            DataType::Int64 => {
                quote!(::tonbo::arrow::datatypes::DataType::Int64)
            }
            DataType::String => {
                quote!(::tonbo::arrow::datatypes::DataType::Utf8)
            }
            DataType::Boolean => {
                quote!(::tonbo::arrow::datatypes::DataType::Boolean)
            }
            DataType::Bytes => {
                quote!(::tonbo::arrow::datatypes::DataType::Binary)
            }
            DataType::Float32 => {
                quote!(::tonbo::arrow::datatypes::DataType::Float32)
            }
            DataType::Float64 => {
                quote!(::tonbo::arrow::datatypes::DataType::Float64)
            }
        }
    }

    pub(crate) fn to_array_ty(&self) -> proc_macro2::TokenStream {
        match self {
            DataType::UInt8 => {
                quote!(::tonbo::arrow::array::UInt8Array)
            }
            DataType::UInt16 => {
                quote!(::tonbo::arrow::array::UInt16Array)
            }
            DataType::UInt32 => {
                quote!(::tonbo::arrow::array::UInt32Array)
            }
            DataType::UInt64 => {
                quote!(::tonbo::arrow::array::UInt64Array)
            }
            DataType::Int8 => {
                quote!(::tonbo::arrow::array::Int8Array)
            }
            DataType::Int16 => {
                quote!(::tonbo::arrow::array::Int16Array)
            }
            DataType::Int32 => {
                quote!(::tonbo::arrow::array::Int32Array)
            }
            DataType::Int64 => {
                quote!(::tonbo::arrow::array::Int64Array)
            }
            DataType::String => {
                quote!(::tonbo::arrow::array::StringArray)
            }
            DataType::Boolean => {
                quote!(::tonbo::arrow::array::BooleanArray)
            }
            DataType::Bytes => {
                quote!(
                    ::tonbo::arrow::array::GenericByteArray::<
                        ::tonbo::arrow::datatypes::GenericBinaryType<i32>,
                    >
                )
            }
            DataType::Float32 => {
                quote!(::tonbo::arrow::array::Float32Array)
            }
            DataType::Float64 => {
                quote!(::tonbo::arrow::array::Float64Array)
            }
        }
    }

    pub(crate) fn to_as_method(&self) -> proc_macro2::TokenStream {
        match self {
            DataType::UInt8 => {
                quote!(as_primitive::<::tonbo::arrow::datatypes::UInt8Type>())
            }
            DataType::UInt16 => {
                quote!(as_primitive::<::tonbo::arrow::datatypes::UInt16Type>())
            }
            DataType::UInt32 => {
                quote!(as_primitive::<::tonbo::arrow::datatypes::UInt32Type>())
            }
            DataType::UInt64 => {
                quote!(as_primitive::<::tonbo::arrow::datatypes::UInt64Type>())
            }
            DataType::Int8 => {
                quote!(as_primitive::<::tonbo::arrow::datatypes::Int8Type>())
            }
            DataType::Int16 => {
                quote!(as_primitive::<::tonbo::arrow::datatypes::Int16Type>())
            }
            DataType::Int32 => {
                quote!(as_primitive::<::tonbo::arrow::datatypes::Int32Type>())
            }
            DataType::Int64 => {
                quote!(as_primitive::<::tonbo::arrow::datatypes::Int64Type>())
            }
            DataType::String => {
                quote!(as_string::<i32>())
            }
            DataType::Boolean => {
                quote!(as_boolean())
            }
            DataType::Bytes => {
                quote!(as_bytes::<::tonbo::arrow::datatypes::GenericBinaryType<i32>>())
            }
            DataType::Float32 => {
                quote!(as_primitive::<::tonbo::arrow::datatypes::Float32Type>())
            }
            DataType::Float64 => {
                quote!(as_primitive::<::tonbo::arrow::datatypes::Float64Type>())
            }
        }
    }

    pub(crate) fn to_builder_with_capacity_method(&self) -> proc_macro2::TokenStream {
        match self {
            DataType::UInt8 => {
                quote!(::tonbo::arrow::array::PrimitiveBuilder::<
                    ::tonbo::arrow::datatypes::UInt8Type,
                >::with_capacity(capacity))
            }
            DataType::UInt16 => {
                quote!(::tonbo::arrow::array::PrimitiveBuilder::<
                    ::tonbo::arrow::datatypes::UInt16Type,
                >::with_capacity(capacity))
            }
            DataType::UInt32 => {
                quote!(::tonbo::arrow::array::PrimitiveBuilder::<
                    ::tonbo::arrow::datatypes::UInt32Type,
                >::with_capacity(capacity))
            }
            DataType::UInt64 => {
                quote!(::tonbo::arrow::array::PrimitiveBuilder::<
                    ::tonbo::arrow::datatypes::UInt64Type,
                >::with_capacity(capacity))
            }
            DataType::Int8 => {
                quote!(::tonbo::arrow::array::PrimitiveBuilder::<
                    ::tonbo::arrow::datatypes::Int8Type,
                >::with_capacity(capacity))
            }
            DataType::Int16 => {
                quote!(::tonbo::arrow::array::PrimitiveBuilder::<
                    ::tonbo::arrow::datatypes::Int16Type,
                >::with_capacity(capacity))
            }
            DataType::Int32 => {
                quote!(::tonbo::arrow::array::PrimitiveBuilder::<
                    ::tonbo::arrow::datatypes::Int32Type,
                >::with_capacity(capacity))
            }
            DataType::Int64 => {
                quote!(::tonbo::arrow::array::PrimitiveBuilder::<
                    ::tonbo::arrow::datatypes::Int64Type,
                >::with_capacity(capacity))
            }
            DataType::String => {
                quote!(::tonbo::arrow::array::StringBuilder::with_capacity(
                    capacity, 0
                ))
            }
            DataType::Boolean => {
                quote!(::tonbo::arrow::array::BooleanBuilder::with_capacity(
                    capacity
                ))
            }
            DataType::Bytes => {
                quote!(::tonbo::arrow::array::GenericByteBuilder::<
                    ::tonbo::arrow::datatypes::GenericBinaryType<i32>,
                >::with_capacity(capacity, 0))
            }
            DataType::Float32 => {
                quote!(::tonbo::arrow::array::PrimitiveBuilder::<
                    ::tonbo::arrow::datatypes::Float32Type,
                >::with_capacity(capacity))
            }
            DataType::Float64 => {
                quote!(::tonbo::arrow::array::PrimitiveBuilder::<
                    ::tonbo::arrow::datatypes::Float64Type,
                >::with_capacity(capacity))
            }
        }
    }

    pub(crate) fn to_builder(&self) -> proc_macro2::TokenStream {
        match self {
            DataType::UInt8 => {
                quote!(
                    ::tonbo::arrow::array::PrimitiveBuilder<
                        ::tonbo::arrow::datatypes::UInt8Type,
                    >
                )
            }
            DataType::UInt16 => {
                quote!(
                    ::tonbo::arrow::array::PrimitiveBuilder<
                        ::tonbo::arrow::datatypes::UInt16Type,
                    >
                )
            }
            DataType::UInt32 => {
                quote!(
                    ::tonbo::arrow::array::PrimitiveBuilder<
                        ::tonbo::arrow::datatypes::UInt32Type,
                    >
                )
            }
            DataType::UInt64 => {
                quote!(
                    ::tonbo::arrow::array::PrimitiveBuilder<
                        ::tonbo::arrow::datatypes::UInt64Type,
                    >
                )
            }
            DataType::Int8 => {
                quote!(
                    ::tonbo::arrow::array::PrimitiveBuilder<
                        ::tonbo::arrow::datatypes::Int8Type,
                    >
                )
            }
            DataType::Int16 => {
                quote!(
                    ::tonbo::arrow::array::PrimitiveBuilder<
                        ::tonbo::arrow::datatypes::Int16Type,
                    >
                )
            }
            DataType::Int32 => {
                quote!(
                    ::tonbo::arrow::array::PrimitiveBuilder<
                        ::tonbo::arrow::datatypes::Int32Type,
                    >
                )
            }
            DataType::Int64 => {
                quote!(
                    ::tonbo::arrow::array::PrimitiveBuilder<
                        ::tonbo::arrow::datatypes::Int64Type,
                    >
                )
            }
            DataType::String => {
                quote!(::tonbo::arrow::array::StringBuilder)
            }
            DataType::Boolean => {
                quote!(::tonbo::arrow::array::BooleanBuilder)
            }
            DataType::Bytes => {
                quote!(
                    ::tonbo::arrow::array::GenericByteBuilder::<
                        ::tonbo::arrow::datatypes::GenericBinaryType<i32>,
                    >
                )
            }
            DataType::Float32 => {
                quote!(
                    ::tonbo::arrow::array::PrimitiveBuilder<
                        ::tonbo::arrow::datatypes::Float32Type,
                    >
                )
            }
            DataType::Float64 => {
                quote!(
                    ::tonbo::arrow::array::PrimitiveBuilder<
                        ::tonbo::arrow::datatypes::Float64Type,
                    >
                )
            }
        }
    }
    pub(crate) fn to_size_method(&self, field_name: &Ident) -> proc_macro2::TokenStream {
        match self {
            DataType::UInt8 => {
                quote!(std::mem::size_of_val(self.#field_name.values_slice()))
            }
            DataType::UInt16 => {
                quote!(std::mem::size_of_val(self.#field_name.values_slice()))
            }
            DataType::UInt32 => {
                quote!(std::mem::size_of_val(self.#field_name.values_slice()))
            }
            DataType::UInt64 => {
                quote!(std::mem::size_of_val(self.#field_name.values_slice()))
            }
            DataType::Int8 => {
                quote!(std::mem::size_of_val(self.#field_name.values_slice()))
            }
            DataType::Int16 => {
                quote!(std::mem::size_of_val(self.#field_name.values_slice()))
            }
            DataType::Int32 => {
                quote!(std::mem::size_of_val(self.#field_name.values_slice()))
            }
            DataType::Int64 => {
                quote!(std::mem::size_of_val(self.#field_name.values_slice()))
            }
            DataType::String => {
                quote!(self.#field_name.values_slice().len())
            }
            DataType::Boolean => {
                quote!(self.#field_name.values_slice().len())
            }
            DataType::Bytes => {
                quote!(self.#field_name.values_slice().len())
            }
            DataType::Float32 => {
                quote!(std::mem::size_of_val(self.#field_name.values_slice()))
            }
            DataType::Float64 => {
                quote!(std::mem::size_of_val(self.#field_name.values_slice()))
            }
        }
    }
    pub(crate) fn to_size_field(
        &self,
        field_name: &Ident,
        is_nullable: bool,
    ) -> proc_macro2::TokenStream {
        match self {
            DataType::UInt8 => {
                quote!(std::mem::size_of::<u8>())
            }
            DataType::UInt16 => {
                quote!(std::mem::size_of::<u16>())
            }
            DataType::UInt32 => {
                quote! {std::mem::size_of::<u32>()}
            }
            DataType::UInt64 => {
                quote! {std::mem::size_of::<u64>()}
            }
            DataType::Int8 => {
                quote! {std::mem::size_of::<i8>()}
            }
            DataType::Int16 => {
                quote! {std::mem::size_of::<i16>()}
            }
            DataType::Int32 => {
                quote! {std::mem::size_of::<i32>()}
            }
            DataType::Int64 => {
                quote! {std::mem::size_of::<i64>()}
            }
            DataType::String => {
                if is_nullable {
                    quote!(self.#field_name.as_ref().map(String::len).unwrap_or(0))
                } else {
                    quote!(self.#field_name.len())
                }
            }
            DataType::Boolean => {
                quote! {std::mem::size_of::<bool>()}
            }
            DataType::Bytes => {
                if is_nullable {
                    quote!(self.#field_name.as_ref().map(Vec::len).unwrap_or(0))
                } else {
                    quote!(self.#field_name.len())
                }
            }
            DataType::Float32 => {
                quote! {std::mem::size_of::<::tonbo::F32>()}
            }
            DataType::Float64 => {
                quote! {std::mem::size_of::<::tonbo::F64>()}
            }
        }
    }

    pub(crate) fn to_as_value_fn(&self) -> proc_macro2::TokenStream {
        match self {
            DataType::UInt8 => quote!(as_u8()),
            DataType::UInt16 => quote!(as_u16()),
            DataType::UInt32 => quote!(as_u32()),
            DataType::UInt64 => quote!(as_u64()),
            DataType::Int8 => quote!(as_i8()),
            DataType::Int16 => quote!(as_i16()),
            DataType::Int32 => quote!(as_i32()),
            DataType::Int64 => quote!(as_i64()),
            DataType::String => quote!(as_string()),
            DataType::Boolean => quote!(as_boolean()),
            DataType::Bytes => quote!(as_bytes()),
            DataType::Float32 => quote!(as_f32()),
            DataType::Float64 => quote!(as_f64()),
        }
    }
}
