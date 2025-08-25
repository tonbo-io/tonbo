use proc_macro2::Ident;
use quote::{quote, ToTokens};

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
        } else if path.is_ident("F32") || path.is_ident("f32") {
            DataType::Float32
        } else if path.is_ident("F64") || path.is_ident("f64") {
            DataType::Float64
        } else if let Some(seg) = path.segments.last() {
            // Support Vec<u8>
            if seg.ident == "Vec" {
                if let syn::PathArguments::AngleBracketed(ref generic_args) = seg.arguments {
                    if generic_args.args.len() == 1 {
                        if let syn::GenericArgument::Type(syn::Type::Path(tp)) =
                            &generic_args.args[0]
                        {
                            if tp.path.is_ident("u8") {
                                return DataType::Bytes;
                            } else {
                                panic!("unsupported Vec<T> element type: {}", tp.to_token_stream());
                            }
                        }
                    }
                    panic!(
                        "unsupported Vec<T> generic args: {}",
                        generic_args.to_token_stream()
                    );
                }
                panic!(
                    "unsupported Vec<T> arguments: {}",
                    seg.arguments.to_token_stream()
                )
            } else {
                panic!("unsupported field type path: {}", path.to_token_stream())
            }
        } else {
            panic!("unsupported field type path: {}", path.to_token_stream())
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
                quote!(Vec<u8>)
            }
            DataType::Float32 => {
                quote!(f32)
            }
            DataType::Float64 => {
                quote!(f64)
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
                quote! {std::mem::size_of::<f32>()}
            }
            DataType::Float64 => {
                quote! {std::mem::size_of::<f64>()}
            }
        }
    }
}
