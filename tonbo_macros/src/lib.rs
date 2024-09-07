mod keys;
mod schema_model;

mod record;

use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Error, Fields, GenericArgument, Path, Type};

use crate::{keys::PrimaryKey, schema_model::ModelAttributes};

enum DataType {
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

/// used to define the structure of Record,
/// will generate the implementation required in Tonbo, allowing derive expansion.
///
/// # Example
///
/// ```no_rust
/// use tonbo::Record;
///
/// #[derive(Record)]
/// pub struct Music {
///     #[primary_key]
///     pub id: u32,
///     pub name: String,
///     pub url: Option<String>,
///     pub is_favorite: bool,
/// }
/// ```
#[proc_macro_derive(Record, attributes(record))]
pub fn tonbo_record(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    let result = record::handle(ast);
    match result {
        Ok(codegen) => codegen.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn path_to_type(path: &Path) -> DataType {
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

#[proc_macro_derive(KeyAttributes, attributes(primary_key))]
pub fn key_attributes(_input: TokenStream) -> TokenStream {
    let gen = quote::quote! {};
    gen.into()
}
