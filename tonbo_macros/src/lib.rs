mod keys;
mod schema_model;

mod record;

pub(crate) mod data_type;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

use crate::data_type::DataType;

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
///     #[record(primary_key)]
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

#[proc_macro_derive(KeyAttributes, attributes(primary_key))]
pub fn key_attributes(_input: TokenStream) -> TokenStream {
    let gen = quote::quote! {};
    gen.into()
}
