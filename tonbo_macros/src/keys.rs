use proc_macro2::{Ident, TokenStream};
use syn::Type;

pub(crate) struct PrimaryKey {
    pub(crate) name: Ident,
    pub(crate) base_ty: Type,
    pub(crate) fn_key: TokenStream,
    pub(crate) builder_append_value: TokenStream,
    pub(crate) index: usize,
}
