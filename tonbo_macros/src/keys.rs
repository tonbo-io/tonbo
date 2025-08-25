use proc_macro2::TokenStream;
use syn::Type;

#[derive(Clone)]
pub(crate) struct PrimaryKey {
    pub(crate) base_ty: Type,
    pub(crate) fn_key: TokenStream,
}
