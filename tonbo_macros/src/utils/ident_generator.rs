use syn::Ident;

pub(crate) trait IdentGenerator {
    fn to_ref_ident(&self) -> Ident;

    fn to_schema_ident(&self) -> Ident;

    fn to_builder_ident(&self) -> Ident;

    fn to_array_ident(&self) -> Ident;

    fn to_immutable_array_ident(&self) -> Ident;

    fn to_key_ident(&self) -> Ident;
}

impl IdentGenerator for proc_macro2::Ident {
    fn to_ref_ident(&self) -> Ident {
        Ident::new(&format!("{}Ref", self), self.span())
    }

    fn to_schema_ident(&self) -> Ident {
        Ident::new(&format!("{}Schema", self), self.span())
    }

    fn to_builder_ident(&self) -> Ident {
        Ident::new(&format!("{}Builder", self), self.span())
    }

    fn to_array_ident(&self) -> Ident {
        Ident::new(&format!("{}_array", self), self.span())
    }

    fn to_immutable_array_ident(&self) -> Ident {
        Ident::new(&format!("{}ImmutableArrays", self), self.span())
    }

    fn to_key_ident(&self) -> Ident {
        Ident::new(&format!("{}Key", self), self.span())
    }
}
