use syn::Ident;

pub(crate) trait IdentGenerator {
    fn to_ref_ident(&self) -> Ident;

    fn to_schema_ident(&self) -> Ident;

    fn to_array_ident(&self) -> Ident;

    fn to_immutable_array_ident(&self) -> Ident;

    fn to_key_ident(&self) -> Ident;

    fn to_key_ref_ident(&self) -> Ident;
}

impl IdentGenerator for proc_macro2::Ident {
    fn to_ref_ident(&self) -> Ident {
        Ident::new(&format!("{self}Ref"), self.span())
    }

    fn to_schema_ident(&self) -> Ident {
        Ident::new(&format!("{self}Schema"), self.span())
    }

    fn to_array_ident(&self) -> Ident {
        Ident::new(&format!("{self}_array"), self.span())
    }

    fn to_immutable_array_ident(&self) -> Ident {
        Ident::new(&format!("{self}ImmutableArrays"), self.span())
    }

    fn to_key_ident(&self) -> Ident {
        Ident::new(&format!("{self}Key"), self.span())
    }

    fn to_key_ref_ident(&self) -> Ident {
        Ident::new(&format!("{self}KeyRef"), self.span())
    }
}
