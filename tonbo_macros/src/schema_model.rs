use syn::{parse::Result, Field};

pub(crate) struct ModelAttributes;

impl ModelAttributes {
    pub(crate) fn parse_field(field: &Field) -> Result<bool> {
        for attr in &field.attrs {
            if attr.path().is_ident("primary_key") {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
