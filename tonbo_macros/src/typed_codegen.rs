use darling::{ast::Data, util::Ignored, FromDeriveInput, FromField};
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{DeriveInput, Error, GenericArgument, Type};

use crate::{
    data_type::DataType,
    ident_generator::IdentGenerator,
    keys::PrimaryKey,
    record::{
        struct_ref_codegen, trait_decode_codegen, trait_decode_ref_codegen, trait_encode_codegen,
    },
};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(record))]
struct RecordOpts {
    ident: Ident,
    data: Data<Ignored, RecordStructFieldOpt>,
}

#[derive(Debug, FromField)]
#[darling(attributes(record))]
struct RecordStructFieldOpt {
    ident: Option<Ident>,
    ty: Type,
    #[darling(default)]
    primary_key: Option<bool>,
}

impl RecordStructFieldOpt {
    fn to_data_type(&self) -> Option<(DataType, bool)> {
        if let Type::Path(type_path) = &self.ty {
            if type_path.path.segments.len() == 1 {
                let segment = &type_path.path.segments[0];
                if segment.ident == "Option" {
                    if let syn::PathArguments::AngleBracketed(ref generic_args) = segment.arguments
                    {
                        if generic_args.args.len() == 1 {
                            return if let GenericArgument::Type(Type::Path(type_path)) =
                                &generic_args.args[0]
                            {
                                Some((DataType::from_path(&type_path.path), true))
                            } else {
                                None
                            };
                        }
                    }
                }
            }
            return Some((DataType::from_path(&type_path.path), false));
        }
        None
    }
}

pub(crate) fn handle_typed(ast: DeriveInput) -> Result<TokenStream, Error> {
    let record_opts: RecordOpts = RecordOpts::from_derive_input(&ast)?;

    let struct_name = &record_opts.ident;
    let Data::Struct(data_struct) = record_opts.data else {
        return Err(syn::Error::new_spanned(
            struct_name,
            "enum is not supported",
        ));
    };

    // Find primary key field
    let Some((_primary_key_field_index, primary_key_field)) = data_struct
        .fields
        .iter()
        .enumerate()
        .find(|field| field.1.primary_key == Some(true))
    else {
        return Err(syn::Error::new_spanned(
            struct_name,
            "missing primary key field, use #[record(primary_key)] to define one",
        ));
    };

    // Validate PK non-nullable
    let primary_key_data_type = primary_key_field
        .to_data_type()
        .expect("only Path ty is supported");
    if primary_key_data_type.1 {
        return Err(syn::Error::new_spanned(
            struct_name,
            "primary key cannot be nullable",
        ));
    }

    let primary_key_ident = primary_key_field
        .ident
        .as_ref()
        .expect("cannot find primary key ident");

    let primary_key_definitions = PrimaryKey {
        base_ty: primary_key_field.ty.clone(),
        fn_key: if matches!(primary_key_data_type.0, DataType::String) {
            quote!(&self.#primary_key_ident)
        } else {
            quote!(self.#primary_key_ident)
        },
    };

    let fields: Vec<crate::record::RecordStructFieldOpt> = data_struct
        .fields
        .iter()
        .map(|f| crate::record::RecordStructFieldOpt {
            ident: f.ident.clone(),
            ty: f.ty.clone(),
            primary_key: f.primary_key,
        })
        .collect();

    let record_impl = trait_record_codegen_typed(&fields, struct_name, &primary_key_definitions);
    let decode_impl = trait_decode_codegen(struct_name, &fields);
    let struct_ref_def = struct_ref_codegen(struct_name, &fields);
    let decode_ref_impl = trait_decode_ref_codegen(&struct_name, primary_key_ident, &fields);
    let encode_impl = trait_encode_codegen(struct_name, &fields);
    let from_ref_impl = from_ref_into_owned_codegen(struct_name, &fields);
    let type_alias = schema_type_alias_typed(struct_name, &primary_key_definitions);
    let arrays_alias = arrays_type_alias_typed(struct_name, &primary_key_definitions);

    let gen = quote! {
        #record_impl
        #decode_impl
        #struct_ref_def
        #decode_ref_impl
        #encode_impl
        #from_ref_impl
        #type_alias
        #arrays_alias
    };

    Ok(gen)
}

fn trait_record_codegen_typed(
    fields: &[crate::record::RecordStructFieldOpt],
    struct_name: &Ident,
    primary_key: &PrimaryKey,
) -> TokenStream {
    let mut size_fields: Vec<TokenStream> = Vec::new();
    let mut to_ref_init_fields: Vec<TokenStream> = Vec::new();
    let mut has_ref = false;

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();
        let (data_type, is_nullable) = field.to_data_type().expect("unreachable code");

        let is_string = matches!(data_type, DataType::String);
        let is_bytes = matches!(data_type, DataType::Bytes);
        let size_field = data_type.to_size_field(field_name, is_nullable);
        has_ref = has_ref || is_string || is_bytes;

        size_fields.push(quote! { + #size_field });

        if field.primary_key.unwrap_or_default() {
            if is_string || is_bytes {
                to_ref_init_fields.push(quote! { #field_name: &self.#field_name, });
            } else {
                to_ref_init_fields.push(quote! { #field_name: self.#field_name, });
            }
        } else {
            match (is_nullable, is_string || is_bytes) {
                (true, true) => {
                    to_ref_init_fields.push(quote! { #field_name: self.#field_name.as_deref(), })
                }
                (true, false) => to_ref_init_fields.push(quote! { #field_name: self.#field_name, }),
                (false, true) => {
                    to_ref_init_fields.push(quote! { #field_name: Some(&self.#field_name), })
                }
                (false, false) => {
                    to_ref_init_fields.push(quote! { #field_name: Some(self.#field_name), })
                }
            }
        }
    }

    let struct_ref_name = struct_name.to_ref_ident();
    let struct_ref_type = if has_ref {
        quote! { #struct_ref_name<'r> }
    } else {
        quote! { #struct_ref_name }
    };

    let PrimaryKey {
        fn_key: fn_primary_key,
        base_ty: primary_key_ty,
        ..
    } = primary_key;

    quote! {
        impl ::tonbo::record::Record for #struct_name {
            type Schema = ::tonbo::record::typed::TonboTypedSchema<#struct_name, #primary_key_ty>;

            type Ref<'r> = #struct_ref_type where Self: 'r;

            fn key(&self) -> <<Self::Schema as ::tonbo::record::Schema>::Key as ::tonbo::record::Key>::Ref<'_> {
                #fn_primary_key
            }

            fn as_record_ref(&self) -> Self::Ref<'_> {
                #struct_ref_name { #(#to_ref_init_fields)* }
            }

            fn size(&self) -> usize { 0 #(#size_fields)* }
        }
    }
}

fn from_ref_into_owned_codegen(
    struct_name: &Ident,
    fields: &[crate::record::RecordStructFieldOpt],
) -> TokenStream {
    let struct_ref_name = struct_name.to_ref_ident();
    let mut assigns = Vec::new();
    let mut has_ref = false;
    for field in fields.iter() {
        let fname = field.ident.as_ref().unwrap();
        let (dt, is_nullable) = field.to_data_type().expect("unreachable code");
        let is_string = matches!(dt, DataType::String);
        let is_bytes = matches!(dt, DataType::Bytes);
        has_ref = has_ref || is_string || is_bytes;
        let expr = if field.primary_key.unwrap_or_default() {
            if is_string {
                quote!(r.#fname.to_string())
            } else if is_bytes {
                quote!(r.#fname.to_vec())
            } else {
                quote!(r.#fname)
            }
        } else if is_nullable {
            if is_string {
                quote!(r.#fname.map(|s| s.to_string()))
            } else if is_bytes {
                quote!(r.#fname.map(|b| b.to_vec()))
            } else {
                quote!(r.#fname)
            }
        } else if is_string {
            quote!(r.#fname.unwrap().to_string())
        } else if is_bytes {
            quote!(r.#fname.unwrap().to_vec())
        } else {
            quote!(r.#fname.unwrap())
        };
        assigns.push(quote!( #fname: #expr ));
    }
    if has_ref {
        quote! {
            impl<'r> From<#struct_ref_name<'r>> for #struct_name {
                fn from(r: #struct_ref_name<'r>) -> Self {
                    Self { #(#assigns),* }
                }
            }
        }
    } else {
        quote! {
            impl From<#struct_ref_name> for #struct_name {
                fn from(r: #struct_ref_name) -> Self {
                    Self { #(#assigns),* }
                }
            }
        }
    }
}

fn schema_type_alias_typed(struct_name: &Ident, primary_key: &PrimaryKey) -> TokenStream {
    let PrimaryKey { base_ty: pk_ty, .. } = primary_key;
    let schema_name = struct_name.to_schema_ident();
    quote! {
        pub type #schema_name = ::tonbo::record::typed::TonboTypedSchema<#struct_name, #pk_ty>;
    }
}

fn arrays_type_alias_typed(struct_name: &Ident, primary_key: &PrimaryKey) -> TokenStream {
    let PrimaryKey { base_ty: pk_ty, .. } = primary_key;
    let arrays_name = struct_name.to_immutable_array_ident();
    quote! {
        pub type #arrays_name = ::tonbo::record::typed::TonboTypedArrays<#struct_name, #pk_ty>;
    }
}
