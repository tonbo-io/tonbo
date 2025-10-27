use darling::{ast::Data, util::Ignored, FromDeriveInput, FromField};
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{DeriveInput, Error, GenericArgument, Type};

use crate::{
    data_type::DataType,
    ident_generator::IdentGenerator,
    record::{
        struct_key_codegen, struct_key_ref_codegen, struct_ref_codegen, trait_decode_codegen,
        trait_decode_ref_codegen, trait_encode_codegen,
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
    let primary_key_fields = data_struct
        .fields
        .iter()
        .filter(|field| field.primary_key == Some(true))
        .collect::<Vec<_>>();

    let mut fn_key = vec![];
    // Validate PK non-nullable
    if primary_key_fields.is_empty() {
        return Err(syn::Error::new_spanned(
            struct_name,
            "missing primary key field, use #[record(primary_key)] to define one",
        ));
    }
    for field in primary_key_fields.iter() {
        let (_, is_nullable) = field.to_data_type().expect("only Path ty is supported");
        if is_nullable {
            return Err(syn::Error::new_spanned(
                struct_name,
                "primary key cannot be nullable",
            ));
        }
        let field_name = field.ident.as_ref().unwrap();
        fn_key.push(quote! { self.#field_name, });
    }

    let fields: Vec<crate::record::RecordStructFieldOpt> = data_struct
        .fields
        .iter()
        .map(|f| crate::record::RecordStructFieldOpt {
            ident: f.ident.clone(),
            ty: f.ty.clone(),
            primary_key: f.primary_key,
        })
        .collect();

    let struct_key = struct_key_codegen(struct_name, &fields);
    let struct_key_ref = struct_key_ref_codegen(struct_name, &fields);
    let key_trait_impl = trait_key_codegen(struct_name, &fields);
    let key_ref_trait_impl = trait_key_ref_codegen(struct_name, &fields);
    let key_encode_impl = trait_key_encode_codegen(struct_name, &fields);
    let key_decode_impl = trait_key_decode_codegen(struct_name, &fields);
    let key_ref_encode_impl = trait_key_ref_encode_codegen(struct_name, &fields);
    let pk_array_impl = trait_pk_array_codegen(struct_name, &fields);
    let record_impl = trait_record_codegen_typed(&fields, struct_name);
    let decode_impl = trait_decode_codegen(struct_name, &fields);
    let struct_ref_def = struct_ref_codegen(struct_name, &fields);
    let decode_ref_impl = trait_decode_ref_codegen(&struct_name, &fields);
    let encode_impl = trait_encode_codegen(struct_name, &fields);
    let from_ref_impl = from_ref_into_owned_codegen(struct_name, &fields);
    let type_alias = schema_type_alias_typed(struct_name);
    let arrays_alias = arrays_type_alias_typed(struct_name);

    let gen = quote! {
        #struct_key
        #struct_key_ref
        #key_trait_impl
        #key_ref_trait_impl
        #key_encode_impl
        #key_decode_impl
        #key_ref_encode_impl
        #pk_array_impl
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
) -> TokenStream {
    let mut size_fields: Vec<TokenStream> = Vec::new();
    let mut to_ref_init_fields: Vec<TokenStream> = Vec::new();
    let mut has_ref = false;
    let mut fn_primary_key_ref = Vec::new();

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
                fn_primary_key_ref.push(quote! { #field_name: &self.#field_name, });
            } else {
                to_ref_init_fields.push(quote! { #field_name: self.#field_name, });
                fn_primary_key_ref.push(quote! { #field_name: self.#field_name, });
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

    let primary_key_ty = struct_name.to_key_ident();
    let primary_key_ref_name = struct_name.to_key_ref_ident();

    quote! {
        impl ::tonbo::record::Record for #struct_name {
            type Schema = ::tonbo::record::typed::TonboTypedSchema<#struct_name, #primary_key_ty>;

            type Ref<'r> = #struct_ref_type where Self: 'r;

            fn key(&self) -> <<Self::Schema as ::tonbo::record::Schema>::Key as ::tonbo::record::Key>::Ref<'_> {
                #primary_key_ref_name { #(#fn_primary_key_ref)* }
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

fn schema_type_alias_typed(struct_name: &Ident) -> TokenStream {
    let pk_ty = struct_name.to_key_ident();
    let schema_name = struct_name.to_schema_ident();
    quote! {
        pub type #schema_name = ::tonbo::record::typed::TonboTypedSchema<#struct_name, #pk_ty>;
    }
}

fn arrays_type_alias_typed(struct_name: &Ident) -> TokenStream {
    let pk_ty = struct_name.to_key_ident();
    let arrays_name = struct_name.to_immutable_array_ident();
    quote! {
        pub type #arrays_name = ::tonbo::record::typed::TonboTypedArrays<#struct_name, #pk_ty>;
    }
}

fn trait_key_codegen(
    struct_name: &Ident,
    fields: &[crate::record::RecordStructFieldOpt],
) -> TokenStream {
    let struct_key_name = struct_name.to_key_ident();
    let struct_key_ref_name = struct_name.to_key_ref_ident();

    let mut has_ref = false;
    // Check if any primary key fields have string/bytes (need lifetime)

    // Build field lists for the impl
    let mut as_key_ref_fields = Vec::new();
    let mut to_arrow_datums_fields = Vec::new();

    for field in fields.iter() {
        if field.primary_key.unwrap_or_default() {
            let (data_type, _) = field.to_data_type().expect("unreachable code");
            let field_name = field.ident.as_ref().unwrap();

            if matches!(data_type, DataType::String | DataType::Bytes) {
                has_ref = true;
                as_key_ref_fields.push(quote! { #field_name: &self.#field_name, });
            } else {
                as_key_ref_fields.push(quote! { #field_name: self.#field_name, });
            }

            to_arrow_datums_fields.push(quote! { out.extend(self.#field_name.to_arrow_datums()); });
        }
    }

    let key_ref_type = if has_ref {
        quote! { #struct_key_ref_name<'r> }
    } else {
        quote! { #struct_key_ref_name }
    };

    quote! {
        impl ::tonbo::record::Key for #struct_key_name {
            type Ref<'r> = #key_ref_type where Self: 'r;

            fn as_key_ref(&self) -> Self::Ref<'_> {
                #struct_key_ref_name {
                    #(#as_key_ref_fields)*
                }
            }

            fn to_arrow_datums(&self) -> Vec<::std::sync::Arc<dyn ::tonbo::arrow::array::Datum>> {
                let mut out = Vec::new();
                #(#to_arrow_datums_fields)*
                out
            }
        }
    }
}

fn trait_key_ref_codegen(
    struct_name: &Ident,
    fields: &[crate::record::RecordStructFieldOpt],
) -> TokenStream {
    let struct_key_name = struct_name.to_key_ident();
    let struct_key_ref_name = struct_name.to_key_ref_ident();

    // Check if any primary key fields have string/bytes (need lifetime)
    let has_ref = fields.iter().any(|field| {
        if field.primary_key.unwrap_or_default() {
            let (data_type, _) = field.to_data_type().expect("unreachable code");
            matches!(data_type, DataType::String | DataType::Bytes)
        } else {
            false
        }
    });

    // Build field list for the impl
    let mut to_key_fields = Vec::new();

    for field in fields.iter() {
        if field.primary_key.unwrap_or_default() {
            let field_name = field.ident.as_ref().unwrap();
            if has_ref {
                to_key_fields.push(quote! { #field_name: self.#field_name.to_owned(), });
            } else {
                to_key_fields.push(quote! { #field_name: self.#field_name, });
            }
        }
    }

    let struct_key_ref_type = if has_ref {
        quote! { #struct_key_ref_name<'r> }
    } else {
        quote! { #struct_key_ref_name }
    };

    quote! {
        impl<'r> ::tonbo::record::KeyRef<'r> for #struct_key_ref_type {
            type Key = #struct_key_name;

            fn to_key(self) -> Self::Key {
                #struct_key_name {
                    #(#to_key_fields)*
                }
            }
        }
    }
}

fn trait_key_encode_codegen(
    struct_name: &Ident,
    fields: &[crate::record::RecordStructFieldOpt],
) -> TokenStream {
    let struct_key_name = struct_name.to_key_ident();

    // Build encode fields
    let mut encode_fields = Vec::new();
    let mut size_fields = Vec::new();

    for field in fields.iter() {
        if field.primary_key.unwrap_or_default() {
            let field_name = field.ident.as_ref().unwrap();
            encode_fields
                .push(quote! { ::tonbo::Encode::encode(&self.#field_name, writer).await?; });
            size_fields.push(quote! { + self.#field_name.size() });
        }
    }

    quote! {
        impl ::tonbo::Encode for #struct_key_name {
            async fn encode<W>(&self, writer: &mut W) -> Result<(), ::fusio::Error>
            where
                W: ::tonbo::Write,
            {
                #(#encode_fields)*
                Ok(())
            }

            fn size(&self) -> usize {
                0 #(#size_fields)*
            }
        }
    }
}

fn trait_key_decode_codegen(
    struct_name: &Ident,
    fields: &[crate::record::RecordStructFieldOpt],
) -> TokenStream {
    let struct_key_name = struct_name.to_key_ident();

    // Build decode fields
    let mut decode_fields = Vec::new();
    let mut field_names = Vec::new();

    for field in fields.iter() {
        if field.primary_key.unwrap_or_default() {
            let field_name = field.ident.as_ref().unwrap();
            let (data_type, _) = field.to_data_type().expect("unreachable code");
            let field_ty = data_type.to_field_ty();

            decode_fields.push(quote! {
                let #field_name = #field_ty::decode(reader).await?;
            });
            field_names.push(quote!(#field_name,));
        }
    }

    quote! {
        impl ::tonbo::Decode for #struct_key_name {
            async fn decode<R>(reader: &mut R) -> Result<Self, ::fusio::Error>
            where
                R: ::tonbo::SeqRead,
            {
                #(#decode_fields)*
                Ok(Self { #(#field_names)* })
            }
        }
    }
}

fn trait_key_ref_encode_codegen(
    struct_name: &Ident,
    fields: &[crate::record::RecordStructFieldOpt],
) -> TokenStream {
    let struct_key_ref_name = struct_name.to_key_ref_ident();

    // Check if any primary key fields have string/bytes (need lifetime)
    let has_ref = fields.iter().any(|field| {
        if field.primary_key.unwrap_or_default() {
            let (data_type, _) = field.to_data_type().expect("unreachable code");
            matches!(data_type, DataType::String | DataType::Bytes)
        } else {
            false
        }
    });

    // Build encode fields
    let mut encode_fields = Vec::new();
    let mut size_fields = Vec::new();

    for field in fields.iter() {
        if field.primary_key.unwrap_or_default() {
            let field_name = field.ident.as_ref().unwrap();
            encode_fields
                .push(quote! { ::tonbo::Encode::encode(&self.#field_name, writer).await?; });
            size_fields.push(quote! { + self.#field_name.size() });
        }
    }

    if has_ref {
        quote! {
            impl<'r> ::tonbo::Encode for #struct_key_ref_name<'r> {
                async fn encode<W>(&self, writer: &mut W) -> Result<(), ::fusio::Error>
                where
                    W: ::tonbo::Write,
                {
                    #(#encode_fields)*
                    Ok(())
                }

                fn size(&self) -> usize {
                    0 #(#size_fields)*
                }
            }
        }
    } else {
        quote! {
            impl ::tonbo::Encode for #struct_key_ref_name {
                async fn encode<W>(&self, writer: &mut W) -> Result<(), ::fusio::Error>
                where
                    W: ::tonbo::Write,
                {
                    #(#encode_fields)*
                    Ok(())
                }

                fn size(&self) -> usize {
                    0 #(#size_fields)*
                }
            }
        }
    }
}

fn trait_pk_array_codegen(
    struct_name: &Ident,
    fields: &[crate::record::RecordStructFieldOpt],
) -> TokenStream {
    let struct_key_name = struct_name.to_key_ident();
    let mut rewrite_fields = Vec::new();
    let mut rewrite_field_ty = Vec::new();
    let mut first_pk_ty = None;
    let mut first_pk_name = None;
    // let mut key_fields = Vec::new();
    let mut first_pk = None;
    for field in fields.iter() {
        if field.primary_key.unwrap_or_default() {
            let field_name = field.ident.as_ref().unwrap();
            rewrite_fields.push(quote! { #field_name, });
            let (data_type, _) = field.to_data_type().expect("unreachable code");
            let field_ty = data_type.to_field_ty();
            rewrite_field_ty
                .push(quote! { #field_ty::rewrite_pk_column(column, nulls, tombstones, keys), });
            if first_pk.is_none() {
                first_pk = Some(field);
                first_pk_ty = Some(field_ty);
                first_pk_name = Some(field_name);
                // key_fields.push(quote! { keys.iter() });
            }
        }
    }

    let first_pk_name = first_pk_name.expect("expect at least one primary key field");
    let first_pk_ty = first_pk_ty.expect("expect at least one primary key field");
    // TODO: handle multiple primary keys

    // For now, implement a simple version that delegates to individual field types
    // This is a simplified approach that works for most use cases
    quote! {
        impl ::tonbo::record::typed::PkArray for #struct_key_name {
            fn rewrite_pk_column(
                column: &::tonbo::arrow::array::ArrayRef,
                nulls: &::tonbo::arrow::array::BooleanArray,
                tombstones: &[bool],
                keys: &[Self],
            ) -> ::tonbo::arrow::array::ArrayRef {

                let ks=keys.iter().map(|key| key.#first_pk_name.clone()).collect::<Vec<_>>();
                #first_pk_ty::rewrite_pk_column(column, nulls, tombstones, ks.as_slice())
            }
        }
    }
}
