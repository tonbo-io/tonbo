#![allow(clippy::too_many_arguments)]
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{GenericArgument, Type};

use crate::{data_type::DataType, ident_generator::IdentGenerator};

#[derive(Debug, darling::FromField)]
#[darling(attributes(record))]
pub(crate) struct RecordStructFieldOpt {
    pub(crate) ident: Option<Ident>,
    pub(crate) ty: Type,
    #[darling(default)]
    pub(crate) primary_key: Option<bool>,
}

impl RecordStructFieldOpt {
    pub(crate) fn to_array_ident(&self) -> Ident {
        let field_name = self.ident.as_ref().expect("expect named struct field");
        field_name.to_array_ident()
    }

    /// convert the ty into data type, and return whether it is nullable
    pub(crate) fn to_data_type(&self) -> Option<(DataType, bool)> {
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

pub(crate) fn trait_decode_codegen(
    struct_name: &Ident,
    fields: &[RecordStructFieldOpt],
) -> TokenStream {
    let mut decode_method_fields: Vec<TokenStream> = Vec::new();
    let mut field_names: Vec<TokenStream> = Vec::new();

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();

        let (data_type, is_nullable) = field.to_data_type().expect("unreachable code");

        let field_ty = data_type.to_field_ty();

        field_names.push(quote!(#field_name,));

        if field.primary_key.unwrap_or_default() {
            decode_method_fields.push(quote! {
                let #field_name = #field_ty::decode(reader).await?;
            });
        } else if is_nullable {
            decode_method_fields.push(quote! {
                let #field_name = Option::<#field_ty>::decode(reader).await?;
            });
        } else {
            decode_method_fields.push(quote! {
                let #field_name = Option::<#field_ty>::decode(reader).await?.unwrap();
            });
        }
    }
    quote! {
        impl ::tonbo::Decode for #struct_name {
            async fn decode<R>(reader: &mut R) -> Result<Self, ::fusio::Error>
            where
                R: ::tonbo::SeqRead,
            {
                #(#decode_method_fields)*
                Ok(Self { #(#field_names)* })
            }
        }
    }
}

pub(crate) fn struct_ref_codegen(
    struct_name: &Ident,
    fields: &[RecordStructFieldOpt],
) -> TokenStream {
    let struct_ref_name = struct_name.to_ref_ident();
    let mut ref_fields: Vec<TokenStream> = Vec::new();

    let mut has_ref = false;
    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();

        let (data_type, _is_nullable) = field.to_data_type().expect("unreachable code");

        let is_string = matches!(data_type, DataType::String);
        let is_bytes = matches!(data_type, DataType::Bytes);
        let field_ty = data_type.to_field_ty();

        has_ref = has_ref || is_string || is_bytes;

        if field.primary_key.unwrap_or_default() {
            if is_string {
                ref_fields.push(quote! { pub #field_name: &'r str, });
            } else if is_bytes {
                ref_fields.push(quote! { pub #field_name: &'r [u8], });
            } else {
                ref_fields.push(quote! { pub #field_name: #field_ty, });
            }
        } else if is_string {
            ref_fields.push(quote! { pub #field_name: Option<&'r str>, });
        } else if is_bytes {
            ref_fields.push(quote! { pub #field_name: Option<&'r [u8]>, });
        } else {
            ref_fields.push(quote! { pub #field_name: Option<#field_ty>, });
        }
    }

    if has_ref {
        quote! {
            #[derive(Debug, PartialEq, Clone, Copy)]
            pub struct #struct_ref_name<'r> { #(#ref_fields)* }
        }
    } else {
        quote! {
            #[derive(Debug, PartialEq, Clone, Copy)]
            pub struct #struct_ref_name { #(#ref_fields)* }
        }
    }
}

pub(crate) fn trait_decode_ref_codegen(
    struct_name: &&Ident,
    primary_key_name: &Ident,
    fields: &[RecordStructFieldOpt],
) -> TokenStream {
    let mut ref_projection_fields: Vec<TokenStream> = Vec::new();
    let mut from_record_batch_fields: Vec<TokenStream> = Vec::new();
    let mut field_names: Vec<TokenStream> = Vec::new();
    let mut has_ref = false;

    for (i, field) in fields.iter().enumerate() {
        let field_name = field.ident.as_ref().unwrap();
        let field_array_name = field.to_array_ident();
        let field_index = i + 2;

        let (data_type, is_nullable) = field.to_data_type().expect("unreachable code");
        if matches!(data_type, DataType::String | DataType::Bytes) {
            has_ref = true;
        }
        let as_method = data_type.to_as_method();
        field_names.push(quote!(#field_name,));

        if field.primary_key.unwrap_or_default() {
            from_record_batch_fields.push(quote! {
                let #field_name = record_batch.column(column_i).#as_method.value(offset).into();
                column_i += 1;
            });
        } else {
            ref_projection_fields.push(quote! {
                if !projection_mask.leaf_included(#field_index) { self.#field_name = None; }
            });
            if is_nullable {
                from_record_batch_fields.push(quote! {
                    let mut #field_name = None;
                    if projection_mask.leaf_included(#field_index) {
                        let #field_array_name = record_batch.column(column_i).#as_method;
                        use ::tonbo::arrow::array::Array;
                        if !#field_array_name.is_null(offset) { #field_name = Some(#field_array_name.value(offset).into()); }
                        column_i += 1;
                    }
                });
            } else {
                from_record_batch_fields.push(quote! {
                    let mut #field_name = None;
                    if projection_mask.leaf_included(#field_index) {
                        #field_name = Some(record_batch.column(column_i).#as_method.value(offset).into());
                        column_i += 1;
                    }
                });
            }
        }
    }

    let struct_ref_name = struct_name.to_ref_ident();
    let struct_ref_type = if has_ref {
        quote!(#struct_ref_name<'r>)
    } else {
        quote!(#struct_ref_name)
    };

    quote! {
        impl<'r> ::tonbo::record::RecordRef<'r> for #struct_ref_type {
            type Record = #struct_name;

            fn key(self) -> <<<<#struct_ref_type as ::tonbo::record::RecordRef<'r>>::Record as ::tonbo::record::Record>::Schema as ::tonbo::record::Schema>::Key as ::tonbo::record::Key>::Ref<'r> {
                self.#primary_key_name
            }

            fn projection(&mut self, projection_mask: &::tonbo::parquet::arrow::ProjectionMask) {
                #(#ref_projection_fields)*
            }

            fn from_record_batch(
                record_batch: &'r ::tonbo::arrow::record_batch::RecordBatch,
                offset: usize,
                projection_mask: &'r ::tonbo::parquet::arrow::ProjectionMask,
                _: &::std::sync::Arc<::tonbo::arrow::datatypes::Schema>,
            ) -> ::tonbo::record::option::OptionRecordRef<'r, Self> {
                use ::tonbo::arrow::array::AsArray;
                let mut column_i = 2;
                let null = record_batch.column(0).as_boolean().value(offset);
                let ts = record_batch.column(1).as_primitive::<::tonbo::arrow::datatypes::UInt32Type>().value(offset).into();
                #(#from_record_batch_fields)*
                let record = #struct_ref_name { #(#field_names)* };
                ::tonbo::record::option::OptionRecordRef::new(ts, record, null)
            }
        }
    }
}

pub(crate) fn trait_encode_codegen(
    struct_name: &Ident,
    fields: &[RecordStructFieldOpt],
) -> TokenStream {
    let mut encode_method_fields: Vec<TokenStream> = Vec::new();
    let mut encode_size_fields: Vec<TokenStream> = Vec::new();
    let mut has_ref = false;

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();
        let (data_type, _is_nullable) = field.to_data_type().expect("unreachable code");
        if matches!(data_type, DataType::String | DataType::Bytes) {
            has_ref = true;
        }
        encode_method_fields
            .push(quote! { ::tonbo::Encode::encode(&self.#field_name, writer).await?; });
        encode_size_fields.push(quote! { + self.#field_name.size() });
    }

    let struct_ref_name = struct_name.to_ref_ident();

    if has_ref {
        quote! {
            impl<'r> ::tonbo::Encode for #struct_ref_name<'r> {
                async fn encode<W>(&self, writer: &mut W) -> Result<(), ::fusio::Error>
                where W: ::tonbo::Write,
                { #(#encode_method_fields)* Ok(()) }
                fn size(&self) -> usize { 0 #(#encode_size_fields)* }
            }
        }
    } else {
        quote! {
            impl ::tonbo::Encode for #struct_ref_name {
                async fn encode<W>(&self, writer: &mut W) -> Result<(), ::fusio::Error>
                where W: ::tonbo::Write,
                { #(#encode_method_fields)* Ok(()) }
                fn size(&self) -> usize { 0 #(#encode_size_fields)* }
            }
        }
    }
}
