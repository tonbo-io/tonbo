#![allow(clippy::too_many_arguments)]
use darling::{ast::Data, util::Ignored, FromDeriveInput, FromField};
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{DeriveInput, Error, GenericArgument, Type};

use crate::{keys::PrimaryKey, utils::ident_generator::IdentGenerator, DataType};
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
    fn to_array_ident(&self) -> Ident {
        let field_name = self.ident.as_ref().expect("expect named struct field");
        field_name.to_array_ident()
    }

    /// convert the ty into data type, and return whether it is nullable
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

pub(crate) fn handle(ast: DeriveInput) -> Result<TokenStream, Error> {
    let record_opts: RecordOpts = RecordOpts::from_derive_input(&ast)?;

    let struct_name = &record_opts.ident;
    let Data::Struct(data_struct) = record_opts.data else {
        return Err(syn::Error::new_spanned(
            struct_name,
            "enum is not supported",
        ));
    };

    // todo: deny multiple primary_key definition
    let Some((primary_key_field_index, primary_key_field)) = data_struct
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

    // check if primary key is nullable
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
        name: primary_key_ident.clone(),
        builder_append_value: quote! {
            self.#primary_key_ident .append_value(key.value);
        },
        base_ty: primary_key_field.ty.clone(),
        index: primary_key_field_index + 2,
        fn_key: if matches!(primary_key_data_type.0, DataType::String) {
            quote!(&self.#primary_key_ident)
        } else {
            quote!(self.#primary_key_ident)
        },
    };

    let builder_append_primary_key = &primary_key_definitions.builder_append_value;

    let record_codegen = trait_record_codegen(
        &data_struct.fields,
        struct_name,
        primary_key_definitions.clone(),
    );

    let decode_codegen = trait_decode_codegen(struct_name, &data_struct.fields);

    let struct_ref_codegen = struct_ref_codegen(struct_name, &data_struct.fields);

    let decode_ref_codegen =
        trait_decode_ref_codegen(&struct_name, primary_key_ident, &data_struct.fields);

    let encode_codegen = trait_encode_codegen(struct_name, &data_struct.fields);

    let struct_array_codegen = struct_array_codegen(struct_name, &data_struct.fields);

    let arrow_array_codegen =
        trait_arrow_array_codegen(struct_name, primary_key_ident, &data_struct.fields);

    let builder_codegen =
        struct_builder_codegen(struct_name, builder_append_primary_key, &data_struct.fields);

    let gen = quote! {

        #record_codegen

        #decode_codegen

        #struct_ref_codegen

        #decode_ref_codegen

        #encode_codegen

        #struct_array_codegen

        #arrow_array_codegen

        #builder_codegen

    };

    Ok(gen)
}

fn trait_record_codegen(
    fields: &[RecordStructFieldOpt],
    struct_name: &Ident,
    primary_key: PrimaryKey,
) -> TokenStream {
    let mut size_fields: Vec<TokenStream> = Vec::new();

    let mut to_ref_init_fields: Vec<TokenStream> = Vec::new();
    let mut schema_fields: Vec<TokenStream> = Vec::new();

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();

        let (data_type, is_nullable) = field.to_data_type().expect("unreachable code");

        let is_string = matches!(data_type, DataType::String);
        let is_bytes = matches!(data_type, DataType::Bytes);
        let mapped_type = data_type.to_mapped_type();
        let size_field = data_type.to_size_field(field_name, is_nullable);

        schema_fields.push(quote! {
                    ::tonbo::arrow::datatypes::Field::new(stringify!(#field_name), #mapped_type, #is_nullable),
                });

        size_fields.push(quote! {
            + #size_field
        });

        if field.primary_key.unwrap_or_default() {
            if is_string || is_bytes {
                to_ref_init_fields.push(quote! { #field_name: &self.#field_name, });
            } else {
                to_ref_init_fields.push(quote! { #field_name: self.#field_name, });
            }
        } else {
            match (is_nullable, is_string || is_bytes) {
                (true, true) => {
                    to_ref_init_fields.push(quote! { #field_name: self.#field_name.as_deref(), });
                }
                (true, false) => {
                    to_ref_init_fields.push(quote! { #field_name: self.#field_name, });
                }
                (false, true) => {
                    to_ref_init_fields.push(quote! { #field_name: Some(&self.#field_name), });
                }
                (false, false) => {
                    to_ref_init_fields.push(quote! { #field_name: Some(self.#field_name), });
                }
            }
        }
    }

    let struct_arrays_name = struct_name.to_immutable_array_ident();
    let struct_ref_name = struct_name.to_ref_ident();

    let PrimaryKey {
        name: primary_key_name,
        base_ty: primary_key_ty,
        fn_key: fn_primary_key,
        builder_append_value: _builder_append_primary_key,
        index: primary_key_index,
    } = primary_key;

    quote! {
        impl ::tonbo::record::Record for #struct_name {
            type Columns = #struct_arrays_name;

            type Key = #primary_key_ty;

            type Ref<'r> = #struct_ref_name<'r>
            where
                Self: 'r;

            fn key(&self) -> <<Self as ::tonbo::record::Record>::Key as ::tonbo::record::Key>::Ref<'_> {
                #fn_primary_key
            }

            fn primary_key_index() -> usize {
                #primary_key_index
            }

            fn primary_key_path() -> (::tonbo::parquet::schema::types::ColumnPath, Vec<::tonbo::parquet::format::SortingColumn>) {
                (
                    ::tonbo::parquet::schema::types::ColumnPath::new(vec!["_ts".to_string(), stringify!(#primary_key_name).to_string()]),
                    vec![::tonbo::parquet::format::SortingColumn::new(1_i32, true, true), ::tonbo::parquet::format::SortingColumn::new(#primary_key_index as i32, false, true)]
                )
            }

            fn as_record_ref(&self) -> Self::Ref<'_> {
                #struct_ref_name {
                    #(#to_ref_init_fields)*
                }
            }

            fn arrow_schema() -> &'static ::std::sync::Arc<::tonbo::arrow::datatypes::Schema> {
                static SCHEMA: ::tonbo::once_cell::sync::Lazy<::std::sync::Arc<::tonbo::arrow::datatypes::Schema>> = ::tonbo::once_cell::sync::Lazy::new(|| {
                    ::std::sync::Arc::new(::tonbo::arrow::datatypes::Schema::new(vec![
                        ::tonbo::arrow::datatypes::Field::new("_null", ::tonbo::arrow::datatypes::DataType::Boolean, false),
                        ::tonbo::arrow::datatypes::Field::new("_ts", ::tonbo::arrow::datatypes::DataType::UInt32, false),
                        #(#schema_fields)*
                    ]))
                });

                &SCHEMA
            }

            fn size(&self) -> usize {
                0 #(#size_fields)*
            }
        }

    }
}

fn trait_decode_codegen(struct_name: &Ident, fields: &[RecordStructFieldOpt]) -> TokenStream {
    let mut decode_method_fields: Vec<TokenStream> = Vec::new();
    let mut field_names: Vec<TokenStream> = Vec::new();

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();

        let (data_type, is_nullable) = field.to_data_type().expect("unreachable code");

        let field_ty = data_type.to_field_ty();

        field_names.push(quote!(#field_name,));

        if field.primary_key.unwrap_or_default() {
            decode_method_fields.push(quote! {
                            let #field_name = #field_ty::decode(reader).await.map_err(|err| ::tonbo::record::RecordDecodeError::Decode {
                                field_name: stringify!(#field_name).to_string(),
                                error: Box::new(err),
                            })?;
                        });
        } else if is_nullable {
            decode_method_fields.push(quote! {
                                let #field_name = Option::<#field_ty>::decode(reader).await.map_err(|err| ::tonbo::record::RecordDecodeError::Decode {
                                    field_name: stringify!(#field_name).to_string(),
                                    error: Box::new(err),
                                })?;
                            });
        } else {
            decode_method_fields.push(quote! {
                                let #field_name = Option::<#field_ty>::decode(reader).await.map_err(|err| ::tonbo::record::RecordDecodeError::Decode {
                                    field_name: stringify!(#field_name).to_string(),
                                    error: Box::new(err),
                                })?.unwrap();
                            });
        }
    }
    quote! {

        impl ::tonbo::serdes::Decode for #struct_name {
            type Error = ::tonbo::record::RecordDecodeError;

            async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
            where
                R: ::tokio::io::AsyncRead + Unpin,
            {
                #(#decode_method_fields)*

                Ok(Self {
                    #(#field_names)*
                })
            }
        }
    }
}

fn struct_ref_codegen(struct_name: &Ident, fields: &[RecordStructFieldOpt]) -> TokenStream {
    let struct_ref_name = struct_name.to_ref_ident();
    let mut ref_fields: Vec<TokenStream> = Vec::new();

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();

        let (data_type, _is_nullable) = field.to_data_type().expect("unreachable code");

        let is_string = matches!(data_type, DataType::String);
        let is_bytes = matches!(data_type, DataType::Bytes);
        let field_ty = data_type.to_field_ty();

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

    quote! {

        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        pub struct #struct_ref_name<'r> {
            #(#ref_fields)*
        }
    }
}

fn trait_decode_ref_codegen(
    struct_name: &&Ident,
    primary_key_name: &Ident,
    fields: &[RecordStructFieldOpt],
) -> TokenStream {
    let mut ref_projection_fields: Vec<TokenStream> = Vec::new();

    let mut from_record_batch_fields: Vec<TokenStream> = Vec::new();
    let mut field_names: Vec<TokenStream> = Vec::new();

    for (i, field) in fields.iter().enumerate() {
        let field_name = field.ident.as_ref().unwrap();
        let field_array_name = field.to_array_ident();
        let field_index = i + 2;

        let (data_type, is_nullable) = field.to_data_type().expect("unreachable code");

        let as_method = data_type.to_as_method();

        field_names.push(quote!(#field_name,));

        if field.primary_key.unwrap_or_default() {
            from_record_batch_fields.push(quote! {
                let #field_name = record_batch
                    .column(column_i)
                    .#as_method
                    .value(offset);
                column_i += 1;
            });
        } else {
            ref_projection_fields.push(quote! {
                if !projection_mask.leaf_included(#field_index) {
                    self.#field_name = None;
                }
            });

            if is_nullable {
                from_record_batch_fields.push(quote! {
                    let mut #field_name = None;

                    if projection_mask.leaf_included(#field_index) {
                        let #field_array_name = record_batch
                            .column(column_i)
                            .#as_method;

                        use ::tonbo::arrow::array::Array;
                        if !#field_array_name.is_null(offset) {
                            #field_name = Some(#field_array_name.value(offset));
                        }
                        column_i += 1;
                    }
                });
            } else {
                from_record_batch_fields.push(quote! {
                    let mut #field_name = None;

                    if projection_mask.leaf_included(#field_index) {
                        #field_name = Some(
                            record_batch
                                .column(column_i)
                                .#as_method
                                .value(offset),
                        );
                        column_i += 1;
                    }
                });
            }
        }
    }

    let struct_ref_name = struct_name.to_ref_ident();

    quote! {
        impl<'r> ::tonbo::record::RecordRef<'r> for #struct_ref_name<'r> {
            type Record = #struct_name;

            fn key(self) -> <<Self::Record as ::tonbo::record::Record>::Key as ::tonbo::record::Key>::Ref<'r> {
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
            ) -> ::tonbo::record::internal::InternalRecordRef<'r, Self> {
                use ::tonbo::arrow::array::AsArray;

                let mut column_i = 2;
                let null = record_batch.column(0).as_boolean().value(offset);

                let ts = record_batch
                    .column(1)
                    .as_primitive::<::tonbo::arrow::datatypes::UInt32Type>()
                    .value(offset)
                    .into();

                #(#from_record_batch_fields)*

                let record = #struct_ref_name {
                    #(#field_names)*
                };
                ::tonbo::record::internal::InternalRecordRef::new(ts, record, null)
            }
        }
    }
}

fn trait_encode_codegen(struct_name: &Ident, fields: &[RecordStructFieldOpt]) -> TokenStream {
    let mut encode_method_fields: Vec<TokenStream> = Vec::new();
    let mut encode_size_fields: Vec<TokenStream> = Vec::new();

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();

        encode_method_fields.push(quote! {
                    ::tonbo::serdes::Encode::encode(&self.#field_name, writer).await.map_err(|err| ::tonbo::record::RecordEncodeError::Encode {
                        field_name: stringify!(#field_name).to_string(),
                        error: Box::new(err),
                    })?;
                });
        encode_size_fields.push(quote! {
            + self.#field_name.size()
        });
    }

    let struct_ref_name = struct_name.to_ref_ident();

    quote! {
        impl<'r> ::tonbo::serdes::Encode for #struct_ref_name<'r> {
            type Error = ::tonbo::record::RecordEncodeError;

            async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
            where
                W: ::tokio::io::AsyncWrite + Unpin + Send,
            {
                #(#encode_method_fields)*

                Ok(())
            }

            fn size(&self) -> usize {
                0 #(#encode_size_fields)*
            }
        }
    }
}

fn struct_array_codegen(struct_name: &Ident, fields: &[RecordStructFieldOpt]) -> TokenStream {
    let struct_arrays_name = struct_name.to_immutable_array_ident();
    let mut arrays_init_fields: Vec<TokenStream> = Vec::new();

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();

        let (data_type, _is_nullable) = field.to_data_type().expect("unreachable code");
        let array_ty = data_type.to_array_ty();

        arrays_init_fields.push(quote! {
            #field_name: ::std::sync::Arc<#array_ty>,
        });
    }

    quote! {
        #[derive(Debug)]
        pub struct #struct_arrays_name {
            _null: ::std::sync::Arc<::tonbo::arrow::array::BooleanArray>,
            _ts: ::std::sync::Arc<::tonbo::arrow::array::UInt32Array>,

            #(#arrays_init_fields)*

            record_batch: ::tonbo::arrow::record_batch::RecordBatch,
        }
    }
}

fn trait_arrow_array_codegen(
    struct_name: &Ident,
    primary_key_name: &Ident,

    fields: &[RecordStructFieldOpt],
) -> TokenStream {
    let struct_builder_name = struct_name.to_builder_ident();
    let mut field_names: Vec<TokenStream> = Vec::new();

    let mut builder_init_fields: Vec<TokenStream> = Vec::new();
    let mut arrays_get_fields: Vec<TokenStream> = Vec::new();

    for (i, field) in fields.iter().enumerate() {
        let field_name = field.ident.as_ref().unwrap();
        let field_index = i + 2;

        let (data_type, is_nullable) = field.to_data_type().expect("unreachable code");

        let builder_with_capacity_method = data_type.to_builder_with_capacity_method();

        field_names.push(quote!(#field_name,));

        builder_init_fields.push(quote! {
            #field_name: #builder_with_capacity_method,
        });

        if field.primary_key.unwrap_or_default() {
            arrays_get_fields.push(quote! {
               let #field_name = self.#field_name.value(offset);
            });
        } else if is_nullable {
            arrays_get_fields.push(quote! {
                                use ::tonbo::arrow::array::Array;
                                let #field_name = (!self.#field_name.is_null(offset) && projection_mask.leaf_included(#field_index))
                                    .then(|| self.#field_name.value(offset));
                            });
        } else {
            arrays_get_fields.push(quote! {
                let #field_name = projection_mask
                    .leaf_included(#field_index)
                    .then(|| self.#field_name.value(offset));
            });
        }
    }

    let struct_ref_name = struct_name.to_ref_ident();
    let struct_arrays_name = struct_name.to_immutable_array_ident();

    quote! {
        impl ::tonbo::inmem::immutable::ArrowArrays for #struct_arrays_name {
            type Record = #struct_name;

            type Builder = #struct_builder_name;

            fn builder(schema: &::std::sync::Arc<::tonbo::arrow::datatypes::Schema>, capacity: usize) -> Self::Builder {
                #struct_builder_name {
                    #(#builder_init_fields)*

                    _null: ::tonbo::arrow::array::BooleanBufferBuilder::new(capacity),
                    _ts: ::tonbo::arrow::array::UInt32Builder::with_capacity(capacity),
                }
            }

            fn get(
                &self,
                offset: u32,
                projection_mask: &::tonbo::parquet::arrow::ProjectionMask,
            ) -> Option<Option<<Self::Record as ::tonbo::record::Record>::Ref<'_>>> {
                let offset = offset as usize;

                if offset >= ::tonbo::arrow::array::Array::len(self.#primary_key_name.as_ref()) {
                    return None;
                }
                if self._null.value(offset) {
                    return Some(None);
                }

                #(#arrays_get_fields)*

                Some(Some(#struct_ref_name {
                    #(#field_names)*
                }))
            }

            fn as_record_batch(&self) -> &::tonbo::arrow::record_batch::RecordBatch {
                &self.record_batch
            }
        }
    }
}

fn struct_builder_codegen(
    struct_name: &Ident,
    builder_append_primary_key: &TokenStream,

    fields: &[RecordStructFieldOpt],
) -> TokenStream {
    let struct_builder_name = struct_name.to_builder_ident();
    let mut field_names: Vec<TokenStream> = Vec::new();

    let mut builder_fields: Vec<TokenStream> = Vec::new();
    let mut builder_finish_fields: Vec<TokenStream> = Vec::new();
    let mut builder_as_any_fields: Vec<TokenStream> = Vec::new();
    // only normal fields
    let mut builder_push_some_fields: Vec<TokenStream> = Vec::new();
    // only normal fields
    let mut builder_push_none_fields: Vec<TokenStream> = Vec::new();
    let mut builder_size_fields: Vec<TokenStream> = Vec::new();

    for field in fields.iter() {
        let field_name = field.ident.as_ref().unwrap();

        let (data_type, is_nullable) = field.to_data_type().expect("unreachable code");

        let is_string = matches!(data_type, DataType::String);
        let is_bytes = matches!(data_type, DataType::Bytes);
        let builder = data_type.to_builder();
        let size_method = data_type.to_size_method(field_name);

        field_names.push(quote!(#field_name,));

        builder_fields.push(quote! {
            #field_name: #builder,
        });
        builder_finish_fields.push(quote! {
            let #field_name = ::std::sync::Arc::new(self.#field_name.finish());
        });
        builder_as_any_fields.push(quote! {
                    ::std::sync::Arc::clone(&#field_name) as ::std::sync::Arc<dyn ::tonbo::arrow::array::Array>,
                });

        builder_size_fields.push(quote! {
            + #size_method
        });

        if field.primary_key.unwrap_or_default() {
        } else if is_nullable {
            builder_push_some_fields.push(quote! {
                match row.#field_name {
                    Some(#field_name) => self.#field_name.append_value(#field_name),
                    None => self.#field_name.append_null(),
                }
            });
            builder_push_none_fields.push(quote! {
                self.#field_name.append_null();
            });
        } else {
            builder_push_some_fields.push(quote! {
                self.#field_name.append_value(row.#field_name.unwrap());
            });
            builder_push_none_fields.push(if is_string {
                quote!(self.#field_name.append_value("");)
            } else if is_bytes {
                quote!(self.#field_name.append_value(&[]);)
            } else {
                quote!(self.#field_name.append_value(Default::default());)
            });
        }
    }

    let struct_ref_name = struct_name.to_ref_ident();
    let struct_arrays_name = struct_name.to_immutable_array_ident();

    quote! {
        pub struct #struct_builder_name {
            #(#builder_fields)*

            _null: ::tonbo::arrow::array::BooleanBufferBuilder,
            _ts: ::tonbo::arrow::array::UInt32Builder,
        }

        impl ::tonbo::inmem::immutable::Builder<#struct_arrays_name> for #struct_builder_name {
            fn push(&mut self, key: ::tonbo::timestamp::timestamped::Timestamped<<<#struct_name as ::tonbo::record::Record>::Key as ::tonbo::record::Key>::Ref<'_>>, row: Option<#struct_ref_name>) {
                #builder_append_primary_key
                match row {
                    Some(row) => {
                        #(#builder_push_some_fields)*

                        self._null.append(false);
                        self._ts.append_value(key.ts.into());
                    }
                    None => {
                        #(#builder_push_none_fields)*

                        self._null.append(true);
                        self._ts.append_value(key.ts.into());
                    }
                }
            }

            fn written_size(&self) -> usize {
                self._null.as_slice().len() + ::std::mem::size_of_val(self._ts.values_slice()) #(#builder_size_fields)*
            }

            fn finish(&mut self, indices: Option<&[usize]>) -> #struct_arrays_name {
                #(#builder_finish_fields)*

                let _null = ::std::sync::Arc::new(::tonbo::arrow::array::BooleanArray::new(self._null.finish(), None));
                let _ts = ::std::sync::Arc::new(self._ts.finish());
                let mut record_batch = ::tonbo::arrow::record_batch::RecordBatch::try_new(
                    ::std::sync::Arc::clone(
                        <<#struct_arrays_name as ::tonbo::inmem::immutable::ArrowArrays>::Record as ::tonbo::record::Record>::arrow_schema(),
                    ),
                    vec![
                        ::std::sync::Arc::clone(&_null) as ::std::sync::Arc<dyn ::tonbo::arrow::array::Array>,
                        ::std::sync::Arc::clone(&_ts) as ::std::sync::Arc<dyn ::tonbo::arrow::array::Array>,

                        #(#builder_as_any_fields)*
                    ],
                )
                .expect("create record batch must be successful");
                if let Some(indices) = indices {
                    record_batch = record_batch
                        .project(indices)
                        .expect("projection indices must be successful");
                }

                #struct_arrays_name {
                    #(#field_names)*

                    _null,
                    _ts,
                    record_batch,
                }
            }
        }
    }
}
