mod keys;
mod schema_model;

use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, GenericArgument, Path, Type};

use crate::{keys::PrimaryKey, schema_model::ModelAttributes};

enum DataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    Boolean,
}

#[proc_macro_attribute]
pub fn morsel_record(_args: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = ast.ident.clone();

    let mut primary_key_definitions = None;

    let mut to_ref_init_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut schema_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut ref_fields: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut from_record_batch_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut field_names: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut arrays_init_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut builder_init_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut arrays_get_fields: Vec<proc_macro2::TokenStream> = Vec::new();

    let mut builder_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut builder_finish_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut builder_as_any_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    // only normal fields
    let mut builder_push_some_fields: Vec<proc_macro2::TokenStream> = Vec::new();
    // only normal fields
    let mut builder_push_none_fields: Vec<proc_macro2::TokenStream> = Vec::new();

    if let Data::Struct(data_struct) = &ast.data {
        if let Fields::Named(fields) = &data_struct.fields {
            for (i, field) in fields.named.iter().enumerate() {
                let field_name = field.ident.as_ref().unwrap();
                let field_array_name =
                    Ident::new(&format!("{}_array", field_name), field_name.span());
                let field_index = i + 2;

                let mut is_string = false;
                let (
                    is_nullable,
                    field_ty,
                    mapped_type,
                    array_ty,
                    as_method,
                    builder_with_capacity_method,
                    builder,
                    default,
                ) = match to_data_type(&field.ty) {
                    Some((DataType::UInt8, is_nullable)) => (
                        is_nullable,
                        quote!(u8),
                        quote!(DataType::UInt8),
                        quote!(UInt8Array),
                        quote!(as_primitive::<UInt8Type>()),
                        quote!(PrimitiveBuilder::<UInt8Type>::with_capacity(capacity)),
                        quote!(PrimitiveBuilder<UInt8Type>),
                        quote!(0),
                    ),
                    Some((DataType::UInt16, is_nullable)) => (
                        is_nullable,
                        quote!(u16),
                        quote!(DataType::UInt16),
                        quote!(UInt16Array),
                        quote!(as_primitive::<UInt16Type>()),
                        quote!(PrimitiveBuilder::<UInt16Type>::with_capacity(capacity)),
                        quote!(PrimitiveBuilder<UInt16Type>),
                        quote!(0),
                    ),
                    Some((DataType::UInt32, is_nullable)) => (
                        is_nullable,
                        quote!(u32),
                        quote!(DataType::UInt32),
                        quote!(UInt32Array),
                        quote!(as_primitive::<UInt32Type>()),
                        quote!(PrimitiveBuilder::<UInt32Type>::with_capacity(capacity)),
                        quote!(PrimitiveBuilder<UInt32Type>),
                        quote!(0),
                    ),
                    Some((DataType::UInt64, is_nullable)) => (
                        is_nullable,
                        quote!(u64),
                        quote!(DataType::UInt64),
                        quote!(UInt64Array),
                        quote!(as_primitive::<UInt64Type>()),
                        quote!(PrimitiveBuilder::<UInt64Type>::with_capacity(capacity)),
                        quote!(PrimitiveBuilder<UInt64Type>),
                        quote!(0),
                    ),

                    Some((DataType::Int8, is_nullable)) => (
                        is_nullable,
                        quote!(i8),
                        quote!(DataType::Int8),
                        quote!(Int8Array),
                        quote!(as_primitive::<Int8Type>()),
                        quote!(PrimitiveBuilder::<Int8Type>::with_capacity(capacity)),
                        quote!(PrimitiveBuilder<Int8Type>),
                        quote!(0),
                    ),
                    Some((DataType::Int16, is_nullable)) => (
                        is_nullable,
                        quote!(i16),
                        quote!(DataType::Int16),
                        quote!(Int16Array),
                        quote!(as_primitive::<Int16Type>()),
                        quote!(PrimitiveBuilder::<Int16Type>::with_capacity(capacity)),
                        quote!(PrimitiveBuilder<Int16Type>),
                        quote!(0),
                    ),
                    Some((DataType::Int32, is_nullable)) => (
                        is_nullable,
                        quote!(i32),
                        quote!(DataType::Int32),
                        quote!(Int32Array),
                        quote!(as_primitive::<Int32Type>()),
                        quote!(PrimitiveBuilder::<Int32Type>::with_capacity(capacity)),
                        quote!(PrimitiveBuilder<Int32Type>),
                        quote!(0),
                    ),
                    Some((DataType::Int64, is_nullable)) => (
                        is_nullable,
                        quote!(i64),
                        quote!(DataType::Int64),
                        quote!(Int64Array),
                        quote!(as_primitive::<Int64Type>()),
                        quote!(PrimitiveBuilder::<Int64Type>::with_capacity(capacity)),
                        quote!(PrimitiveBuilder<Int64Type>),
                        quote!(0),
                    ),

                    Some((DataType::String, is_nullable)) => {
                        is_string = true;
                        (
                            is_nullable,
                            quote!(String),
                            quote!(DataType::Utf8),
                            quote!(StringArray),
                            quote!(as_string::<i32>()),
                            quote!(StringBuilder::with_capacity(capacity, 0)),
                            quote!(StringBuilder),
                            quote!(""),
                        )
                    }
                    Some((DataType::Boolean, is_nullable)) => (
                        is_nullable,
                        quote!(bool),
                        quote!(DataType::Boolean),
                        quote!(BooleanArray),
                        quote!(as_boolean()),
                        quote!(BooleanBuilder::with_capacity(capacity)),
                        quote!(BooleanBuilder),
                        quote!(false),
                    ),

                    None => unreachable!(),
                };

                schema_fields.push(quote! {
                    Field::new(stringify!(#field_name), #mapped_type, #is_nullable),
                });
                field_names.push(quote! (#field_name,));
                arrays_init_fields.push(quote! {
                    #field_name: Arc<#array_ty>,
                });
                builder_init_fields.push(quote! {
                    #field_name: #builder_with_capacity_method,
                });
                builder_fields.push(quote! {
                    #field_name: #builder,
                });
                builder_finish_fields.push(quote! {
                    let #field_name = Arc::new(self.#field_name.finish());
                });
                builder_as_any_fields.push(quote! {
                    Arc::clone(&#field_name) as Arc<dyn Array>,
                });

                match ModelAttributes::parse_field(field) {
                    Ok(false) => {
                        match (is_nullable, is_string) {
                            (true, true) => {
                                to_ref_init_fields.push(quote! { #field_name: &self.#field_name, });
                            }
                            (true, false) => {
                                to_ref_init_fields.push(quote! { #field_name: self.#field_name, });
                            }
                            (false, true) => {
                                to_ref_init_fields
                                    .push(quote! { #field_name: Some(&self.#field_name), });
                            }
                            (false, false) => {
                                to_ref_init_fields
                                    .push(quote! { #field_name: Some(self.#field_name), });
                            }
                        }
                        if is_string {
                            ref_fields.push(quote! { pub #field_name: Option<&'r str>, });
                        } else {
                            ref_fields.push(quote! { pub #field_name: Option<#field_ty>, });
                        }
                        if is_nullable {
                            from_record_batch_fields.push(quote! {
                                let mut #field_name = None;

                                if projection_mask.leaf_included(#field_index) {
                                    let #field_array_name = record_batch
                                        .column(column_i)
                                        .#as_method;

                                    if !#field_array_name.is_null(offset) {
                                        #field_name = Some(#field_array_name.value(offset));
                                    }
                                    column_i += 1;
                                }
                            });
                            arrays_get_fields.push(quote! {
                                let #field_name = (!self.#field_name.is_null(offset) && projection_mask.leaf_included(#field_index))
                                    .then(|| self.#field_name.value(offset));
                            });
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
                            arrays_get_fields.push(quote! {
                                let #field_name = projection_mask
                                    .leaf_included(#field_index)
                                    .then(|| self.#field_name.value(offset));
                            });
                            builder_push_some_fields.push(quote! {
                                self.#field_name.append_value(row.#field_name.unwrap());
                            });
                            builder_push_none_fields.push(quote! {
                                self.#field_name.append_value(#default);
                            });
                        }
                    }
                    Ok(true) => {
                        primary_key_definitions = Some(PrimaryKey {
                            name: field_name.clone(),
                            builder_append_value: quote! {
                                self.#field_name.append_value(key.value);
                            },
                            base_ty: field.ty.clone(),
                        });

                        if is_nullable {
                            return syn::Error::new_spanned(
                                ast.ident,
                                "primary key cannot be nullable",
                            )
                            .to_compile_error()
                            .into();
                        }
                        if is_string {
                            to_ref_init_fields.push(quote! { #field_name: &self.#field_name, });
                            ref_fields.push(quote! { pub #field_name: &'r str, });
                        } else {
                            to_ref_init_fields.push(quote! { #field_name: self.#field_name, });
                            ref_fields.push(quote! { pub #field_name: #field_ty, });
                        }
                        from_record_batch_fields.push(quote! {
                            let #field_name = record_batch
                                .column(column_i)
                                .#as_method
                                .value(offset);
                            column_i += 1;
                        });
                        arrays_get_fields.push(quote! {
                           let #field_name = self.#field_name.value(offset);
                        });
                    }
                    Err(err) => return TokenStream::from(err.to_compile_error()),
                }
            }
        }
    } else {
        return syn::Error::new_spanned(ast.ident, "This macro only supports structs")
            .to_compile_error()
            .into();
    }
    let PrimaryKey {
        name: primary_key_name,
        base_ty,
        builder_append_value: builder_append_primary_key,
    } = primary_key_definitions.unwrap();

    let struct_ref_name = Ident::new(&format!("{}Ref", struct_name), struct_name.span());
    let struct_arrays_name = Ident::new(
        &format!("{}ImmutableArrays", struct_name),
        struct_name.span(),
    );
    let struct_builder_name = Ident::new(&format!("{}Builder", struct_name), struct_name.span());

    let gen = quote! {
        #[derive(morseldb_marco::KeyAttributes, Debug, PartialEq, Eq, Clone)]
        #ast

        impl Record for #struct_name {
            type Columns = #struct_arrays_name;

            type Key = #base_ty;

            type Ref<'r> = #struct_ref_name<'r>
            where
                Self: 'r;

            fn key(&self) -> <<Self as Record>::Key as Key>::Ref<'_> {
                &self.#primary_key_name
            }

            fn as_record_ref(&self) -> Self::Ref<'_> {
                #struct_ref_name {
                    #(#to_ref_init_fields)*
                }
            }

            fn arrow_schema() -> &'static Arc<Schema> {
                static SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
                    Arc::new(Schema::new(vec![
                        Field::new("_null", DataType::Boolean, false),
                        Field::new("_ts", DataType::UInt32, false),
                        #(#schema_fields)*
                    ]))
                });

                &SCHEMA
            }
        }

        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        pub struct #struct_ref_name<'r> {
            #(#ref_fields)*
        }

        impl<'r> RecordRef<'r> for #struct_ref_name<'r> {
            type Record = #struct_name;

            fn key(self) -> <<Self::Record as Record>::Key as crate::record::Key>::Ref<'r> {
                self.#primary_key_name
            }

            fn from_record_batch(
                record_batch: &'r RecordBatch,
                offset: usize,
                projection_mask: &'r ProjectionMask,
            ) -> InternalRecordRef<'r, Self> {
                let mut column_i = 2;
                let null = record_batch.column(0).as_boolean().value(offset);

                let ts = record_batch
                    .column(1)
                    .as_primitive::<UInt32Type>()
                    .value(offset)
                    .into();

                #(#from_record_batch_fields)*

                let record = TestRef {
                    #(#field_names)*
                };
                InternalRecordRef::new(ts, record, null)
            }
        }

        #[derive(Debug)]
        pub struct #struct_arrays_name {
            _null: Arc<BooleanArray>,
            _ts: Arc<UInt32Array>,

            #(#arrays_init_fields)*

            record_batch: RecordBatch,
        }

        impl ArrowArrays for #struct_arrays_name {
            type Record = #struct_name;

            type Builder = #struct_builder_name;

            fn builder(capacity: usize) -> Self::Builder {
                TestBuilder {
                    #(#builder_init_fields)*

                    _null: BooleanBufferBuilder::new(capacity),
                    _ts: UInt32Builder::with_capacity(capacity),
                }
            }

            fn get(
                &self,
                offset: u32,
                projection_mask: &ProjectionMask,
            ) -> Option<Option<<Self::Record as Record>::Ref<'_>>> {
                let offset = offset as usize;

                if offset >= self.vstring.len() {
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

            fn as_record_batch(&self) -> &RecordBatch {
                &self.record_batch
            }
        }

        pub struct #struct_builder_name {
            #(#builder_fields)*

            _null: BooleanBufferBuilder,
            _ts: UInt32Builder,
        }

        impl Builder<TestImmutableArrays> for #struct_builder_name {
            fn push(&mut self, key: Timestamped<&str>, row: Option<TestRef>) {
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

            fn finish(&mut self) -> #struct_arrays_name {
                #(#builder_finish_fields)*

                let _null = Arc::new(BooleanArray::new(self._null.finish(), None));
                let _ts = Arc::new(self._ts.finish());
                let record_batch = RecordBatch::try_new(
                    Arc::clone(
                        <<#struct_arrays_name as ArrowArrays>::Record as Record>::arrow_schema(),
                    ),
                    vec![
                        Arc::clone(&_null) as Arc<dyn Array>,
                        Arc::clone(&_ts) as Arc<dyn Array>,

                        #(#builder_as_any_fields)*
                    ],
                )
                .expect("create record batch must be successful");

                #struct_arrays_name {
                    #(#field_names)*

                    _null,
                    _ts,
                    record_batch,
                }
            }
        }
    };
    // std::fs::write("../test.rs", gen.to_string()).unwrap();

    gen.into()
}

fn to_data_type(ty: &Type) -> Option<(DataType, bool)> {
    if let Type::Path(type_path) = ty {
        if type_path.path.segments.len() == 1 {
            let segment = &type_path.path.segments[0];
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(ref generic_args) = segment.arguments {
                    if generic_args.args.len() == 1 {
                        return if let GenericArgument::Type(Type::Path(type_path)) =
                            &generic_args.args[0]
                        {
                            Some((path_to_type(&type_path.path), true))
                        } else {
                            None
                        };
                    }
                }
            }
        }
        return Some((path_to_type(&type_path.path), false));
    }
    None
}

fn path_to_type(path: &Path) -> DataType {
    if path.is_ident("u8") {
        DataType::UInt8
    } else if path.is_ident("u16") {
        DataType::UInt16
    } else if path.is_ident("u32") {
        DataType::UInt32
    } else if path.is_ident("u64") {
        DataType::UInt64
    } else if path.is_ident("i8") {
        DataType::Int8
    } else if path.is_ident("i16") {
        DataType::Int16
    } else if path.is_ident("i32") {
        DataType::Int32
    } else if path.is_ident("i64") {
        DataType::Int64
    } else if path.is_ident("String") {
        DataType::String
    } else if path.is_ident("bool") {
        DataType::Boolean
    } else {
        todo!()
    }
}

#[proc_macro_derive(KeyAttributes, attributes(primary_key))]
pub fn key_attributes(_input: TokenStream) -> TokenStream {
    let gen = quote::quote! {};
    gen.into()
}
