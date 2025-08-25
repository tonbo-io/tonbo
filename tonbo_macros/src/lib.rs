mod ident_generator;
mod keys;
mod record;
pub(crate) mod typed_codegen;

pub(crate) mod data_type;

use proc_macro::TokenStream;
use syn::DeriveInput;

// no direct DataType usage here; keep module export for typed_codegen

// Legacy derive removed: use `#[tonbo::typed::record]` single-annotation instead.

/// Attribute macro for typed-arrow integration.
///
/// Usage (single annotation):
///
/// #[tonbo::typed::record]
/// struct MyRow { #[record(primary_key)] id: i64, ... }
///
/// The macro will:
/// - find and strip the `#[record(primary_key)]` field attribute,
/// - inject `#[derive(typed_arrow::Record, ...)]` (preserving any other derives), and
/// - inject `#[schema_metadata(k = "tonbo.primary_key_user_indices", v = "<idx>")]` on the struct.
///
/// It also supports the two-annotation form and will de-duplicate any existing
/// `Record` derive entry to avoid duplicate implementations:
///
/// #[tonbo::typed::record]
/// #[derive(tonbo::typed::Record, Debug)]
/// struct MyRow { ... }
#[proc_macro_attribute]
pub fn typed_record(_args: TokenStream, input: TokenStream) -> TokenStream {
    // Keep a DeriveInput copy for generating Tonbo DB interop code via the existing
    // `tonbo_macros::Record` implementation machinery.
    let derive_input = match syn::parse::<DeriveInput>(input.clone()) {
        Ok(di) => di,
        Err(e) => return e.to_compile_error().into(),
    };

    // Also parse as ItemStruct to rewrite derives and field attrs for typed-arrow.
    let mut item: syn::ItemStruct = match syn::parse(input.clone()) {
        Ok(s) => s,
        Err(e) => return e.to_compile_error().into(),
    };

    // find primary key field index (for typed schema metadata)
    let mut pk_index: Option<usize> = None;
    for (i, field) in item.fields.iter_mut().enumerate() {
        let mut remove_idx: Option<usize> = None;
        for (ai, a) in field.attrs.iter().enumerate() {
            if a.path().is_ident("record") {
                let mut has_pk = false;
                let _ = a.parse_nested_meta(|nm| {
                    if nm.path.is_ident("primary_key") {
                        has_pk = true;
                    }
                    Ok(())
                });
                if has_pk {
                    pk_index = Some(i);
                    // Remove the attribute only from the typed-arrow view.
                    // The copied `DeriveInput` above still contains the attribute and is used
                    // to generate Tonbo's DB interop code (Schema/Record/Arrays/Builder).
                    remove_idx = Some(ai);
                    break;
                }
            }
        }
        if let Some(idx) = remove_idx {
            field.attrs.remove(idx);
        }
    }

    let Some(idx) = pk_index else {
        return syn::Error::new_spanned(
            &item.ident,
            "missing primary key field, use #[record(primary_key)] to define one",
        )
        .to_compile_error()
        .into();
    };

    // Gather and normalize derives: remove any existing `Record` (qualified or not),
    // then prepend `typed_arrow::Record` to ensure it exists exactly once.
    use syn::{punctuated::Punctuated, Attribute, Path, Token};
    let mut other_derives: Vec<Path> = Vec::new();
    let mut retained_attrs: Vec<Attribute> = Vec::with_capacity(item.attrs.len());
    for attr in item.attrs.into_iter() {
        if attr.path().is_ident("derive") {
            // Parse paths inside derive
            if let Ok(paths) = attr.parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated)
            {
                for p in paths.into_iter() {
                    // Keep any derive whose last segment isn't `Record`
                    if p.segments
                        .last()
                        .map(|s| s.ident == "Record")
                        .unwrap_or(false)
                    {
                        // skip existing `Record` to avoid duplicates
                    } else {
                        other_derives.push(p);
                    }
                }
            }
            // Do not retain the original derive attribute; we'll rebuild it
            continue;
        }
        retained_attrs.push(attr);
    }

    // Build a single consolidated derive attribute: #[derive(typed_arrow::Record, <others>...)]
    let mut derive_items: Punctuated<Path, Token![,]> = Punctuated::new();
    derive_items.push(syn::parse_quote!(typed_arrow::Record));
    for p in other_derives {
        derive_items.push(p);
    }
    let new_derive: Attribute = syn::parse_quote! { #[derive(#derive_items)] };

    // inject schema_metadata attribute at the struct level
    let idx_str = idx.to_string();
    let meta_attr: syn::Attribute = syn::parse_quote! {
        #[schema_metadata(k = "tonbo.primary_key_user_indices", v = #idx_str )]
    };
    // Rebuild the attribute list: new derive, existing retained attrs, then metadata
    let mut new_attrs = Vec::with_capacity(retained_attrs.len() + 2);
    new_attrs.push(new_derive);
    new_attrs.extend(retained_attrs);
    new_attrs.push(meta_attr);
    item.attrs = new_attrs;

    // Generate Tonbo DB interop for typed records, minimizing per-type codegen:
    // - impl Record with Schema = TonboTypedSchema<R, PK>
    // - Ref struct + RecordRef impl + Encode/Decode
    // - type alias `<Name>Schema = TonboTypedSchema<...>`
    let tonbo_codegen = match crate::typed_codegen::handle_typed(derive_input) {
        Ok(ts) => ts,
        Err(e) => return e.to_compile_error().into(),
    };

    // output modified struct followed by generated Tonbo code
    let ts: proc_macro2::TokenStream = quote::quote! { #item #tonbo_codegen };
    ts.into()
}
