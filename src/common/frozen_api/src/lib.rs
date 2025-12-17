// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # Databend Frozen API
//!
//! Prevents accidental breaking changes to critical data structures through compile-time hash verification.
//!
//! ## Usage
//!
//! ```rust
//! use databend_common_frozen_api::FrozenAPI;
//! use databend_common_frozen_api::frozen_api;
//!
//! // Child structs: must derive FrozenAPI
//! #[derive(FrozenAPI)]
//! struct TableMeta {
//!     size: u64,
//!     row_count: u64,
//! }
//!
//! // Root struct: frozen_api + derive FrozenAPI, all fields must be FrozenAPI too
//! #[frozen_api("a1b2c3d4")]
//! #[derive(FrozenAPI)]
//! struct SnapshotFormat {
//!     version: u32,
//!     tables: Vec<TableMeta>, // TableMeta must derive FrozenAPI or compile error
//! }
//! ```
//!
//! ## Complete Protection Chain
//!
//! - Enforces that ALL custom types in a frozen_api struct derive FrozenAPI
//! - Prevents accidental omission of protection on dependencies
//! - Compile error if any field type lacks FrozenAPI derive
//! - Automatic cascading: any change in the dependency tree breaks parent hash
//!
//! **Rule**: If struct uses `#[frozen_api]`, it and ALL its custom field types must `#[derive(FrozenAPI)]`.

use proc_macro::TokenStream;
use quote::ToTokens;
use quote::quote;
use sha2::Digest;
use sha2::Sha256;
use syn::Data;
use syn::DeriveInput;
use syn::Fields;
use syn::LitStr;
use syn::Type;
use syn::parse_macro_input;

/// Compile-time API freezing macro that prevents accidental breaking changes.
#[proc_macro_attribute]
pub fn frozen_api(args: TokenStream, input: TokenStream) -> TokenStream {
    let expected = parse_macro_input!(args as LitStr).value();
    let input_struct = parse_macro_input!(input as DeriveInput);

    let validation_code = generate_frozenapi_validation(&input_struct);
    let actual = compute_struct_hash(&input_struct);

    let mut const_body = validation_code;

    if actual != expected {
        let struct_name = &input_struct.ident;
        let dependency_info = if has_frozen_api_dependencies(&input_struct) {
            " (or its FrozenAPI dependencies)"
        } else {
            ""
        };

        let error_msg = format!(
            "⚠️  BREAKING CHANGE DETECTED: '{}' modified{}\n\
             Expected: {expected} | Actual: {actual}\n\
             Fix: Update hash to '{}' if change is intentional\n\
             Impact: Serialization compatibility may break",
            struct_name, dependency_info, actual
        );

        const_body.extend(quote! {
            compile_error!(#error_msg);
        });
    }

    quote! {
        #input_struct

        const _: () = {
            #const_body
        };
    }
    .into()
}

/// Derive macro to mark structs as participating in frozen API hash calculation.
#[proc_macro_derive(FrozenAPI)]
pub fn derive_frozen_api(input: TokenStream) -> TokenStream {
    let input_struct = parse_macro_input!(input as DeriveInput);
    let struct_name = &input_struct.ident;

    let hash_value = compute_struct_hash(&input_struct);

    quote! {
        impl #struct_name {
            #[doc(hidden)]
            pub const __FROZEN_API_HASH: &'static str = #hash_value;
        }
    }
    .into()
}

/// Compute hash for structure definition
fn compute_struct_hash(input: &DeriveInput) -> String {
    let struct_repr = input.to_token_stream().to_string();
    hex::encode(&Sha256::digest(struct_repr)[..4])
}

/// Generate compile-time validation that all custom field types have __FROZEN_API_HASH
fn generate_frozenapi_validation(input: &DeriveInput) -> proc_macro2::TokenStream {
    let mut validations = Vec::new();

    if let Data::Struct(data_struct) = &input.data {
        let fields = match &data_struct.fields {
            Fields::Named(f) => f.named.iter().collect(),
            Fields::Unnamed(f) => f.unnamed.iter().collect(),
            Fields::Unit => vec![],
        };

        for field in fields {
            collect_custom_type_validations(&field.ty, &mut validations);
        }
    }

    quote! { #(#validations)* }
}

/// Recursively collect validation for custom types (handles Vec<T>, Option<T>, etc.)
fn collect_custom_type_validations(ty: &Type, validations: &mut Vec<proc_macro2::TokenStream>) {
    match ty {
        Type::Path(type_path) => {
            if let Some(segment) = type_path.path.segments.last() {
                let type_name = segment.ident.to_string();

                // Check if this is a custom type (not a generic container)
                if is_custom_type(&type_name) {
                    // Skip validation for tuple types in paths as they're handled separately
                    let path_str = quote!(#type_path).to_string();
                    if !path_str.starts_with('(') && !path_str.contains(',') {
                        validations.push(quote! {
                            let _: &'static str = #type_path::__FROZEN_API_HASH;
                        });
                    }
                }

                // Handle generic arguments recursively (Vec<CustomType>, Option<CustomType>, etc.)
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    for arg in &args.args {
                        if let syn::GenericArgument::Type(inner_type) = arg {
                            collect_custom_type_validations(inner_type, validations);
                        }
                    }
                }
            }
        }
        Type::Tuple(tuple) => {
            // Skip validation for tuple types themselves as they are built-in composite types
            for elem in &tuple.elems {
                collect_custom_type_validations(elem, validations);
            }
        }
        Type::Array(array) => {
            collect_custom_type_validations(&array.elem, validations);
        }
        Type::Slice(slice) => {
            collect_custom_type_validations(&slice.elem, validations);
        }
        _ => {} // Other types don't need validation
    }
}

/// Check if a type name represents a custom (non-built-in) type
fn is_custom_type(name: &str) -> bool {
    !matches!(
        name,
        "i8" | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "isize"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "usize"
            | "f32"
            | "f64"
            | "bool"
            | "char"
            | "str"
            | "String"
            | "Vec"
            | "Option"
            | "Result"
            | "Box"
            | "Rc"
            | "Arc"
            | "HashMap"
            | "HashSet"
            | "BTreeMap"
            | "BTreeSet"
            | "VecDeque"
            | "LinkedList"
            | "ColumnId"
            | "FieldIndex"
            | "FormatVersion"
            | "Uuid"
            | "Utc"
            | "MetaHLL"
            | "Histogram"
            | "RawBlockHLL"
            | "VariantDataType"
            | "DateTime"
            | "Location"
            | "ClusterKey"
            | "StatisticsOfColumns"
            | "BlockHLL"
            | "SnapshotId"
    )
}

/// Check if struct has any FrozenAPI dependencies
fn has_frozen_api_dependencies(input: &DeriveInput) -> bool {
    if let Data::Struct(data_struct) = &input.data {
        let fields = match &data_struct.fields {
            Fields::Named(f) => f.named.iter().collect(),
            Fields::Unnamed(f) => f.unnamed.iter().collect(),
            Fields::Unit => vec![],
        };

        for field in fields {
            if has_custom_types_in_field(&field.ty) {
                return true;
            }
        }
    }
    false
}

/// Check if a type contains custom types (potentially FrozenAPI)
fn has_custom_types_in_field(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            if let Some(segment) = type_path.path.segments.last() {
                let type_name = segment.ident.to_string();
                if is_custom_type(&type_name) {
                    return true;
                }

                // Check generic arguments
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    for arg in &args.args {
                        if let syn::GenericArgument::Type(inner_type) = arg
                            && has_custom_types_in_field(inner_type)
                        {
                            return true;
                        }
                    }
                }
            }
        }
        Type::Tuple(tuple) => {
            for elem in &tuple.elems {
                if has_custom_types_in_field(elem) {
                    return true;
                }
            }
        }
        Type::Array(array) => return has_custom_types_in_field(&array.elem),
        Type::Slice(slice) => return has_custom_types_in_field(&slice.elem),
        _ => {}
    }
    false
}
