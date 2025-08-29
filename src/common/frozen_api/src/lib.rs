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
//! use databend_common_frozen_api::frozen_api;
//!
//! // First time: use dummy hash to get real one
//! #[frozen_api("dummy")]
//! struct CriticalStruct {
//!     field: i32,
//! }
//! // Compile error shows: Update hash to 'a1b2c3d4' if change is intentional.
//!
//! // Apply protection with real hash
//! #[frozen_api("a1b2c3d4")]
//! struct CriticalStruct {
//!     field: i32,
//! }
//! // Now any changes require updating the hash!
//! ```
//!
//! ## What Gets Detected
//!
//! - Field names, types, order
//! - Visibility modifiers  
//! - Type names (including nested types)
//! - Generic parameters
//!
//! ## Dependency Chain Protection
//!
//! This implementation enforces **layered protection**: when you use `#[frozen_api]` on a struct,
//! all custom types it references must also be protected with `#[frozen_api]`.
//!
//! ### How it works:
//! - Each struct protects its own direct structure
//! - Dependency validation ensures no "protection gaps"  
//! - Changes anywhere in the chain require explicit approval
//! - Smart error messages guide impact assessment
//!
//! ### Example workflow:
//! ```rust
//! #[frozen_api("hash1")]
//! struct CoreData {
//!     field: i32,
//! }
//! #[frozen_api("hash2")]
//! struct Config {
//!     core: CoreData,
//! }
//! #[frozen_api("hash3")]
//! struct App {
//!     config: Config,
//! }
//! ```
//! When `CoreData` changes → update hash1 → check/update Config → check/update App
//!
//! This ensures comprehensive protection while maintaining clear responsibility boundaries.

use std::collections::HashSet;

use proc_macro::TokenStream;
use quote::quote;
use quote::ToTokens;
use sha2::Digest;
use sha2::Sha256;
use syn::parse_macro_input;
use syn::DeriveInput;
use syn::Fields;
use syn::LitStr;
use syn::Type;

/// Extract custom type names that need frozen_api protection
fn extract_custom_dependencies(input: &DeriveInput) -> Vec<String> {
    let mut deps = HashSet::new();

    if let syn::Data::Struct(data_struct) = &input.data {
        match &data_struct.fields {
            Fields::Named(fields) => {
                for field in &fields.named {
                    extract_custom_types(&field.ty, &mut deps);
                }
            }
            Fields::Unnamed(fields) => {
                for field in &fields.unnamed {
                    extract_custom_types(&field.ty, &mut deps);
                }
            }
            Fields::Unit => {}
        }
    }

    let mut result: Vec<String> = deps.into_iter().collect();
    result.sort();
    result
}

/// Extract custom type names from field types (excluding built-in types)
fn extract_custom_types(ty: &Type, deps: &mut HashSet<String>) {
    match ty {
        Type::Path(type_path) => {
            for segment in &type_path.path.segments {
                let type_name = segment.ident.to_string();
                if !is_builtin_type(&type_name) {
                    deps.insert(type_name);
                }

                // Handle generic parameters
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    for arg in &args.args {
                        if let syn::GenericArgument::Type(inner_ty) = arg {
                            extract_custom_types(inner_ty, deps);
                        }
                    }
                }
            }
        }
        Type::Reference(type_ref) => {
            extract_custom_types(&type_ref.elem, deps);
        }
        Type::Array(type_array) => {
            extract_custom_types(&type_array.elem, deps);
        }
        Type::Tuple(type_tuple) => {
            for elem in &type_tuple.elems {
                extract_custom_types(elem, deps);
            }
        }
        _ => {}
    }
}

/// Check if a type is a built-in Rust type that doesn't need frozen_api protection
fn is_builtin_type(type_name: &str) -> bool {
    matches!(
        type_name,
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
            | "HashMap"
            | "HashSet"
            | "BTreeMap"
            | "BTreeSet"
            | "Box"
            | "Rc"
            | "Arc"
            | "Cell"
            | "RefCell"
            | "Cow"
            | "Path"
            | "PathBuf"
            | "OsString"
            | "OsStr"
            | "DateTime"
            | "Utc"
            | "Uuid"
        // Databend stable foundation types
            | "TableSchema"
            | "TableDataType" 
            | "TableField"
            | "ColumnId"
            | "FormatVersion"
            | "SnapshotId"
            | "NativeColumnMeta"
            | "MetaEncoding"
            | "MetaCompression"
            | "RawBlockHLL"
            | "Histogram"
            | "MetaHLL"
            | "VariantDataType"
            | "Location"
            | "ClusterKey"
    )
}

/// Compute hash of the struct definition
fn compute_struct_hash(input: &DeriveInput) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.to_token_stream().to_string());
    hex::encode(&hasher.finalize()[..4])
}

/// Generate dependency validation code
fn generate_dependency_checks(dependencies: &[String]) -> proc_macro2::TokenStream {
    let dep_idents: Vec<_> = dependencies
        .iter()
        .map(|dep| syn::Ident::new(dep, proc_macro2::Span::call_site()))
        .collect();

    quote! {
        const _: () = {
            #(
                const _: bool = #dep_idents::__FROZEN_API_PROTECTED;
            )*
        };
    }
}

/// Compile-time API freezing macro that prevents accidental breaking changes.
#[proc_macro_attribute]
pub fn frozen_api(args: TokenStream, input: TokenStream) -> TokenStream {
    let expected = parse_macro_input!(args as LitStr).value();
    let input_struct = parse_macro_input!(input as DeriveInput);
    let struct_name = &input_struct.ident;

    let actual = compute_struct_hash(&input_struct);

    if actual != expected {
        let error_msg = format!(
            "FROZEN API CHANGED: '{}' structure modified\n\
             Expected hash: {expected}\n\
             Actual hash:   {actual}\n\
             \n\
             To fix: Update hash to '{}' if change is intentional\n\
             Note: Other structs using '{}' may also need hash updates",
            struct_name, actual, struct_name
        );

        return syn::Error::new_spanned(&input_struct.ident, error_msg)
            .to_compile_error()
            .into();
    }

    let custom_dependencies = extract_custom_dependencies(&input_struct);
    let dependency_checks = if !custom_dependencies.is_empty() {
        generate_dependency_checks(&custom_dependencies)
    } else {
        quote! {}
    };

    quote! {
        #input_struct

        impl #struct_name {
            #[doc(hidden)]
            pub const __FROZEN_API_PROTECTED: bool = true;
        }

        #dependency_checks
    }
    .into()
}
