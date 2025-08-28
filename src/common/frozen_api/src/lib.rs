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
//! ## Nested Dependencies
//!
//! For comprehensive protection, also apply `#[frozen_api]` to all nested types.
//! Changes to nested types will only be detected if they're also protected.
//!
//! Use on structs that are serialized, part of network protocols, or must maintain compatibility.

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

/// Extract type names from struct fields for deep hashing
fn extract_field_types(input: &DeriveInput) -> Vec<String> {
    let mut types = Vec::new();

    if let syn::Data::Struct(data_struct) = &input.data {
        match &data_struct.fields {
            Fields::Named(fields) => {
                for field in &fields.named {
                    types.push(type_to_string(&field.ty));
                }
            }
            Fields::Unnamed(fields) => {
                for field in &fields.unnamed {
                    types.push(type_to_string(&field.ty));
                }
            }
            Fields::Unit => {}
        }
    }

    types
}

/// Convert a Type to string representation
fn type_to_string(ty: &Type) -> String {
    match ty {
        Type::Path(type_path) => type_path
            .path
            .segments
            .iter()
            .map(|seg| seg.ident.to_string())
            .collect::<Vec<_>>()
            .join("::"),
        _ => ty.to_token_stream().to_string(),
    }
}

/// Compute deep hash including nested type information
fn compute_deep_hash(input: &DeriveInput) -> String {
    let mut hasher = Sha256::new();

    // Hash the main structure definition
    hasher.update(input.to_token_stream().to_string());

    // Hash the types of all fields for deeper analysis
    let field_types = extract_field_types(input);
    for field_type in field_types {
        hasher.update(field_type);
        hasher.update(b"|"); // separator
    }

    hex::encode(&hasher.finalize()[..4])
}

/// Compile-time API freezing macro that prevents accidental breaking changes.
#[proc_macro_attribute]
pub fn frozen_api(args: TokenStream, input: TokenStream) -> TokenStream {
    let expected = parse_macro_input!(args as LitStr).value();
    let input_struct = parse_macro_input!(input as DeriveInput);

    // Compute deep hash including nested type information
    let actual = compute_deep_hash(&input_struct);

    if actual != expected {
        syn::Error::new_spanned(
            &input_struct.ident,
            format!(
                "⚠️  FROZEN API VIOLATION: Struct '{}' is locked and cannot be modified!\n\
                 Expected: {expected}, Actual: {actual}\n\
                 ⚠️  This API is frozen for compatibility. Changes require explicit approval.\n\
                 Update hash to '{}' ONLY if breaking change is approved.",
                input_struct.ident, actual
            ),
        )
        .to_compile_error()
        .into()
    } else {
        quote! { #input_struct }.into()
    }
}
