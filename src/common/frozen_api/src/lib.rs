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
//! - Nested structure changes
//! - Generic parameters
//!
//! Use on structs that are serialized, part of network protocols, or must maintain compatibility.

use proc_macro::TokenStream;
use quote::quote;
use quote::ToTokens;
use sha2::Digest;
use sha2::Sha256;
use syn::parse_macro_input;
use syn::DeriveInput;
use syn::LitStr;

/// Compile-time API freezing macro that prevents accidental breaking changes.
#[proc_macro_attribute]
pub fn frozen_api(args: TokenStream, input: TokenStream) -> TokenStream {
    let expected = parse_macro_input!(args as LitStr).value();
    let input_struct = parse_macro_input!(input as DeriveInput);

    // Compute actual hash directly from TokenStream
    let actual = hex::encode(&Sha256::digest(input_struct.to_token_stream().to_string())[..4]);

    if actual != expected {
        syn::Error::new_spanned(
            &input_struct.ident,
            format!(
                "API hash mismatch for '{}'! Expected: {expected}, Actual: {actual}\n\
                 Update hash to '{actual}' if change is intentional.",
                input_struct.ident
            ),
        )
        .to_compile_error()
        .into()
    } else {
        quote! { #input_struct }.into()
    }
}
