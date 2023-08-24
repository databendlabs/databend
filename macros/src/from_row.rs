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

use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{spanned::Spanned, DeriveInput};

/// #[derive(TryFromRow)] derives TryFromRow for struct
pub fn from_row_derive(tokens_input: TokenStream) -> TokenStream {
    let item = syn::parse::<DeriveInput>(tokens_input).expect("No DeriveInput");
    let struct_fields = crate::parser::parse_named_fields(&item, "TryFromRow");

    let struct_name = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    let path = quote!(databend_driver::_macro_internal);

    let set_fields_code = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;
        let field_type = &field.ty;

        quote_spanned! {field.span() =>
            #field_name: {
                let (col_ix, col_value) = vals_iter
                    .next()
                    .unwrap(); // vals_iter size is checked before this code is reached, so
                               // it is safe to unwrap
                let t = col_value.get_type();
                <#field_type>::try_from(col_value)
                    .map_err(|_| format!("failed converting column {} from type({:?}) to type({})", col_ix, t, std::any::type_name::<#field_type>()))?
            },
        }
    });

    let fields_count = struct_fields.named.len();
    let generated = quote! {
        impl #impl_generics TryFrom<#path::Row> for #struct_name #ty_generics #where_clause {
            type Error = String;
            fn try_from(row: #path::Row) -> #path::Result<Self, String> {
                if #fields_count != row.len() {
                    return Err(format!("row size mismatch: expected {} columns, got {}", #fields_count, row.len()));
                }
                let mut vals_iter = row.into_iter().enumerate();
                Ok(#struct_name {
                    #(#set_fields_code)*
                })
            }
        }
    };

    TokenStream::from(generated)
}
