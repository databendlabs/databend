// Copyright 2021 Datafuse Labs.
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
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A crate for deriving the MallocSizeOf trait.
//! this is only used for databend

use syn::*;
use synstructure::*;

pub fn malloc_size_of_derive(s: synstructure::Structure) -> proc_macro2::TokenStream {
    let match_body = s.each(|binding| {
        let mut ignore = false;
        let mut conditional = false;
        for attr in binding.ast().attrs.iter() {
            match attr.parse_meta() {
                Err(_) => {}
                Ok(meta) => match meta {
                    syn::Meta::Path(ref path) | syn::Meta::List(syn::MetaList { ref path, .. }) => {
                        assert!(
                            !path.is_ident("ignore_malloc_size_of"),
                            "#[ignore_malloc_size_of] should have an explanation, \
                             e.g. #[ignore_malloc_size_of = \"because reasons\"]"
                        );
                        if path.is_ident("conditional_malloc_size_of") {
                            conditional = true;
                        }
                    }
                    syn::Meta::NameValue(syn::MetaNameValue { ref path, .. }) => {
                        if path.is_ident("ignore_malloc_size_of") {
                            ignore = true;
                        }
                        if path.is_ident("conditional_malloc_size_of") {
                            conditional = true;
                        }
                    }
                },
            }
        }

        assert!(
            !ignore || !conditional,
            "ignore_malloc_size_of and conditional_malloc_size_of are incompatible"
        );

        if ignore {
            return None;
        }

        let path = if conditional {
            quote! { common_base::mem_allocator::MallocConditionalSizeOf::conditional_size_of }
        } else {
            quote! { common_base::mem_allocator::MallocSizeOf::size_of }
        };

        if let syn::Type::Array(..) = binding.ast().ty {
            Some(quote! {
                for item in #binding.iter() {
                    sum += #path(item, ops);
                }
            })
        } else {
            Some(quote! {
                sum += #path(#binding, ops);
            })
        }
    });

    let ast = s.ast();
    let name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    let mut where_clause = where_clause.unwrap_or(&parse_quote!(where)).clone();
    for param in ast.generics.type_params() {
        let ident = &param.ident;
        where_clause
            .predicates
            .push(parse_quote!(#ident: common_base::mem_allocator::MallocSizeOf));
    }

    let tokens = quote! {
        impl #impl_generics common_base::mem_allocator::MallocSizeOf for #name #ty_generics #where_clause {
            #[inline]
            #[allow(unused_variables, unused_mut, unreachable_code)]
            fn size_of(&self, ops: &mut common_base::mem_allocator::MallocSizeOfOps) -> usize {
                let mut sum = 0;
                match *self {
                    #match_body
                }
                sum
            }
        }
    };

    tokens
}
