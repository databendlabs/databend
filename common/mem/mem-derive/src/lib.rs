// Copyright 2016-2017 The Servo Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A crate for deriving the MallocSizeOf trait.
//! this is only used for databend

extern crate proc_macro2;
#[macro_use]
extern crate syn;
#[macro_use]
extern crate synstructure;

decl_derive!([MallocSizeOf, attributes(ignore_malloc_size_of, conditional_malloc_size_of)] => malloc_size_of_derive);

fn malloc_size_of_derive(s: synstructure::Structure) -> proc_macro2::TokenStream {
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
            quote! { common_mem_allocator::MallocConditionalSizeOf::conditional_size_of }
        } else {
            quote! { common_mem_allocator::MallocSizeOf::size_of }
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
            .push(parse_quote!(#ident: common_mem_allocator::MallocSizeOf));
    }

    let tokens = quote! {
        impl #impl_generics common_mem_allocator::MallocSizeOf for #name #ty_generics #where_clause {
            #[inline]
            #[allow(unused_variables, unused_mut, unreachable_code)]
            fn size_of(&self, ops: &mut common_mem_allocator::MallocSizeOfOps) -> usize {
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
