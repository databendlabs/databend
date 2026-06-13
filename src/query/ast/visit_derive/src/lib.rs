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
use proc_macro2::TokenStream as TokenStream2;
use quote::ToTokens;
use quote::format_ident;
use quote::quote;
use syn::Data;
use syn::DataEnum;
use syn::DataStruct;
use syn::DeriveInput;
use syn::Error;
use syn::Field;
use syn::Fields;
use syn::Type;
use syn::parse_macro_input;
use syn::spanned::Spanned;

#[proc_macro_derive(Walk)]
pub fn derive_walk(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    impl_walk(input, false)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

#[proc_macro_derive(WalkMut)]
pub fn derive_walk_mut(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    impl_walk(input, true)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

fn impl_walk(input: DeriveInput, mutable: bool) -> syn::Result<TokenStream2> {
    let ident = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let walk_body = emit_walk_body(&input.data, mutable)?;

    if mutable {
        Ok(quote! {
            #[automatically_derived]
            impl #impl_generics crate::visit::WalkMut for #ident #ty_generics #where_clause {
                fn walk_mut<V: crate::visit::VisitorMut + ?Sized>(
                    &mut self,
                    visitor: &mut V,
                ) -> std::result::Result<crate::visit::VisitControl<V::Break>, V::Error> {
                    #walk_body
                    Ok(crate::visit::VisitControl::Continue)
                }
            }
        })
    } else {
        Ok(quote! {
            #[automatically_derived]
            impl #impl_generics crate::visit::Walk for #ident #ty_generics #where_clause {
                fn walk<V: crate::visit::Visitor + ?Sized>(
                    &self,
                    visitor: &mut V,
                ) -> std::result::Result<crate::visit::VisitControl<V::Break>, V::Error> {
                    #walk_body
                    Ok(crate::visit::VisitControl::Continue)
                }
            }
        })
    }
}

fn emit_walk_body(data: &Data, mutable: bool) -> syn::Result<TokenStream2> {
    match data {
        Data::Struct(data) => emit_struct_body(data, mutable),
        Data::Enum(data) => emit_enum_body(data, mutable),
        Data::Union(data) => Err(Error::new(
            data.union_token.span(),
            "unions are not supported",
        )),
    }
}

fn emit_struct_body(data: &DataStruct, mutable: bool) -> syn::Result<TokenStream2> {
    let access = |member: TokenStream2| {
        if mutable {
            quote!(&mut self.#member)
        } else {
            quote!(&self.#member)
        }
    };

    let stmts = match &data.fields {
        Fields::Named(fields) => fields
            .named
            .iter()
            .map(|field| {
                let ident = field.ident.as_ref().unwrap();
                emit_field_stmt(field, access(quote!(#ident)), mutable)
            })
            .collect::<syn::Result<Vec<_>>>()?,
        Fields::Unnamed(fields) => fields
            .unnamed
            .iter()
            .enumerate()
            .map(|(idx, field)| emit_field_stmt(field, access(quote!(#idx)), mutable))
            .collect::<syn::Result<Vec<_>>>()?,
        Fields::Unit => vec![],
    };

    Ok(quote! { #(#stmts)* })
}

fn emit_enum_body(data: &DataEnum, mutable: bool) -> syn::Result<TokenStream2> {
    let mut arms = Vec::new();

    for variant in &data.variants {
        let ident = &variant.ident;
        let arm = match &variant.fields {
            Fields::Named(fields) => {
                let mut bindings = Vec::new();
                let mut stmts = Vec::new();

                for field in &fields.named {
                    let field_ident = field.ident.as_ref().unwrap();
                    let stmt = emit_walk_for_type(&field.ty, quote!(#field_ident), mutable)?;
                    if stmt.is_empty() {
                        bindings.push(quote!(#field_ident: _));
                    } else {
                        bindings.push(quote!(#field_ident));
                        stmts.push(stmt);
                    }
                }

                quote! {
                    Self::#ident { #(#bindings),* } => {
                        #(#stmts)*
                    }
                }
            }
            Fields::Unnamed(fields) => {
                let mut bindings = Vec::new();
                let mut stmts = Vec::new();

                for (idx, field) in fields.unnamed.iter().enumerate() {
                    let binding = format_ident!("field_{idx}");
                    let stmt = emit_walk_for_type(&field.ty, quote!(#binding), mutable)?;
                    if stmt.is_empty() {
                        bindings.push(quote!(_));
                    } else {
                        bindings.push(binding.to_token_stream());
                        stmts.push(stmt);
                    }
                }

                quote! {
                    Self::#ident(#(#bindings),*) => {
                        #(#stmts)*
                    }
                }
            }
            Fields::Unit => quote!(Self::#ident => {}),
        };
        arms.push(arm);
    }

    Ok(quote! {
        match self {
            #(#arms),*
        }
    })
}

fn emit_field_stmt(
    field: &Field,
    access: TokenStream2,
    mutable: bool,
) -> syn::Result<TokenStream2> {
    emit_walk_for_type(&field.ty, access, mutable)
}

fn emit_walk_for_type(ty: &Type, access: TokenStream2, mutable: bool) -> syn::Result<TokenStream2> {
    match ty {
        Type::Path(type_path) => {
            let segment = type_path
                .path
                .segments
                .last()
                .ok_or_else(|| Error::new(type_path.span(), "unsupported type"))?;
            let name = segment.ident.to_string();

            if is_primitive_noop_leaf(&name) {
                return Ok(TokenStream2::new());
            }

            let trait_name = if mutable {
                quote!(crate::visit::WalkMut)
            } else {
                quote!(crate::visit::Walk)
            };
            let method = if mutable {
                quote!(walk_mut)
            } else {
                quote!(walk)
            };
            Ok(quote! {
                match #trait_name::#method(#access, visitor)? {
                    crate::visit::VisitControl::Continue => {}
                    crate::visit::VisitControl::SkipChildren => {}
                    crate::visit::VisitControl::Break(value) => {
                        return Ok(crate::visit::VisitControl::Break(value));
                    }
                }
            })
        }
        _ => Err(Error::new(
            ty.span(),
            "unsupported field type for derive(Walk)/derive(WalkMut)",
        )),
    }
}

fn is_primitive_noop_leaf(name: &str) -> bool {
    // Keep this list intentionally small and in sync with the explicit
    // `impl_noop_walk!` block in `src/query/ast/src/visit.rs`.
    matches!(
        name,
        "bool"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "usize"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "isize"
            | "f32"
            | "f64"
    )
}
