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
use proc_macro2::Span;
use quote::quote;
use syn::Attribute;
use syn::DeriveInput;
use syn::Ident;
use syn::Item;
use syn::ItemEnum;
use syn::ItemImpl;
use syn::ItemStruct;
use syn::ItemType;
use syn::Type;
use syn::parse_macro_input;
use syn::parse_quote;

/// Derives `::databend_common_base::base::Service` for a struct or enum.
///
/// Supported forms:
/// - `#[derive(Service)]` on a struct or enum uses the default symbol
///   `<TypeName>Symbol`.
/// - `#[derive(Service)]` with `#[service_symbol(FooSymbol)]` uses an explicit
///   service symbol.
///
/// # Examples
///
/// Default symbol:
///
/// ```ignore
/// use databend_common_base::base::Service;
///
/// #[derive(Service)]
/// pub struct MyService;
/// ```
///
/// Explicit symbol:
///
/// ```ignore
/// use databend_common_base::base::Service;
///
/// pub struct FooSymbol;
///
/// #[derive(Service)]
/// #[service_symbol(FooSymbol)]
/// pub struct MyService;
/// ```
#[proc_macro_derive(Service, attributes(service_symbol))]
pub fn derive_service(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    match &input.data {
        syn::Data::Struct(_) | syn::Data::Enum(_) => expand_derive(input),
        syn::Data::Union(_) => syn::Error::new_spanned(
            input,
            "`#[derive(Service)]` only supports structs and enums",
        )
        .to_compile_error()
        .into(),
    }
}

/// Assigns a `::databend_common_base::base::Service::Symbol` to a service type
/// or `impl Service for ...` block.
///
/// Supported forms:
/// - `#[service_symbol]` on a struct uses the default symbol
///   `<TypeName>Symbol`.
/// - `#[service_symbol(FooSymbol)]` on a struct uses an explicit symbol.
/// - `#[service_symbol]` on an enum uses the default symbol
///   `<TypeName>Symbol`.
/// - `#[service_symbol(FooSymbol)]` on an enum uses an explicit symbol.
/// - `#[service_symbol]` on a type alias uses the default symbol
///   `<AliasName>Symbol`.
/// - `#[service_symbol(FooSymbol)]` on a type alias uses an explicit symbol.
/// - `#[service_symbol]` on `impl Service for T` uses the default symbol
///   `<TypeName>Symbol`.
/// - `#[service_symbol(FooSymbol)]` on `impl Service for T` uses an explicit
///   symbol.
///
/// # Examples
///
/// Struct:
///
/// ```ignore
/// use databend_common_base::base::service_symbol;
///
/// #[service_symbol]
/// pub struct MyService;
/// ```
///
/// Enum with an explicit symbol:
///
/// ```ignore
/// use databend_common_base::base::service_symbol;
///
/// pub struct FooSymbol;
///
/// #[service_symbol(FooSymbol)]
/// pub enum MyService {
///     A,
///     B,
/// }
/// ```
///
/// Type alias:
///
/// ```ignore
/// use databend_common_base::base::service_symbol;
///
/// #[service_symbol]
/// pub type BuildInfoRef = &'static BuildInfo;
///
/// pub struct BuildInfo;
/// ```
///
/// Existing `impl Service for ...` block:
///
/// ```ignore
/// use databend_common_base::base::Service;
/// use databend_common_base::base::service_symbol;
///
/// pub struct FooSymbol;
/// pub struct MyService;
///
/// #[service_symbol(FooSymbol)]
/// impl Service for MyService {}
/// ```
#[proc_macro_attribute]
pub fn service_symbol(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item = parse_macro_input!(item as Item);

    match item {
        Item::Impl(item_impl) => expand_impl(attr, item_impl),
        Item::Struct(item_struct) => expand_struct(attr, item_struct),
        Item::Enum(item_enum) => expand_enum(attr, item_enum),
        Item::Type(item_type) => expand_type_alias(attr, item_type),
        item => syn::Error::new_spanned(
            item,
            "`#[service_symbol]` can only be used on service types, enums, type aliases, or `impl Service for ...` blocks",
        )
        .to_compile_error()
        .into(),
    }
}

fn default_symbol_type(self_ty: &Type) -> syn::Result<Type> {
    let Type::Path(type_path) = self_ty else {
        return Err(syn::Error::new_spanned(
            self_ty,
            "`#[service_symbol]` without arguments requires a concrete type path",
        ));
    };

    let Some(last_segment) = type_path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            &type_path.path,
            "`#[service_symbol]` without arguments requires a concrete type path",
        ));
    };

    let symbol_ident = Ident::new(&format!("{}Symbol", last_segment.ident), Span::call_site());
    Ok(parse_quote!(::databend_common_base::base::#symbol_ident))
}

fn parse_symbol_attr(attrs: &[Attribute], default_ty: &Type) -> syn::Result<Type> {
    let mut symbol_attr = None;

    for attr in attrs {
        if attr.path().is_ident("service_symbol") {
            symbol_attr = Some(attr);
        }
    }

    match symbol_attr {
        Some(attr) => attr.parse_args(),
        None => default_symbol_type(default_ty),
    }
}

fn parse_symbol(attr: TokenStream, default_ty: &Type) -> syn::Result<Type> {
    if attr.is_empty() {
        default_symbol_type(default_ty)
    } else {
        syn::parse(attr)
    }
}

fn expand_impl(attr: TokenStream, mut item_impl: ItemImpl) -> TokenStream {
    let symbol = match parse_symbol(attr, &item_impl.self_ty) {
        Ok(symbol) => symbol,
        Err(err) => return err.to_compile_error().into(),
    };

    let Some((_, trait_path, _)) = &item_impl.trait_ else {
        return syn::Error::new_spanned(
            &item_impl.self_ty,
            "`#[service_symbol]` on an impl block requires `impl Service for ...`",
        )
        .to_compile_error()
        .into();
    };

    let Some(last_segment) = trait_path.segments.last() else {
        return syn::Error::new_spanned(
            trait_path,
            "`#[service_symbol]` on an impl block requires `impl Service for ...`",
        )
        .to_compile_error()
        .into();
    };

    if last_segment.ident != "Service" {
        return syn::Error::new_spanned(
            trait_path,
            "`#[service_symbol]` on an impl block requires `impl Service for ...`",
        )
        .to_compile_error()
        .into();
    }

    item_impl.items.push(parse_quote!(type Symbol = #symbol;));
    quote!(#item_impl).into()
}

fn expand_struct(attr: TokenStream, item_struct: ItemStruct) -> TokenStream {
    let ident = item_struct.ident.clone();
    let generics = item_struct.generics.clone();
    let (_, ty_generics, where_clause) = generics.split_for_impl();
    let default_ty: Type = parse_quote!(#ident #ty_generics);
    let symbol = match parse_symbol(attr, &default_ty) {
        Ok(symbol) => symbol,
        Err(err) => return err.to_compile_error().into(),
    };

    quote!(
        #item_struct

        impl #generics ::databend_common_base::base::Service for #ident #ty_generics #where_clause {
            type Symbol = #symbol;
        }
    )
    .into()
}

fn expand_enum(attr: TokenStream, item_enum: ItemEnum) -> TokenStream {
    let ident = item_enum.ident.clone();
    let generics = item_enum.generics.clone();
    let (_, ty_generics, where_clause) = generics.split_for_impl();
    let default_ty: Type = parse_quote!(#ident #ty_generics);
    let symbol = match parse_symbol(attr, &default_ty) {
        Ok(symbol) => symbol,
        Err(err) => return err.to_compile_error().into(),
    };

    quote!(
        #item_enum

        impl #generics ::databend_common_base::base::Service for #ident #ty_generics #where_clause {
            type Symbol = #symbol;
        }
    )
    .into()
}

fn expand_type_alias(attr: TokenStream, item_type: ItemType) -> TokenStream {
    let ident = item_type.ident.clone();
    let generics = item_type.generics.clone();
    let (_, ty_generics, where_clause) = generics.split_for_impl();
    let default_ty: Type = parse_quote!(#ident #ty_generics);
    let symbol = match parse_symbol(attr, &default_ty) {
        Ok(symbol) => symbol,
        Err(err) => return err.to_compile_error().into(),
    };

    quote!(
        #item_type

        impl #generics ::databend_common_base::base::Service for #ident #ty_generics #where_clause {
            type Symbol = #symbol;
        }
    )
    .into()
}

fn expand_derive(input: DeriveInput) -> TokenStream {
    let ident = input.ident.clone();
    let generics = input.generics.clone();
    let (_, ty_generics, where_clause) = generics.split_for_impl();
    let default_ty: Type = parse_quote!(#ident #ty_generics);
    let symbol = match parse_symbol_attr(&input.attrs, &default_ty) {
        Ok(symbol) => symbol,
        Err(err) => return err.to_compile_error().into(),
    };

    quote!(
        impl #generics ::databend_common_base::base::Service for #ident #ty_generics #where_clause {
            type Symbol = #symbol;
        }
    )
    .into()
}
