// Copyright 2020 Datafuse Labs.
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
use proc_macro2::Ident;
use syn::spanned::Spanned;

fn token_stream_with_error(mut tokens: TokenStream, error: syn::Error) -> TokenStream {
    tokens.extend(TokenStream::from(error.into_compile_error()));
    tokens
}

pub fn async_main(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input: syn::ItemFn = match syn::parse(item.clone()) {
        Ok(it) => it,
        Err(e) => return token_stream_with_error(item, e),
    };

    match input.sig.inputs.len() {
        0 => parse_knobs(input, false, false),
        1 => parse_knobs(input, false, true),
        _ => token_stream_with_error(item, syn::Error::new(input.sig.span(), "")),
    }
}

pub fn async_test(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input: syn::ItemFn = match syn::parse(item.clone()) {
        Ok(it) => it,
        Err(e) => return token_stream_with_error(item, e),
    };

    match input.sig.inputs.len() {
        0 => parse_knobs(input, true, false),
        1 => parse_knobs(input, true, true),
        _ => token_stream_with_error(item, syn::Error::new(input.sig.span(), "")),
    }
}

fn parse_knobs(mut input: syn::ItemFn, is_test: bool, has_tracker: bool) -> TokenStream {
    let mut inner_impl = input.clone();
    inner_impl.sig.ident = Ident::new("main_impl", inner_impl.sig.ident.span());

    input.sig.asyncness = None;
    input.sig.inputs.clear();
    let (last_stmt_start_span, last_stmt_end_span) = {
        let mut last_stmt = input
            .block
            .stmts
            .last()
            .map(quote::ToTokens::into_token_stream)
            .unwrap_or_default()
            .into_iter();

        let start = last_stmt
            .next()
            .map_or_else(proc_macro2::Span::call_site, |t| t.span());
        let end = last_stmt.last().map_or(start, |t| t.span());
        (start, end)
    };

    let rt = quote::quote_spanned! {last_stmt_start_span =>
        common_base::Runtime::with_default_worker_threads().unwrap()
    };

    let wait_in_future = match has_tracker {
        true => quote::quote! {
            {
                let tracker = runtime.get_tracker();
                main_impl(tracker.clone())
            }
        },
        false => quote::quote! { main_impl() },
    };

    let header = if is_test {
        quote::quote! {
            use common_base::Runtime;
            #[::core::prelude::v1::test]
        }
    } else {
        quote::quote! {}
    };

    let body = &input.block;
    let brace_token = input.block.brace_token;
    let (tail_return, tail_semicolon) = match body.stmts.last() {
        Some(syn::Stmt::Semi(expr, _)) => (
            match expr {
                syn::Expr::Return(_) => quote::quote! { return },
                _ => quote::quote! {},
            },
            quote::quote! {
                ;
            },
        ),
        _ => (quote::quote! {}, quote::quote! {}),
    };

    input.block = syn::parse2(quote::quote_spanned! {last_stmt_end_span=>
        {
            #inner_impl

            {
                let runtime = #rt;
                let wait_future = #wait_in_future;
                #tail_return runtime.block_on(wait_future)#tail_semicolon
            }
        }
    })
    .expect("Parsing failure");
    input.block.brace_token = brace_token;

    let result = quote::quote! {
        #header
        #input
    };

    result.into()
}
