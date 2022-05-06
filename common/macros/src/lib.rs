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

mod async_entrypoint;
mod malloc_sizeof;

synstructure::decl_derive!([MallocSizeOf, attributes(ignore_malloc_size_of, conditional_malloc_size_of)] => malloc_sizeof::malloc_size_of_derive);

#[allow(unused)]
use proc_macro::TokenStream;

#[proc_macro_attribute]
#[cfg(not(test))] // Work around for rust-lang/rust#62127
pub fn databend_main(args: TokenStream, item: TokenStream) -> TokenStream {
    async_entrypoint::async_main(args, item)
}

#[proc_macro_attribute]
#[cfg(not(test))] // Work around for rust-lang/rust#62127
pub fn databend_test(args: TokenStream, item: TokenStream) -> TokenStream {
    async_entrypoint::async_test(args, item)
}
