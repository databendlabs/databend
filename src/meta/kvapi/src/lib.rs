//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#![allow(clippy::uninlined_format_args)]
#![deny(unused_crate_dependencies)]

mod kv_api;
pub mod kv_api_key;
mod kv_api_test_suite;

pub use kv_api::get_start_and_end_of_prefix;
pub use kv_api::prefix_of_string;
pub use kv_api::ApiBuilder;
pub use kv_api::AsKVApi;
pub use kv_api::KVApi;
pub use kv_api_key::KVApiKey;
pub use kv_api_key::KVApiKeyError;
pub use kv_api_test_suite::KVApiTestSuite;
