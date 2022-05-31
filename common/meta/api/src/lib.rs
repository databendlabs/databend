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
//

#![allow(unused_imports)]
extern crate common_meta_types;

mod kv_api;
mod kv_api_key;
mod kv_api_test_suite;
mod schema_api;
mod schema_api_impl;
mod schema_api_keys;
mod schema_api_test_suite;

pub use kv_api::KVApi;
pub use kv_api::KVApiBuilder;
pub use kv_api_key::KVApiKey;
pub use kv_api_key::KVApiKeyError;
pub use kv_api_test_suite::KVApiTestSuite;
pub use schema_api::SchemaApi;
pub use schema_api_keys::DatabaseIdGen;
pub use schema_api_keys::TableIdGen;
pub use schema_api_test_suite::SchemaApiTestSuite;
