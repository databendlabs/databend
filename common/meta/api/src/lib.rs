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

extern crate common_meta_types;

mod kv_api;
mod kv_api_test_suite;
mod meta_api;
mod meta_api_test_suite;

pub use kv_api::KVApi;
pub use kv_api::KVApiBuilder;
pub use kv_api_test_suite::KVApiTestSuite;
pub use meta_api::MetaApi;
pub use meta_api_test_suite::MetaApiTestSuite;
