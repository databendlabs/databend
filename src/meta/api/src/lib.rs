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

#![deny(unused_crate_dependencies)]
extern crate common_meta_types;

mod id;
mod id_generator;
mod kv_api;
mod kv_api_key;
mod kv_api_test_suite;
mod kv_api_utils;
mod schema_api;
mod schema_api_impl;
mod schema_api_keys;
mod schema_api_test_suite;
mod share_api;
mod share_api_impl;
mod share_api_keys;
mod share_api_test_suite;

pub use id::Id;
pub(crate) use id_generator::IdGenerator;
pub use kv_api::get_start_and_end_of_prefix;
pub use kv_api::prefix_of_string;
pub use kv_api::ApiBuilder;
pub use kv_api::AsKVApi;
pub use kv_api::KVApi;
pub use kv_api_key::KVApiKey;
pub use kv_api_key::KVApiKeyError;
pub use kv_api_test_suite::KVApiTestSuite;
pub use kv_api_utils::db_has_to_exist;
pub use kv_api_utils::deserialize_struct;
pub use kv_api_utils::deserialize_u64;
pub use kv_api_utils::fetch_id;
pub use kv_api_utils::get_kv_data;
pub use kv_api_utils::get_object_shared_by_share_ids;
pub use kv_api_utils::get_share_account_meta_or_err;
pub use kv_api_utils::get_share_database_id_and_privilege;
pub use kv_api_utils::get_share_id_to_name_or_err;
pub use kv_api_utils::get_share_meta_by_id_or_err;
pub use kv_api_utils::get_share_or_err;
pub use kv_api_utils::get_struct_value;
pub use kv_api_utils::get_u64_value;
pub use kv_api_utils::is_all_db_data_removed;
pub use kv_api_utils::is_db_need_to_be_remove;
pub use kv_api_utils::list_keys;
pub use kv_api_utils::list_u64_value;
pub use kv_api_utils::meta_encode_err;
pub use kv_api_utils::send_txn;
pub use kv_api_utils::serialize_struct;
pub use kv_api_utils::serialize_u64;
pub use kv_api_utils::table_has_to_exist;
pub use kv_api_utils::txn_cond_seq;
pub use kv_api_utils::txn_op_del;
pub use kv_api_utils::txn_op_put;
pub use kv_api_utils::TXN_MAX_RETRY_TIMES;
pub use schema_api::SchemaApi;
pub(crate) use schema_api_impl::get_db_or_err;
pub use schema_api_test_suite::SchemaApiTestSuite;
pub use share_api::ShareApi;
pub use share_api_test_suite::ShareApiTestSuite;
