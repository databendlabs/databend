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

#![allow(clippy::uninlined_format_args)]
#![allow(clippy::diverging_sub_expression)]
#![allow(clippy::type_complexity)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::unnecessary_unwrap)]
#![feature(try_blocks)]

extern crate databend_common_meta_types;
pub mod catalog_api;
pub mod data_mask_api;
mod data_mask_api_impl;
pub mod data_retention_util;
mod database_api;
pub mod database_util;
pub mod dictionary_api;
pub mod error_util;
pub mod garbage_collection_api;
pub mod index_api;
pub mod kv_app_error;
pub mod kv_fetch_util;
pub mod kv_pb_api;
pub mod kv_pb_crud_api;
pub mod lock_api;
pub mod meta_txn_error;
pub mod name_id_value_api;
pub mod name_value_api;
pub mod reply;
mod schema_api;
mod schema_api_test_suite;
pub mod security_api;
mod sequence_api;
pub mod serialization_util;
pub mod table_api;
pub mod tag_api;
pub(crate) mod testing;
pub mod txn_backoff;
pub mod txn_condition_util;
pub mod txn_core_util;
pub mod txn_op_builder_util;
pub mod txn_retry_util;
pub mod util;

mod auto_increment_api;
mod auto_increment_api_test_suite;
mod auto_increment_impl;
pub(crate) mod auto_increment_nextval_impl;
pub mod crud;
mod errors;
mod row_access_policy_api;
mod row_access_policy_api_impl;
mod sequence_api_impl;
pub(crate) mod sequence_nextval_impl;

pub use auto_increment_api::AutoIncrementApi;
pub use auto_increment_api_test_suite::AutoIncrementApiTestSuite;
pub use catalog_api::CatalogApi;
pub use data_mask_api::DatamaskApi;
// Re-export from new data_retention_util module for backward compatibility
pub use data_retention_util::get_retention_boundary;
pub use data_retention_util::is_drop_time_retainable;
pub use database_api::DatabaseApi;
pub use databend_common_meta_app::schema::TagError;
pub use dictionary_api::DictionaryApi;
// Re-export from new error_util module for backward compatibility
pub use error_util::assert_table_exist;
pub use error_util::db_has_to_exist;
pub use error_util::db_has_to_not_exist;
pub use error_util::db_id_has_to_exist;
pub use error_util::table_has_to_not_exist;
pub use error_util::unknown_database_error;
pub use errors::MaskingPolicyError;
pub use errors::RowAccessPolicyError;
pub use garbage_collection_api::GarbageCollectionApi;
pub use index_api::IndexApi;
// Re-export from new kv_fetch_util module for backward compatibility
pub use kv_fetch_util::deserialize_id_get_response;
pub use kv_fetch_util::deserialize_struct_get_response;
pub use kv_fetch_util::fetch_id;
pub use kv_fetch_util::get_u64_value;
pub use kv_fetch_util::list_u64_value;
pub use kv_fetch_util::mget_pb_values;
pub use lock_api::LockApi;
pub use row_access_policy_api::RowAccessPolicyApi;
pub use schema_api::SchemaApi;
pub use schema_api_test_suite::SchemaApiTestSuite;
pub use security_api::SecurityApi;
pub use sequence_api::SequenceApi;
// Re-export from new serialization_util module for backward compatibility
pub use serialization_util::{
    deserialize_struct, deserialize_u64, serialize_struct, serialize_u64,
};
pub use table_api::TableApi;
pub use tag_api::TagApi;
// Re-export from new txn_condition_util module for backward compatibility
pub use txn_condition_util::txn_cond_eq_seq;
pub use txn_condition_util::txn_cond_seq;
// Re-export from new txn_core_util module for backward compatibility
pub use txn_core_util::send_txn;
pub use txn_core_util::txn_delete_exact;
pub use txn_core_util::txn_replace_exact;
// Re-export from new txn_op_builder_util module for backward compatibility
pub use txn_op_builder_util::txn_op_del;
pub use txn_op_builder_util::txn_op_get;
pub use txn_op_builder_util::txn_op_put;
pub use txn_op_builder_util::txn_op_put_pb;
pub use txn_op_builder_util::txn_put_pb;
// Re-export from new txn_retry_util module for backward compatibility
pub use txn_retry_util::txn_backoff;
pub use util::DEFAULT_MGET_SIZE;
