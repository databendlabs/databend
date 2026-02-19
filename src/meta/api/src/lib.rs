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

pub mod api_impl;
pub mod error;
pub mod kv;
pub mod txn;
pub mod util;

pub(crate) use api_impl::auto_increment_api;
pub(crate) use api_impl::auto_increment_api_test_suite;
pub use api_impl::catalog_api;
pub use api_impl::data_mask_api;
pub use api_impl::data_retention_util;
pub(crate) use api_impl::database_api;
pub use api_impl::database_util;
pub use api_impl::dictionary_api;
pub(crate) use api_impl::errors;
pub use api_impl::garbage_collection_api;
pub use api_impl::index_api;
pub use api_impl::lock_api;
pub use api_impl::name_id_value_api;
pub use api_impl::name_value_api;
pub(crate) use api_impl::row_access_policy_api;
pub(crate) use api_impl::schema_api;
pub use api_impl::security_api;
pub(crate) use api_impl::sequence_api;
pub use api_impl::table_api;
pub use api_impl::tag_api;
pub use auto_increment_api::AutoIncrementApi;
pub use auto_increment_api_test_suite::AutoIncrementApiTestSuite;
pub use catalog_api::CatalogApi;
pub use data_mask_api::DatamaskApi;
pub use data_retention_util::get_retention_boundary;
pub use data_retention_util::is_drop_time_retainable;
pub use database_api::DatabaseApi;
pub use databend_common_meta_app::schema::TagError;
pub use dictionary_api::DictionaryApi;
pub use error::app_error as kv_app_error;
pub use error::constructors as error_util;
pub use error::txn_error as meta_txn_error;
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
pub use kv::crud;
pub use kv::fetch_util as kv_fetch_util;
pub use kv::pb_api as kv_pb_api;
pub use kv::pb_crud_api as kv_pb_crud_api;
pub use kv_app_error::KVAppResultExt;
pub use kv_app_error::from_nested;
pub use kv_fetch_util::deserialize_id_get_response;
pub use kv_fetch_util::deserialize_struct_get_response;
pub use kv_fetch_util::fetch_id;
pub use kv_fetch_util::get_u64_value;
pub use kv_fetch_util::list_u64_value;
pub use kv_fetch_util::mget_pb_values;
pub use lock_api::LockApi;
pub use row_access_policy_api::RowAccessPolicyApi;
pub use schema_api::SchemaApi;
pub use security_api::SecurityApi;
pub use sequence_api::SequenceApi;
pub use serialization_util::deserialize_struct;
pub use serialization_util::deserialize_u64;
pub use serialization_util::serialize_struct;
pub use serialization_util::serialize_u64;
pub use table_api::TableApi;
pub use tag_api::TagApi;
pub use txn::backoff as txn_backoff;
pub use txn::backoff::txn_backoff;
pub use txn::condition as txn_condition_util;
pub use txn::core as txn_core_util;
pub use txn::op_builder as txn_op_builder_util;
pub use txn::reply;
pub use txn_condition_util::txn_cond_eq_seq;
pub use txn_condition_util::txn_cond_seq;
pub use txn_core_util::send_txn;
pub use txn_core_util::txn_delete_exact;
pub use txn_core_util::txn_replace_exact;
pub use txn_op_builder_util::txn_del;
pub use txn_op_builder_util::txn_get;
pub use txn_op_builder_util::txn_put_pb;
pub use txn_op_builder_util::txn_put_pb_with_ttl;
pub use txn_op_builder_util::txn_put_u64;
pub use util::DEFAULT_MGET_SIZE;
pub use util::serialization as serialization_util;
