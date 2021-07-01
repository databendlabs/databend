// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub use common::flight_result_to_str;
pub use common::status_err;
pub use common_store_api::kv_api::MGetKVActionResult;
pub use common_store_api::kv_api::PrefixListReply;
pub use common_store_api::ReadPlanResult;
pub use dns_resolver::ConnectionFactory;
pub use dns_resolver::DNSResolver;
pub use flight_token::FlightClaim;
pub use flight_token::FlightToken;
pub use impls::kv_api_impl::DeleteKVReply;
pub use impls::kv_api_impl::DeleteKVReq;
pub use impls::kv_api_impl::GetKVAction;
pub use impls::kv_api_impl::MGetKVAction;
pub use impls::kv_api_impl::PrefixListReq;
pub use impls::kv_api_impl::UpdateByKeyReply;
pub use impls::kv_api_impl::UpdateKVReq;
pub use impls::kv_api_impl::UpsertKVAction;
pub use impls::meta_api_impl::CreateDatabaseAction;
pub use impls::meta_api_impl::CreateTableAction;
pub use impls::meta_api_impl::DropDatabaseAction;
pub use impls::meta_api_impl::DropTableAction;
pub use impls::meta_api_impl::GetDatabaseAction;
pub use impls::meta_api_impl::GetTableAction;
pub use impls::storage_api_impl::DataPartInfo;
pub use impls::storage_api_impl::ReadPlanAction;
pub use impls::storage_api_impl_utils::get_do_put_meta;
pub use impls::storage_api_impl_utils::set_do_put_meta;
pub use store_client::StoreClient;
pub use store_do_action::RequestFor;
pub use store_do_action::StoreDoAction;
pub use store_do_get::StoreDoGet;

mod common;
mod dns_resolver;
mod flight_token;
mod impls;
mod store_client;
#[macro_use]
mod store_do_action;
mod store_do_get;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("queryflight");
    tonic::include_proto!("storeflight");
}

#[cfg(test)]
mod dns_resolver_test;
