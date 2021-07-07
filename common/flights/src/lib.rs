// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod address_test;
#[cfg(test)]
mod dns_resolver_test;

mod common;
mod dns_resolver;
mod flight_token;
mod impls;
mod store_client;
#[macro_use]
mod store_do_action;
mod address;
mod store_do_get;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("queryflight");
    tonic::include_proto!("storeflight");
}

pub use address::Address;
pub use common::flight_result_to_str;
pub use common::status_err;
pub use common_store_api::KVApi;
pub use common_store_api::MetaApi;
pub use common_store_api::StorageApi;
pub use dns_resolver::ConnectionFactory;
pub use dns_resolver::DNSResolver;
pub use flight_token::FlightClaim;
pub use flight_token::FlightToken;
pub use impls::kv_api_impl;
pub use impls::meta_api_impl;
pub use impls::storage_api_impl;
pub use store_client::StoreClient;
pub use store_do_action::RequestFor;
pub use store_do_action::StoreDoAction;
pub use store_do_get::StoreDoGet;
