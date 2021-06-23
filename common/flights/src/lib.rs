// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub use common::flight_result_to_str;
pub use common::status_err;
pub use common_store_api::ReadPlanResult;
pub use dns_resolver::ConnectionFactory;
pub use dns_resolver::DNSResolver;
pub use flight_token::FlightClaim;
pub use flight_token::FlightToken;
pub use impls::storage_api_impl_utils::get_do_put_meta;
pub use impls::storage_api_impl_utils::set_do_put_meta;
pub use store_client::StoreClient;
pub use store_do_action::AddUserAction;
pub use store_do_action::AddUserActionResult;
pub use store_do_action::CreateDatabaseAction;
pub use store_do_action::CreateTableAction;
pub use store_do_action::DropDatabaseAction;
pub use store_do_action::DropTableAction;
pub use store_do_action::DropUserAction;
pub use store_do_action::DropUserActionResult;
pub use store_do_action::GetAllUsersAction;
pub use store_do_action::GetAllUsersActionResult;
pub use store_do_action::GetDatabaseAction;
pub use store_do_action::GetKVAction;
pub use store_do_action::GetTableAction;
pub use store_do_action::GetUserAction;
pub use store_do_action::GetUserActionResult;
pub use store_do_action::GetUsersAction;
pub use store_do_action::GetUsersActionResult;
pub use store_do_action::ReadPlanAction;
pub use store_do_action::RequestFor;
pub use store_do_action::StoreDoAction;
pub use store_do_action::UpdateUserAction;
pub use store_do_action::UpdateUserActionResult;
pub use store_do_action::UpsertKVAction;
pub use store_do_action::UserInfo;
pub use store_do_get::StoreDoGet;

mod common;
mod dns_resolver;
mod flight_token;
mod impls;
mod store_client;
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
