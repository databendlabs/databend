// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub mod flight_token;
pub mod query_do_action;
pub mod query_do_get;
pub mod store_client;
pub mod store_do_action;
pub mod store_do_get;
pub mod util;

pub use flight_token::FlightClaim;
pub use flight_token::FlightToken;
pub use store_client::StoreClient;
pub use store_do_action::CreateDatabaseAction;
pub use store_do_action::CreateDatabaseActionResult;
pub use store_do_action::CreateTableAction;
pub use store_do_action::CreateTableActionResult;
pub use store_do_action::GetTableAction;
pub use store_do_action::GetTableActionResult;
pub use store_do_action::ReadPlanAction;
pub use store_do_action::ReadPlanActionResult;
pub use store_do_action::StoreDoAction;
pub use store_do_action::StoreDoActionResult;
pub use util::flight_result_to_str;
pub use util::status_err;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("queryflight");
    tonic::include_proto!("storeflight");
}
