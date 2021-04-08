// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub mod flight_token;
pub mod query_do_action;
pub mod query_do_get;
pub mod store_client;
pub mod store_do_action;
pub mod store_do_get;

pub use flight_token::FlightClaim;
pub use flight_token::FlightToken;
pub use store_client::StoreClient;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("queryflight");
    tonic::include_proto!("storeflight");
}
