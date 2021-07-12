// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

// The api module only used for internal communication, such as GRPC between cluster and the managed HTTP REST API.

pub use http_service::HttpService;
pub use rpc::FlightClient;
pub use rpc::FlightTicket;
pub use rpc::ShuffleAction;
pub use rpc::FlightAction;
pub use rpc_service::RpcService;

mod http;
mod http_service;
mod rpc;
mod rpc_service;
