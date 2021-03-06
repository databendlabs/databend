// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

// The rpcs module only used for internal communication, such as GRPC between cluster and the managed HTTP REST API.

mod http;
mod http_service;
pub mod rpc;
mod rpc_service;

pub use http_service::HttpService;
pub use rpc_service::RpcService;
