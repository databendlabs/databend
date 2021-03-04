// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

// The rpcs module only used for internal communication, such as GRPC between cluster and the managed HTTP REST API.

pub mod http;
pub mod http_service;
pub mod rpc;
pub mod rpc_service;

pub use self::http_service::HttpService;
pub use self::rpc_service::RpcService;
