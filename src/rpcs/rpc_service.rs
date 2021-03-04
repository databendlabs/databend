// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;

use tonic::transport::{Error, Server};

use crate::rpcs::rpc::ExecutorRPCService;

pub struct RpcService {}

impl RpcService {
    pub async fn make_server(addr: SocketAddr) -> Result<(), Error> {
        Server::builder()
            .add_service(ExecutorRPCService::make_server())
            .serve(addr)
            .await
    }
}
