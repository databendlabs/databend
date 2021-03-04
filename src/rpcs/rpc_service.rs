// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::SocketAddr;

use tonic::transport::{Error, Server};

use crate::proto::executor_server::ExecutorServer;
use crate::rpcs::rpc::ExecutorRPCService;

pub async fn make_service(addr: SocketAddr) -> Result<(), Error> {
    let rpc_executor = ExecutorRPCService::default();
    Server::builder()
        .add_service(ExecutorServer::new(rpc_executor))
        .serve(addr)
        .await
}
