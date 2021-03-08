// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use tonic::{Request, Response, Status};

use crate::protobuf::executor_server::Executor;
use crate::protobuf::executor_server::ExecutorServer;
use crate::protobuf::{PingRequest, PingResponse};

#[derive(Default)]
pub struct ExecutorRPCService {}

impl ExecutorRPCService {
    pub fn create() -> Self {
        Self {}
    }

    pub fn make_server(&self) -> ExecutorServer<impl Executor> {
        ExecutorServer::new(ExecutorRPCService::default())
    }
}

#[tonic::async_trait]
impl Executor for ExecutorRPCService {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        ping(request)
    }
}

pub fn ping(request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
    println!("Got a request from {:?}", request.remote_addr());

    let reply = PingResponse {
        message: format!("Hello {}!", request.into_inner().name),
    };
    Ok(Response::new(reply))
}
