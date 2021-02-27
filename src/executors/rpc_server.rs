// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use tonic::{Request, Response, Status};

use crate::proto::executor_server::Executor;
use crate::proto::{PingRequest, PingResponse};

#[derive(Default)]
pub struct ExecutorRPCServer {}

#[tonic::async_trait]
impl Executor for ExecutorRPCServer {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        super::rpc::rpc_ping::ping(request)
    }
}
