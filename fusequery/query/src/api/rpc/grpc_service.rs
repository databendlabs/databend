// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use tonic::{Request, Response, Status};

use crate::protobuf::query_rpc_server::QueryRpc;
use crate::protobuf::query_rpc_server::QueryRpcServer;
use crate::protobuf::{
    FetchPartitionRequest, FetchPartitionResponse, PartitionProto, PingRequest, PingResponse,
};
use crate::sessions::SessionRef;

pub struct GrpcService {
    session_manager: SessionRef,
}

impl GrpcService {
    pub fn create(session_manager: SessionRef) -> Self {
        Self { session_manager }
    }

    pub fn make_server(self) -> QueryRpcServer<impl QueryRpc> {
        QueryRpcServer::new(self)
    }
}

#[tonic::async_trait]
impl QueryRpc for GrpcService {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        ping(request)
    }

    async fn fetch_partition(
        &self,
        request: Request<FetchPartitionRequest>,
    ) -> Result<Response<FetchPartitionResponse>, Status> {
        fetch_partition(self.session_manager.clone(), request)
    }
}

pub fn ping(request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
    println!("Got a request from {:?}", request.remote_addr());

    let reply = PingResponse {
        message: format!("Hello {}!", request.into_inner().message),
    };
    Ok(Response::new(reply))
}

pub fn fetch_partition(
    session_manager: SessionRef,
    request: Request<FetchPartitionRequest>,
) -> Result<Response<FetchPartitionResponse>, Status> {
    println!("Got a request from {:?}", request.remote_addr());

    let req = request.into_inner();
    let uuid = req.uuid;
    let nums = req.nums;
    let partitions = session_manager
        .try_fetch_partitions(uuid, nums as usize)
        .map_err(|e| Status::internal(e.to_string()))?;

    let mut protos = vec![];
    for partition in partitions {
        protos.push(PartitionProto {
            name: partition.name,
            version: partition.version,
        })
    }

    let reply = FetchPartitionResponse { partitions: protos };
    Ok(Response::new(reply))
}
