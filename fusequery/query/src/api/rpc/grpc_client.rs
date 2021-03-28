// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_planners::{Partition, Partitions};

use crate::error::FuseQueryResult;
use crate::protobuf::query_rpc_client::QueryRpcClient;
use crate::protobuf::{FetchPartitionRequest, PingRequest};

pub struct GrpcClient {
    addr: String,
}

impl GrpcClient {
    pub fn create(addr: String) -> Self {
        Self {
            addr: format!("http://{}", addr),
        }
    }

    pub async fn ping(&self, msg: String) -> FuseQueryResult<String> {
        let request = tonic::Request::new(PingRequest { message: msg });
        let mut client = QueryRpcClient::connect(self.addr.clone()).await?;
        Ok(client.ping(request).await?.into_inner().message)
    }

    pub async fn fetch_partition(&self, nums: u32, uuid: String) -> FuseQueryResult<Partitions> {
        let request = tonic::Request::new(FetchPartitionRequest { uuid, nums });
        let mut client = QueryRpcClient::connect(self.addr.clone()).await?;

        let mut result = vec![];
        let resp = client.fetch_partition(request).await?.into_inner();
        for part in resp.partitions {
            result.push(Partition {
                name: part.name,
                version: part.version,
            })
        }

        Ok(result)
    }
}
