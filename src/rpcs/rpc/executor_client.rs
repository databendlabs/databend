// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::datasources::{Partition, Partitions};
use crate::error::FuseQueryResult;
use crate::protobuf::executor_client::ExecutorClient as RpcExecutorClient;
use crate::protobuf::{FetchPartitionRequest, PingRequest};

pub struct ExecutorClient {
    client: RpcExecutorClient<tonic::transport::channel::Channel>,
}

impl ExecutorClient {
    pub async fn try_create(addr: String) -> FuseQueryResult<Self> {
        let client = RpcExecutorClient::connect(format!("http://{}", addr)).await?;
        Ok(Self { client })
    }

    pub async fn ping(&mut self, msg: String) -> FuseQueryResult<String> {
        let request = tonic::Request::new(PingRequest { message: msg });
        Ok(self.client.ping(request).await?.into_inner().message)
    }

    pub async fn fetch_partition(
        &mut self,
        nums: u32,
        uuid: String,
    ) -> FuseQueryResult<Partitions> {
        let request = tonic::Request::new(FetchPartitionRequest { uuid, nums });

        let mut result = vec![];
        let resp = self.client.fetch_partition(request).await?.into_inner();
        for part in resp.partitions {
            result.push(Partition {
                name: part.name,
                version: part.version,
            })
        }

        Ok(result)
    }
}
