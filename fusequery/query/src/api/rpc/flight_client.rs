// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use anyhow::{bail, Result};
use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::{Action, Ticket};
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_planners::Partitions;
use common_streams::SendableDataBlockStream;
use prost::Message;
use tokio_stream::StreamExt;

use crate::api::rpc::{ExecuteAction, FetchPartitionRequest};
use crate::protobuf::{FlightActionRequest, FlightRequest};

pub struct FlightClient {
    client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl FlightClient {
    pub async fn try_create(addr: String) -> Result<Self> {
        let client = FlightServiceClient::connect(format!("http://{}", addr)).await?;
        Ok(Self { client })
    }

    pub async fn execute(&mut self, action: &ExecuteAction) -> Result<SendableDataBlockStream> {
        let flight_request = FlightRequest {
            action: serde_json::to_string(action)?,
        };
        let mut buf = vec![];
        flight_request.encode(&mut buf)?;
        let request = tonic::Request::new(Ticket { ticket: buf });

        let mut stream = self.client.do_get(request).await?.into_inner();
        match stream.message().await? {
            Some(flight_data) => {
                let schema = Arc::new(DataSchema::try_from(&flight_data)?);
                let block_stream = stream.map(move |flight_data| {
                    let batch = flight_data_to_arrow_batch(&flight_data?, schema.clone(), &[])?;
                    DataBlock::try_from_arrow_batch(&batch)
                });
                Ok(Box::pin(block_stream))
            }
            None => bail!(
                "Can not receive data from flight server, action:{:?}",
                action
            ),
        }
    }

    pub async fn fetch_partition_action(&mut self, uuid: String, nums: u32) -> Result<Partitions> {
        // Request.
        let action_request = FlightActionRequest {
            body: serde_json::to_string(&FetchPartitionRequest { uuid, nums })?,
        };
        let mut buf = vec![];
        action_request.encode(&mut buf)?;
        let tonic_request = tonic::Request::new(Action {
            r#type: "fetch_partition_action".to_string(),
            body: buf,
        });

        // Stream.
        let mut stream = self.client.do_action(tonic_request).await?.into_inner();
        match stream.message().await? {
            None => {
                bail!("Can not receive data from flight server, action: fetch_partition_action")
            }
            Some(resp) => {
                let parts: Partitions = serde_json::from_slice(&resp.body)?;
                Ok(parts)
            }
        }
    }
}
