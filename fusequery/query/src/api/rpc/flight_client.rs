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
use common_planners::{Partitions, PlanNode};
use common_streams::SendableDataBlockStream;
use prost::Message;
use tokio_stream::StreamExt;

use crate::api::rpc::flight_action::DoActionAction;
use crate::api::rpc::{DoGetAction, ExecutePlanAction, FetchPartitionAction};
use crate::protobuf::FlightRequest;

pub struct FlightClient {
    client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl FlightClient {
    pub async fn try_create(addr: String) -> Result<Self> {
        let client = FlightServiceClient::connect(format!("http://{}", addr)).await?;
        Ok(Self { client })
    }

    // Execute do_get by action.
    async fn do_get(&mut self, action: &DoGetAction) -> Result<SendableDataBlockStream> {
        // Encode flight request.
        let flight_request = FlightRequest {
            body: serde_json::to_string(action)?,
        };

        // Encode tonic request.
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

    /// Execute the plan in the remote and get the stream.
    pub async fn execute_remote_plan_action(
        &mut self,
        job_id: String,
        plan: &PlanNode,
    ) -> Result<SendableDataBlockStream> {
        let action = DoGetAction::ExecutePlan(ExecutePlanAction {
            job_id: job_id.clone(),
            plan: plan.clone(),
        });
        self.do_get(&action).await
    }

    pub async fn do_action(&mut self, action: &DoActionAction) -> Result<Vec<u8>> {
        // Request.
        let action_request = FlightRequest {
            body: serde_json::to_string(&action)?,
        };
        let mut buf = vec![];
        action_request.encode(&mut buf)?;
        let tonic_request = tonic::Request::new(Action {
            r#type: "".to_string(),
            body: buf,
        });

        // Stream.
        let mut stream = self.client.do_action(tonic_request).await?.into_inner();
        match stream.message().await? {
            None => {
                bail!(
                    "Can not receive data from flight server, action: {:?}",
                    action
                )
            }
            Some(resp) => Ok(resp.body),
        }
    }

    pub async fn fetch_partition_action(&mut self, uuid: String, nums: u32) -> Result<Partitions> {
        let action = DoActionAction::FetchPartition(FetchPartitionAction { uuid, nums });
        let body = self.do_action(&action).await?;
        let parts: Partitions = serde_json::from_slice(&body)?;
        Ok(parts)
    }
}
