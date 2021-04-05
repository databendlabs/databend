// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use anyhow::{bail, Result};
use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::{Action, Ticket};
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_planners::{Partitions, PlanNode};
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;
use tonic::Request;

use crate::api::rpc::flight_action::DoActionAction;
use crate::api::rpc::{DoGetAction, ExecutePlanAction, FetchPartitionAction};

pub struct FlightClient {
    client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl FlightClient {
    pub async fn try_create(addr: String) -> Result<Self> {
        let client = FlightServiceClient::connect(format!("http://{}", addr)).await?;
        Ok(Self { client })
    }

    /// Execute the plan in the remote action and get the block stream.
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

    // Fetch partition action and get the result.
    pub async fn fetch_partition_action(&mut self, uuid: String, nums: u32) -> Result<Partitions> {
        let action = DoActionAction::FetchPartition(FetchPartitionAction { uuid, nums });
        let body = self.do_action(&action).await?;
        let parts: Partitions = serde_json::from_slice(&body)?;
        Ok(parts)
    }

    // Execute do_get.
    async fn do_get(&mut self, action: &DoGetAction) -> Result<SendableDataBlockStream> {
        let request: Request<Ticket> = action.try_into()?;
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

    async fn do_action(&mut self, action: &DoActionAction) -> Result<Vec<u8>> {
        let request: Request<Action> = action.try_into()?;
        let mut stream = self.client.do_action(request).await?.into_inner();
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
}
