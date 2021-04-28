// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::convert::TryInto;
use std::sync::Arc;

use anyhow::bail;
use anyhow::Result;
use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::{Action, FlightData};
use common_arrow::arrow_flight::Ticket;
use common_datavalues::DataSchema;
use common_flights::query_do_action::FetchPartitionAction;
use common_flights::query_do_action::QueryDoAction;
use common_flights::query_do_get::ExecutePlanAction;
use common_flights::query_do_get::QueryDoGet;
use common_planners::Partitions;
use common_planners::PlanNode;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;
use tonic::{Request, Status};
use common_exception::ErrorCodes;
use common_datablocks::DataBlock;
use common_arrow::arrow::datatypes::SchemaRef;

pub struct FlightClient {
    client: FlightServiceClient<tonic::transport::channel::Channel>
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
        plan: &PlanNode
    ) -> Result<SendableDataBlockStream> {
        let action = QueryDoGet::ExecutePlan(ExecutePlanAction {
            job_id: job_id.clone(),
            plan: plan.clone()
        });
        self.do_get(&action).await
    }

    // Fetch partition action and get the result.
    pub async fn fetch_partition_action(&mut self, uuid: String, nums: u32) -> Result<Partitions> {
        let action = QueryDoAction::FetchPartition(FetchPartitionAction { uuid, nums });
        let body = self.do_action(&action).await?;
        let parts: Partitions = serde_json::from_slice(&body)?;
        Ok(parts)
    }

    // Execute do_get.
    async fn do_get(&mut self, action: &QueryDoGet) -> Result<SendableDataBlockStream> {
        let request: Request<Ticket> = action.try_into()?;
        let mut stream = self.client.do_get(request).await?.into_inner();
        match stream.message().await? {
            Some(flight_data) => {
                let schema = Arc::new(DataSchema::try_from(&flight_data)?);
                let block_stream = stream.map(move |flight_data| {
                    fn fetch_impl(data: Result<FlightData, Status>, schema: &SchemaRef) -> anyhow::Result<DataBlock> {
                        let batch = flight_data_to_arrow_batch(&data?, schema.clone(), &[])?;
                        batch.try_into()
                    }

                    fetch_impl(flight_data, &schema).map_err(ErrorCodes::from_anyhow)
                });
                Ok(Box::pin(block_stream))
            }
            None => bail!(
                "Can not receive data from flight server, action:{:?}",
                action
            )
        }
    }

    // Execute do_action.
    async fn do_action(&mut self, action: &QueryDoAction) -> Result<Vec<u8>> {
        let request: Request<Action> = action.try_into()?;
        let mut stream = self.client.do_action(request).await?.into_inner();
        match stream.message().await? {
            None => {
                bail!(
                    "Can not receive data from flight server, action: {:?}",
                    action
                )
            }
            Some(resp) => Ok(resp.body)
        }
    }
}
