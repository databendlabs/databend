// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::datatypes::SchemaRef;
use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::Ticket;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use tokio::time::Duration;
use tonic::transport::channel::Channel;
use tonic::Request;

use crate::api::rpc::actions::ExecutePlanWithShuffleAction;
use crate::api::rpc::flight_data_stream::FlightDataStream;
use crate::api::rpc::from_status;

pub struct FlightClient {
    inner: FlightServiceClient<Channel>,
}

// TODO: Integration testing required
impl FlightClient {
    pub fn new(inner: FlightServiceClient<Channel>) -> FlightClient {
        FlightClient { inner }
    }

    pub async fn fetch_stream(
        &mut self,
        name: String,
        schema: SchemaRef,
        timeout: u64,
    ) -> Result<SendableDataBlockStream> {
        self.do_get(
            Ticket {
                ticket: name.as_bytes().to_vec(),
            },
            schema,
            timeout,
        )
        .await
    }

    pub async fn prepare_query_stage(
        &mut self,
        action: ExecutePlanWithShuffleAction,
        timeout: u64,
    ) -> Result<()> {
        self.do_action(
            Action {
                r#type: "PrepareQueryStage".to_string(),
                body: serde_json::to_string(&action)?.as_bytes().to_vec(),
            },
            timeout,
        )
        .await?;

        Ok(())
    }

    // Execute do_get.
    async fn do_get(
        &mut self,
        ticket: Ticket,
        schema: SchemaRef,
        timeout: u64,
    ) -> Result<SendableDataBlockStream> {
        let mut request = Request::new(ticket);
        request.set_timeout(Duration::from_secs(timeout));

        let response = self.inner.do_get(request).await.map_err(from_status);

        Ok(Box::pin(FlightDataStream::from_remote(
            schema,
            response?.into_inner(),
        )))
    }

    // Execute do_action.
    async fn do_action(&mut self, action: Action, timeout: u64) -> Result<Vec<u8>> {
        let action_type = action.r#type.clone();
        let mut request = Request::new(action);
        request.set_timeout(Duration::from_secs(timeout));

        let response = self.inner.do_action(request).await.map_err(from_status);

        match response?
            .into_inner()
            .message()
            .await
            .map_err(from_status)?
        {
            Some(response) => Ok(response.body),
            None => Result::Err(ErrorCode::EmptyDataFromServer(format!(
                "Can not receive data from flight server, action: {:?}",
                action_type
            ))),
        }
    }
}
