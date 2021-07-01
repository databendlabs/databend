// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::FlightData;
use common_arrow::arrow_flight::Ticket;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio::time::Duration;
use common_streams::SendableDataBlockStream;
use tonic::transport::channel::Channel;
use tonic::Request;
use tonic::Streaming;

use crate::api::rpc::flight_actions::FlightAction;
use crate::api::rpc::flight_data_stream::FlightDataStream;
use crate::api::rpc::flight_tickets::FlightTicket;
use crate::api::ShuffleAction;

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
        ticket: FlightTicket,
        schema: DataSchemaRef,
        timeout: u64,
    ) -> Result<SendableDataBlockStream> {
        let ticket = ticket.try_into()?;
        let inner = self.do_get(ticket, timeout).await?;
        Ok(Box::pin(FlightDataStream::from_remote(schema, inner)))
    }

    pub async fn prepare_query_stage(&mut self, action: ShuffleAction, timeout: u64) -> Result<()> {
        let action = FlightAction::PrepareShuffleAction(action);
        self.do_action(action, timeout).await?;
        Ok(())
    }

    // Execute do_get.
    async fn do_get(&mut self, ticket: Ticket, timeout: u64) -> Result<Streaming<FlightData>> {
        let mut request = Request::new(ticket);
        request.set_timeout(Duration::from_secs(timeout));

        let response = self.inner.do_get(request).await?;
        Ok(response.into_inner())
    }

    // Execute do_action.
    async fn do_action(&mut self, action: FlightAction, timeout: u64) -> Result<Vec<u8>> {
        let action: Action = action.try_into()?;
        let action_type = action.r#type.clone();
        let mut request = Request::new(action);
        request.set_timeout(Duration::from_secs(timeout));

        let response = self.inner.do_action(request).await?;

        match response.into_inner().message().await? {
            Some(response) => Ok(response.body),
            None => Result::Err(ErrorCode::EmptyDataFromServer(format!(
                "Can not receive data from flight server, action: {:?}",
                action_type
            ))),
        }
    }
}
