// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::{Result, ErrorCodes};
use tonic::transport::channel::Channel;
use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_flights::ExecutePlanWithShuffleAction;
use tonic::Request;
use common_arrow::arrow_flight::Action;
use tokio::time::Duration;
use crate::api::rpc::flight_data_stream::FlightDataStream;
use common_arrow::arrow::datatypes::SchemaRef;
use crate::api::rpc::{to_status, from_status};

pub struct FlightClient {
    inner: FlightServiceClient<Channel>
}


// TODO: Integration testing required
impl FlightClient {
    pub async fn connect(addr: String) -> Result<FlightClient> {
        // TODO: For different addresses, it should be singleton
        Ok(FlightClient {
            inner: FlightServiceClient::connect(format!("http://{}", addr)).await
                .map_err(|e| ErrorCodes::UnknownException(e.to_string()))?
        })
        // cache
    }

    pub async fn prepare_query_stage(&mut self, action: ExecutePlanWithShuffleAction, timeout: u64) -> Result<()> {
        self.do_action(Action {
            r#type: "PrepareQueryStage".to_string(),
            body: serde_json::to_string(&action).map_err(ErrorCodes::from_serde)?.as_bytes().to_vec(),
        }, timeout).await?;

        Ok(())
    }

    // Execute do_action.
    async fn do_action(&mut self, action: Action, timeout: u64) -> Result<Vec<u8>> {
        let action_type = action.r#type.clone();
        let mut request = Request::new(action);
        request.set_timeout(Duration::from_secs(timeout));

        let response = self.inner.do_action(request).await.map_err(from_status);

        match response?.into_inner().message().await.map_err(from_status)? {
            Some(response) => Ok(response.body),
            None => Result::Err(ErrorCodes::EmptyDataFromServer(
                format!(
                    "Can not receive data from flight server, action: {:?}",
                    action_type
                )
            )),
        }
    }
}
