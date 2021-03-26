// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use common_datablocks::DataBlock;
use prost::Message;

use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::protobuf::FlightRequest;
use crate::rpcs::rpc::ExecuteAction;

pub struct FlightClient {
    client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl FlightClient {
    pub async fn try_create(addr: String) -> FuseQueryResult<Self> {
        let client = FlightServiceClient::connect(format!("http://{}", addr)).await?;
        Ok(Self { client })
    }

    pub async fn execute(
        &mut self,
        action: &ExecuteAction,
    ) -> FuseQueryResult<SendableDataBlockStream> {
        let flight_request = FlightRequest {
            action: serde_json::to_string(action)?,
        };
        let mut buf = vec![];
        flight_request.encode(&mut buf)?;
        let request = tonic::Request::new(Ticket { ticket: buf });

        let mut stream = self.client.do_get(request).await?.into_inner();
        match stream.message().await? {
            Some(flight_data) => {
                let schema = Arc::new(Schema::try_from(&flight_data)?);
                let mut blocks = vec![];
                while let Some(flight_data) = stream.message().await? {
                    let batch = flight_data_to_arrow_batch(&flight_data, schema.clone(), &[])?;
                    blocks.push(DataBlock::try_from_arrow_batch(&batch)?);
                }
                Ok(Box::pin(DataBlockStream::create(schema, None, blocks)))
            }
            None => Err(FuseQueryError::build_internal_error(format!(
                "Can not receive data from flight server, action:{:?}",
                action
            ))),
        }
    }
}
