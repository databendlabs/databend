// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use prost::Message;

use crate::datablocks::DataBlock;
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::protobuf::ExecuteRequest;
use crate::rpcs::rpc::ExecuteAction;

pub struct FlightClient {
    client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl FlightClient {
    pub async fn try_create(addr: String) -> FuseQueryResult<Self> {
        let client = FlightServiceClient::connect(format!("http://{}", addr))
            .await
            .map_err(|e| {
                FuseQueryError::build_internal_error(format!(
                    "Error connecting to flight server: {}, error: {}",
                    addr, e
                ))
            })?;
        Ok(Self { client })
    }

    pub async fn execute(
        &mut self,
        action: &ExecuteAction,
    ) -> FuseQueryResult<SendableDataBlockStream> {
        let execute_request = ExecuteRequest {
            action: serde_json::to_string(action)?,
        };
        let mut buf = vec![];
        execute_request.encode(&mut buf)?;
        let request = tonic::Request::new(Ticket { ticket: buf });

        let mut stream = self
            .client
            .do_get(request)
            .await
            .map_err(super::error::tonic_to_fuse_err)?
            .into_inner();

        match stream
            .message()
            .await
            .map_err(super::error::tonic_to_fuse_err)?
        {
            Some(flight_data) => {
                let schema = Arc::new(Schema::try_from(&flight_data)?);
                let mut blocks = vec![];
                while let Some(flight_data) = stream
                    .message()
                    .await
                    .map_err(super::error::tonic_to_fuse_err)?
                {
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
