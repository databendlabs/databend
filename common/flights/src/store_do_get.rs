// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

/// Actions for store do_get.
use std::convert::TryInto;
use std::io::Cursor;

use common_arrow::arrow_flight::Ticket;
use common_planners::Partitions;
use common_planners::PlanNode;
use prost::Message;
use tonic::Request;

use crate::protobuf::FlightStoreRequest;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ReadAction {
    pub partition: Partitions,
    pub push_down: PlanNode,
}

// Action wrapper for do_get.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum StoreDoGet {
    Read(ReadAction),
}

/// Try convert tonic::Request<Ticket> to DoGetAction.
impl TryInto<StoreDoGet> for tonic::Request<Ticket> {
    type Error = tonic::Status;

    fn try_into(self) -> Result<StoreDoGet, Self::Error> {
        let ticket = self.into_inner();
        let mut buf = Cursor::new(&ticket.ticket);

        // Decode FlightRequest from buffer.
        let request: FlightStoreRequest = FlightStoreRequest::decode(&mut buf)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        // Decode DoGetAction from request body.
        let json_str = request.body.as_str();
        let action = serde_json::from_str::<StoreDoGet>(json_str)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(action)
    }
}

/// Try convert DoGetAction to tonic::Request<Ticket>.
impl TryInto<Request<Ticket>> for &StoreDoGet {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Request<Ticket>, Self::Error> {
        let flight_request = FlightStoreRequest {
            body: serde_json::to_string(&self)?,
        };

        let mut buf = vec![];
        flight_request.encode(&mut buf)?;
        let request = tonic::Request::new(Ticket { ticket: buf });
        Ok(request)
    }
}
