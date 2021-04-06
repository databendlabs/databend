// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

/// Actions for query do_get.
use std::convert::TryInto;
use std::io::Cursor;

use common_arrow::arrow_flight::Ticket;
use common_planners::PlanNode;
use prost::Message;
use tonic::Request;

use crate::protobuf::FlightQueryRequest;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ExecutePlanAction {
    pub job_id: String,
    pub plan: PlanNode,
}

// Action wrapper for do_get.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum DoGetAction {
    ExecutePlan(ExecutePlanAction),
}

/// Try convert tonic::Request<Ticket> to DoGetAction.
impl TryInto<DoGetAction> for tonic::Request<Ticket> {
    type Error = tonic::Status;

    fn try_into(self) -> Result<DoGetAction, Self::Error> {
        let ticket = self.into_inner();
        let mut buf = Cursor::new(&ticket.ticket);

        // Decode FlightQueryRequest from buffer.
        let request: FlightQueryRequest = FlightQueryRequest::decode(&mut buf)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        // Decode DoGetAction from request body.
        let json_str = request.body.as_str();
        let action = serde_json::from_str::<DoGetAction>(json_str)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(action)
    }
}

/// Try convert DoGetAction to tonic::Request<Ticket>.
impl TryInto<Request<Ticket>> for &DoGetAction {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Request<Ticket>, Self::Error> {
        let flight_request = FlightQueryRequest {
            body: serde_json::to_string(&self)?,
        };

        let mut buf = vec![];
        flight_request.encode(&mut buf)?;
        let request = tonic::Request::new(Ticket { ticket: buf });
        Ok(request)
    }
}
