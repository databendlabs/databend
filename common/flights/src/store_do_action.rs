// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

/// Actions for store do_action.
use std::convert::TryInto;
use std::io::Cursor;

use common_arrow::arrow_flight::Action;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use prost::Message;
use tonic::Request;

use crate::protobuf::FlightStoreRequest;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateDatabaseAction {
    pub plan: CreateDatabasePlan,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateTableAction {
    pub plan: CreateTablePlan,
}

// Action wrapper for do_action.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum StoreDoAction {
    CreateDatabase(CreateDatabaseAction),
    CreateTable(CreateTableAction),
}

/// Try convert tonic::Request<Action> to DoActionAction.
impl TryInto<StoreDoAction> for Request<Action> {
    type Error = tonic::Status;

    fn try_into(self) -> Result<StoreDoAction, Self::Error> {
        let action = self.into_inner();
        let mut buf = Cursor::new(&action.body);

        // Decode FlightRequest from buffer.
        let request: FlightStoreRequest = FlightStoreRequest::decode(&mut buf)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        // Decode DoActionAction from flight request body.
        let json_str = request.body.as_str();
        let action = serde_json::from_str::<StoreDoAction>(json_str)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(action)
    }
}

/// Try convert DoActionAction to tonic::Request<Action>.
impl TryInto<Request<Action>> for &StoreDoAction {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Request<Action>, Self::Error> {
        let flight_request = FlightStoreRequest {
            body: serde_json::to_string(&self)?,
        };
        let mut buf = vec![];
        flight_request.encode(&mut buf)?;
        let request = tonic::Request::new(Action {
            r#type: "".to_string(),
            body: buf,
        });
        Ok(request)
    }
}
