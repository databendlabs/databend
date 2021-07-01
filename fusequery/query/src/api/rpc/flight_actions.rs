// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;

use common_arrow::arrow_flight::Action;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_planners::Expression;
use common_planners::PlanNode;
use tonic::Status;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ShuffleAction {
    pub query_id: String,
    pub stage_id: String,
    pub plan: PlanNode,
    pub sinks: Vec<String>,
    pub scatters_expression: Expression,
}

impl TryInto<ShuffleAction> for Vec<u8> {
    type Error = Status;

    fn try_into(self) -> Result<ShuffleAction, Self::Error> {
        match std::str::from_utf8(&self) {
            Err(cause) => Err(Status::invalid_argument(cause.to_string())),
            Ok(utf8_body) => match serde_json::from_str::<ShuffleAction>(utf8_body) {
                Err(cause) => Err(Status::invalid_argument(cause.to_string())),
                Ok(action) => Ok(action),
            },
        }
    }
}

impl TryInto<Vec<u8>> for ShuffleAction {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err_to_code(ErrorCode::LogicalError, || {
            "Logical error: cannot serialize ShuffleAction."
        })
    }
}

pub enum FlightAction {
    PrepareQueryStage(ShuffleAction),
}

impl TryInto<FlightAction> for Action {
    type Error = Status;

    fn try_into(self) -> Result<FlightAction, Self::Error> {
        match self.r#type.as_str() {
            "PrepareQueryStage" => Ok(FlightAction::PrepareQueryStage(self.body.try_into()?)),
            action_type => Err(Status::unimplemented(format!(
                "UnImplement action {}",
                action_type
            ))),
        }
    }
}

impl TryInto<Action> for FlightAction {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Action, Self::Error> {
        match self {
            FlightAction::PrepareQueryStage(shuffle_action) => Ok(Action {
                r#type: String::from("PrepareQueryStage"),
                body: shuffle_action.try_into()?,
            }),
        }
    }
}
