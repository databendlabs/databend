// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::convert::TryInto;
use std::sync::Arc;

use common_arrow::arrow_format::flight::data::Action;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_planners::Expression;
use common_planners::PlanNode;
use tonic::Status;
use crate::api::ExecutorPacket;
use crate::sessions::QueryContext;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ShuffleAction {
    pub query_id: String,
    pub stage_id: String,
    pub plan: PlanNode,
    pub sinks: Vec<String>,
    pub scatters_expression: Expression,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BroadcastAction {
    pub query_id: String,
    pub stage_id: String,
    pub plan: PlanNode,
    pub sinks: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CancelAction {
    pub query_id: String,
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

impl TryInto<BroadcastAction> for Vec<u8> {
    type Error = Status;

    fn try_into(self) -> Result<BroadcastAction, Self::Error> {
        match std::str::from_utf8(&self) {
            Err(cause) => Err(Status::invalid_argument(cause.to_string())),
            Ok(utf8_body) => match serde_json::from_str::<BroadcastAction>(utf8_body) {
                Err(cause) => Err(Status::invalid_argument(cause.to_string())),
                Ok(action) => Ok(action),
            },
        }
    }
}

impl TryInto<Vec<u8>> for BroadcastAction {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err_to_code(ErrorCode::LogicalError, || {
            "Logical error: cannot serialize BroadcastAction."
        })
    }
}

impl TryInto<CancelAction> for Vec<u8> {
    type Error = Status;

    fn try_into(self) -> Result<CancelAction, Self::Error> {
        match std::str::from_utf8(&self) {
            Err(cause) => Err(Status::invalid_argument(cause.to_string())),
            Ok(utf8_body) => match serde_json::from_str::<CancelAction>(utf8_body) {
                Err(cause) => Err(Status::invalid_argument(cause.to_string())),
                Ok(action) => Ok(action),
            },
        }
    }
}

impl TryInto<Vec<u8>> for CancelAction {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err_to_code(ErrorCode::LogicalError, || {
            "Logical error: cannot serialize BroadcastAction."
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct PrepareExecutor {
    pub executor_packet: ExecutorPacket,
}

impl TryInto<PrepareExecutor> for Vec<u8> {
    type Error = Status;

    fn try_into(self) -> Result<PrepareExecutor, Self::Error> {
        match std::str::from_utf8(&self) {
            Err(cause) => Err(Status::invalid_argument(cause.to_string())),
            Ok(utf8_body) => match serde_json::from_str::<PrepareExecutor>(utf8_body) {
                Err(cause) => Err(Status::invalid_argument(cause.to_string())),
                Ok(action) => Ok(action),
            },
        }
    }
}

impl TryInto<Vec<u8>> for PrepareExecutor {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err_to_code(ErrorCode::LogicalError, || {
            "Logical error: cannot serialize BroadcastAction."
        })
    }
}

#[derive(Clone, Debug)]
pub enum FlightAction {
    PrepareShuffleAction(ShuffleAction),
    BroadcastAction(BroadcastAction),
    CancelAction(CancelAction),
    PrepareExecutor(PrepareExecutor),
}

impl FlightAction {
    pub fn get_query_id(&self) -> String {
        match self {
            FlightAction::BroadcastAction(action) => action.query_id.clone(),
            FlightAction::PrepareShuffleAction(action) => action.query_id.clone(),
            _ => unimplemented!(),
        }
    }

    pub fn get_stage_id(&self) -> String {
        match self {
            FlightAction::BroadcastAction(action) => action.stage_id.clone(),
            FlightAction::PrepareShuffleAction(action) => action.stage_id.clone(),
            _ => unimplemented!(),
        }
    }

    pub fn get_sinks(&self) -> Vec<String> {
        match self {
            FlightAction::BroadcastAction(action) => action.sinks.clone(),
            FlightAction::PrepareShuffleAction(action) => action.sinks.clone(),
            _ => unimplemented!(),
        }
    }

    pub fn get_plan(&self) -> PlanNode {
        match self {
            FlightAction::BroadcastAction(action) => action.plan.clone(),
            FlightAction::PrepareShuffleAction(action) => action.plan.clone(),
            _ => unimplemented!(),
        }
    }

    pub fn get_scatter_expression(&self) -> Option<Expression> {
        match self {
            FlightAction::BroadcastAction(_) => None,
            FlightAction::PrepareShuffleAction(action) => Some(action.scatters_expression.clone()),
            _ => unimplemented!(),
        }
    }
}

impl TryInto<FlightAction> for Action {
    type Error = Status;

    fn try_into(self) -> Result<FlightAction, Self::Error> {
        match self.r#type.as_str() {
            "PrepareShuffleAction" => Ok(FlightAction::PrepareShuffleAction(self.body.try_into()?)),
            "BroadcastAction" => Ok(FlightAction::BroadcastAction(self.body.try_into()?)),
            "CancelAction" => Ok(FlightAction::CancelAction(self.body.try_into()?)),
            "PrepareExecutor" => Ok(FlightAction::PrepareExecutor(self.body.try_into()?)),
            un_implemented => Err(Status::unimplemented(format!(
                "UnImplement action {}",
                un_implemented
            ))),
        }
    }
}

impl TryInto<Action> for FlightAction {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Action, Self::Error> {
        match self {
            FlightAction::PrepareShuffleAction(shuffle_action) => Ok(Action {
                r#type: String::from("PrepareShuffleAction"),
                body: shuffle_action.try_into()?,
            }),
            FlightAction::BroadcastAction(broadcast_action) => Ok(Action {
                r#type: String::from("BroadcastAction"),
                body: broadcast_action.try_into()?,
            }),
            FlightAction::CancelAction(cancel_action) => Ok(Action {
                r#type: String::from("CancelAction"),
                body: cancel_action.try_into()?,
            }),
            FlightAction::PrepareExecutor(executor) => Ok(Action {
                r#type: String::from("PrepareExecutor"),
                body: executor.try_into()?,
            })
        }
    }
}
