// Copyright 2021 Datafuse Labs
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

use databend_common_arrow::arrow_format::flight::data::Action;
use databend_common_exception::ErrorCode;
use databend_common_exception::ToErrorCode;
use tonic::Status;

use crate::servers::flight::v1::packets::InitNodesChannelPacket;
use crate::servers::flight::v1::packets::KillQueryPacket;
use crate::servers::flight::v1::packets::QueryFragmentsPlanPacket;
use crate::servers::flight::v1::packets::SetPriorityPacket;
use crate::servers::flight::v1::packets::TruncateTablePacket;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct InitQueryFragmentsPlan {
    pub executor_packet: QueryFragmentsPlanPacket,
}

impl TryInto<InitQueryFragmentsPlan> for Vec<u8> {
    type Error = Status;

    fn try_into(self) -> Result<InitQueryFragmentsPlan, Self::Error> {
        match serde_json::from_slice::<InitQueryFragmentsPlan>(&self) {
            Err(cause) => Err(Status::invalid_argument(cause.to_string())),
            Ok(action) => Ok(action),
        }
    }
}

impl TryInto<Vec<u8>> for InitQueryFragmentsPlan {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err_to_code(
            ErrorCode::Internal,
            || "Logical error: cannot serialize InitQueryFragmentsPlan.",
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct InitNodesChannel {
    pub init_nodes_channel_packet: InitNodesChannelPacket,
}

impl TryInto<InitNodesChannel> for Vec<u8> {
    type Error = Status;

    fn try_into(self) -> Result<InitNodesChannel, Self::Error> {
        match serde_json::from_slice::<InitNodesChannel>(&self) {
            Err(cause) => Err(Status::invalid_argument(cause.to_string())),
            Ok(action) => Ok(action),
        }
    }
}

impl TryInto<Vec<u8>> for InitNodesChannel {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err_to_code(
            ErrorCode::Internal,
            || "Logical error: cannot serialize PreparePublisher.",
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TruncateTable {
    pub packet: TruncateTablePacket,
}

impl TryInto<TruncateTable> for Vec<u8> {
    type Error = Status;

    fn try_into(self) -> Result<TruncateTable, Self::Error> {
        match serde_json::from_slice::<TruncateTable>(&self) {
            Err(cause) => Err(Status::invalid_argument(cause.to_string())),
            Ok(action) => Ok(action),
        }
    }
}

impl TryInto<Vec<u8>> for TruncateTable {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err_to_code(
            ErrorCode::Internal,
            || "Logical error: cannot serialize PreparePublisher.",
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct KillQuery {
    pub packet: KillQueryPacket,
}

impl TryInto<KillQuery> for Vec<u8> {
    type Error = Status;

    fn try_into(self) -> Result<KillQuery, Self::Error> {
        match serde_json::from_slice::<KillQuery>(&self) {
            Err(cause) => Err(Status::invalid_argument(cause.to_string())),
            Ok(action) => Ok(action),
        }
    }
}

impl TryInto<Vec<u8>> for KillQuery {
    type Error = ErrorCode;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err_to_code(
            ErrorCode::Internal,
            || "Logical error: cannot serialize KillPacket.",
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SetPriority {
    pub packet: SetPriorityPacket,
}

impl TryInto<SetPriority> for Vec<u8> {
    type Error = Status;

    fn try_into(self) -> Result<SetPriority, Self::Error> {
        match serde_json::from_slice::<SetPriority>(&self) {
            Err(cause) => Err(Status::invalid_argument(cause.to_string())),
            Ok(action) => Ok(action),
        }
    }
}

impl TryInto<Vec<u8>> for SetPriority {
    type Error = ErrorCode;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&self).map_err_to_code(
            ErrorCode::Internal,
            || "Logical error: cannot serialize SetPriority.",
        )
    }
}

#[derive(Clone, Debug)]
pub enum FlightAction {
    InitQueryFragmentsPlan(InitQueryFragmentsPlan),
    InitNodesChannel(InitNodesChannel),
    ExecutePartialQuery(String),
    TruncateTable(TruncateTable),
    KillQuery(KillQuery),
    SetPriority(SetPriority),
}

impl TryInto<FlightAction> for Action {
    type Error = Status;

    fn try_into(self) -> Result<FlightAction, Self::Error> {
        match self.r#type.as_str() {
            "InitQueryFragmentsPlan" => {
                Ok(FlightAction::InitQueryFragmentsPlan(self.body.try_into()?))
            }
            "InitNodesChannel" => Ok(FlightAction::InitNodesChannel(self.body.try_into()?)),
            "ExecutePartialQuery" => unsafe {
                let (buf, length, capacity) = self.body.into_raw_parts();
                Ok(FlightAction::ExecutePartialQuery(String::from_raw_parts(
                    buf, length, capacity,
                )))
            },
            "TruncateTable" => Ok(FlightAction::TruncateTable(self.body.try_into()?)),
            "KillQuery" => Ok(FlightAction::KillQuery(self.body.try_into()?)),
            "SetPriority" => Ok(FlightAction::SetPriority(self.body.try_into()?)),
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
            FlightAction::InitQueryFragmentsPlan(init_query_fragments_plan) => Ok(Action {
                r#type: String::from("InitQueryFragmentsPlan"),
                body: init_query_fragments_plan.try_into()?,
            }),
            FlightAction::InitNodesChannel(init_nodes_channel) => Ok(Action {
                r#type: String::from("InitNodesChannel"),
                body: init_nodes_channel.try_into()?,
            }),
            FlightAction::ExecutePartialQuery(query_id) => Ok(Action {
                r#type: String::from("ExecutePartialQuery"),
                body: serde_json::to_vec(&query_id).unwrap(),
            }),
            FlightAction::TruncateTable(truncate_table) => Ok(Action {
                r#type: String::from("TruncateTable"),
                body: truncate_table.try_into()?,
            }),
            FlightAction::KillQuery(kill_query) => Ok(Action {
                r#type: String::from("KillQuery"),
                body: kill_query.try_into()?,
            }),
            FlightAction::SetPriority(set_priority) => Ok(Action {
                r#type: String::from("SetPriority"),
                body: set_priority.try_into()?,
            }),
        }
    }
}
