// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::io::Cursor;
use std::sync::Arc;

use common_arrow::arrow_flight::Action;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::Part;
use common_planners::ScanPlan;
use common_store_api::CreateDatabaseActionResult;
use common_store_api::CreateTableActionResult;
use common_store_api::DropDatabaseActionResult;
use common_store_api::DropTableActionResult;
use common_store_api::GetDatabaseActionResult;
use common_store_api::GetKVActionResult;
use common_store_api::GetTableActionResult;
use common_store_api::ReadPlanResult;
use common_store_api::UpsertKVActionResult;
use prost::Message;
use tonic::Request;

use crate::protobuf::FlightStoreRequest;

pub trait RequestFor {
    type Reply;
}

macro_rules! action_declare {
    ($req:ident, $reply:ident, $enum_ctor:expr) => {
        impl RequestFor for $req {
            type Reply = $reply;
        }

        impl From<$req> for StoreDoAction {
            fn from(act: $req) -> Self {
                $enum_ctor(act)
            }
        }
    };
}

// === general-kv: upsert ===
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct UpsertKVAction {
    pub key: String,
    pub seq: Option<u64>,
    pub value: Vec<u8>,
}

impl RequestFor for UpsertKVAction {
    type Reply = UpsertKVActionResult;
}

impl From<UpsertKVAction> for StoreDoAction {
    fn from(act: UpsertKVAction) -> Self {
        StoreDoAction::UpsertKV(act)
    }
}

// === general-kv: get ===
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetKVAction {
    pub key: String,
}

impl RequestFor for GetKVAction {
    type Reply = GetKVActionResult;
}

impl From<GetKVAction> for StoreDoAction {
    fn from(act: GetKVAction) -> Self {
        StoreDoAction::GetKV(act)
    }
}

// === database: create ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateDatabaseAction {
    pub plan: CreateDatabasePlan,
}

action_declare!(
    CreateDatabaseAction,
    CreateDatabaseActionResult,
    StoreDoAction::CreateDatabase
);

// === database: get ===
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetDatabaseAction {
    pub db: String,
}

action_declare!(
    GetDatabaseAction,
    GetDatabaseActionResult,
    StoreDoAction::GetDatabase
);

// === database: drop ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropDatabaseAction {
    pub plan: DropDatabasePlan,
}

action_declare!(
    DropDatabaseAction,
    DropDatabaseActionResult,
    StoreDoAction::DropDatabase
);

// === table: create ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateTableAction {
    pub plan: CreateTablePlan,
}

action_declare!(
    CreateTableAction,
    CreateTableActionResult,
    StoreDoAction::CreateTable
);

// === table: drop ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropTableAction {
    pub plan: DropTablePlan,
}

impl RequestFor for DropTableAction {
    type Reply = DropTableActionResult;
}

impl From<DropTableAction> for StoreDoAction {
    fn from(act: DropTableAction) -> Self {
        StoreDoAction::DropTable(act)
    }
}

// === table: get ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableAction {
    pub db: String,
    pub table: String,
}

impl RequestFor for GetTableAction {
    type Reply = GetTableActionResult;
}

impl From<GetTableAction> for StoreDoAction {
    fn from(act: GetTableAction) -> Self {
        StoreDoAction::GetTable(act)
    }
}

// === partition: read_plan===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ReadPlanAction {
    pub scan_plan: ScanPlan,
}

impl RequestFor for ReadPlanAction {
    type Reply = ReadPlanResult;
}

impl From<ReadPlanAction> for StoreDoAction {
    fn from(act: ReadPlanAction) -> Self {
        StoreDoAction::ReadPlan(act)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DataPartInfo {
    pub part: Part,
    pub stats: Statistics,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct AddUserActionResult;

// currently, it is the same as `AddUserAction`, may be changed latter
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct UpdateUserAction {
    pub username: String,
    pub new_password: Option<String>,
    pub new_salt: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct UpdateUserActionResult;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DropUserAction {
    pub username: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DropUserActionResult;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetUsersAction {
    pub usernames: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct UserInfo {
    pub name: String,
    pub password_sha256: [u8; 32],
    pub salt_sha256: [u8; 32],
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetUsersActionResult {
    pub users_info: Vec<Option<UserInfo>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetAllUsersAction {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetAllUsersActionResult {
    pub users_info: Vec<UserInfo>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetUserAction {
    pub username: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetUserActionResult {
    pub user_info: Option<UserInfo>,
}

// TODO maybe we can avoid the memory copy
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct KVPutAction {
    pub key: Arc<[u8]>,
    pub value: Arc<[u8]>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct KVPutActionResult;

// TODO better way to encode the relation of req/resp into types

// Action wrapper for do_action.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum StoreDoAction {
    // meta-database
    CreateDatabase(CreateDatabaseAction),
    GetDatabase(GetDatabaseAction),
    DropDatabase(DropDatabaseAction),
    // meta-table
    CreateTable(CreateTableAction),
    DropTable(DropTableAction),
    // storage
    ReadPlan(ReadPlanAction),
    GetTable(GetTableAction),
    // general purpose kv
    UpsertKV(UpsertKVAction),
    GetKV(GetKVAction),

    KVPut(KVPutAction),
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
