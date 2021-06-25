// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::convert::TryInto;
use std::io::Cursor;

use common_arrow::arrow_flight;
use common_arrow::arrow_flight::Action;
use common_datavalues::DataSchemaRef;
use common_metatypes::SeqValue;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::Part;
use common_planners::ScanPlan;
use common_planners::Statistics;
use prost::Message;
use tonic::Request;

use crate::protobuf::FlightStoreRequest;

// === general-kv: upsert ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct UpsertKVAction {
    pub key: String,
    pub seq: Option<u64>,
    pub value: Vec<u8>,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct UpsertKVActionResult {
    /// prev is the value before upsert.
    pub prev: Option<SeqValue>,
    /// result is the value after upsert.
    pub result: Option<SeqValue>,
}

// === general-kv: get ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetKVAction {
    pub key: String,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetKVActionResult {
    pub result: Option<SeqValue>,
}

// === part: scan ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ReadPlanAction {
    pub scan: ScanPlan,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct ReadPlanActionResult {}

// === database: create ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateDatabaseAction {
    pub plan: CreateDatabasePlan,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDatabaseActionResult {
    pub database_id: i64,
}

// === database: get ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetDatabaseAction {
    pub db: String,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetDatabaseActionResult {
    pub database_id: i64,
    pub db: String,
}

// === database: drop ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropDatabaseAction {
    pub plan: DropDatabasePlan,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DropDatabaseActionResult {}

// === table: create ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateTableAction {
    pub plan: CreateTablePlan,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateTableActionResult {
    pub table_id: i64,
}

// === table: drop ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropTableAction {
    pub plan: DropTablePlan,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DropTableActionResult {}

// === table: get ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableAction {
    pub db: String,
    pub table: String,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableActionResult {
    pub table_id: i64,
    pub db: String,
    pub name: String,
    pub schema: DataSchemaRef,
}

// === partition: scan ===

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ScanPartitionAction {
    pub scan_plan: ScanPlan,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DataPartInfo {
    pub part: Part,
    pub stats: Statistics,
}

pub type ScanPartitionResult = Option<Vec<DataPartInfo>>;

// Action wrapper for do_action.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum StoreDoAction {
    ReadPlan(ReadPlanAction),
    CreateDatabase(CreateDatabaseAction),
    GetDatabase(GetDatabaseAction),
    DropDatabase(DropDatabaseAction),
    CreateTable(CreateTableAction),
    DropTable(DropTableAction),
    ScanPartition(ScanPartitionAction),
    GetTable(GetTableAction),

    // general purpose kv
    UpsertKV(UpsertKVAction),
    GetKV(GetKVAction),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum StoreDoActionResult {
    ReadPlan(ReadPlanActionResult),
    CreateDatabase(CreateDatabaseActionResult),
    GetDatabase(GetDatabaseActionResult),
    DropDatabase(DropDatabaseActionResult),
    CreateTable(CreateTableActionResult),
    DropTable(DropTableActionResult),
    ScanPartition(ScanPartitionResult),
    GetTable(GetTableActionResult),

    // general purpose kv
    UpsertKV(UpsertKVActionResult),
    GetKV(GetKVActionResult),
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

impl TryFrom<arrow_flight::Result> for StoreDoActionResult {
    type Error = anyhow::Error;
    fn try_from(rst: arrow_flight::Result) -> Result<Self, Self::Error> {
        let action_rst = serde_json::from_slice::<StoreDoActionResult>(&rst.body)?;

        Ok(action_rst)
    }
}

impl From<StoreDoActionResult> for arrow_flight::Result {
    fn from(action_rst: StoreDoActionResult) -> Self {
        let body = serde_json::to_vec(&action_rst).unwrap();

        arrow_flight::Result { body }
    }
}
