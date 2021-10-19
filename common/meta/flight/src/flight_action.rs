// Copyright 2020 Datafuse Labs.
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
use std::io::Cursor;
use std::sync::Arc;

use common_arrow::arrow_format::flight::data::Action;
use common_exception::ErrorCode;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateTableReply;
use common_meta_types::DatabaseInfo;
use common_meta_types::GetKVActionReply;
use common_meta_types::KVMeta;
use common_meta_types::MGetKVActionReply;
use common_meta_types::MatchSeq;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::PrefixListReply;
use common_meta_types::TableInfo;
use common_meta_types::UpsertKVActionReply;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use prost::Message;
use tonic::Request;

use crate::protobuf::FlightMetaRequest;

pub trait RequestFor {
    type Reply;
}

#[macro_export]
macro_rules! action_declare {
    ($req:ident, $reply:ty, $enum_ctor:expr) => {
        impl RequestFor for $req {
            type Reply = $reply;
        }

        impl From<$req> for MetaFlightAction {
            fn from(act: $req) -> Self {
                $enum_ctor(act)
            }
        }
    };
}

// Action wrapper for do_action.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum MetaFlightAction {
    // database meta
    CreateDatabase(CreateDatabaseAction),
    GetDatabase(GetDatabaseAction),
    DropDatabase(DropDatabaseAction),
    CreateTable(CreateTableAction),
    DropTable(DropTableAction),
    GetTable(GetTableAction),
    GetTableExt(GetTableExtReq),
    GetTables(GetTablesAction),
    GetDatabases(GetDatabasesAction),

    // general purpose kv
    UpsertKV(UpsertKVAction),
    UpdateKVMeta(KVMetaAction),
    GetKV(GetKVAction),
    MGetKV(MGetKVAction),
    PrefixListKV(PrefixListReq),
}

/// Try convert tonic::Request<Action> to DoActionAction.
impl TryInto<MetaFlightAction> for Request<Action> {
    type Error = tonic::Status;

    fn try_into(self) -> Result<MetaFlightAction, Self::Error> {
        let action = self.into_inner();
        let mut buf = Cursor::new(&action.body);

        // Decode FlightRequest from buffer.
        let request: FlightMetaRequest = FlightMetaRequest::decode(&mut buf)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        // Decode DoActionAction from flight request body.
        let json_str = request.body.as_str();
        let action = serde_json::from_str::<MetaFlightAction>(json_str)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(action)
    }
}

/// Try convert DoActionAction to tonic::Request<Action>.
impl TryInto<Request<Action>> for &MetaFlightAction {
    type Error = ErrorCode;

    fn try_into(self) -> common_exception::Result<Request<Action>> {
        let flight_request = FlightMetaRequest {
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetKVAction {
    pub key: String,
}

// Explicitly defined (the request / reply relation)
// this can be simplified by using macro (see code below)
impl RequestFor for GetKVAction {
    type Reply = GetKVActionReply;
}

// Explicitly defined the converter for MetaDoAction
// It's implementations' choice, that they gonna using enum MetaDoAction as wrapper.
// This can be simplified by using macro (see code below)
impl From<GetKVAction> for MetaFlightAction {
    fn from(act: GetKVAction) -> Self {
        MetaFlightAction::GetKV(act)
    }
}

// - MGetKV

// Again, impl chooses to wrap it up
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct MGetKVAction {
    pub keys: Vec<String>,
}

// here we use a macro to simplify the declarations
action_declare!(MGetKVAction, MGetKVActionReply, MetaFlightAction::MGetKV);

// - prefix list
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct PrefixListReq(pub String);
action_declare!(
    PrefixListReq,
    PrefixListReply,
    MetaFlightAction::PrefixListKV
);

// === general-kv: upsert ===
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct UpsertKVAction {
    pub key: String,
    pub seq: MatchSeq,
    pub value: Option<Vec<u8>>,
    pub value_meta: Option<KVMeta>,
}

action_declare!(
    UpsertKVAction,
    UpsertKVActionReply,
    MetaFlightAction::UpsertKV
);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct KVMetaAction {
    pub key: String,
    pub seq: MatchSeq,
    pub value_meta: Option<KVMeta>,
}

action_declare!(
    KVMetaAction,
    UpsertKVActionReply,
    MetaFlightAction::UpdateKVMeta
);

// == database actions ==
// - create database
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateDatabaseAction {
    pub plan: CreateDatabasePlan,
}
action_declare!(
    CreateDatabaseAction,
    CreateDatabaseReply,
    MetaFlightAction::CreateDatabase
);

// - get database
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetDatabaseAction {
    pub db: String,
}
action_declare!(
    GetDatabaseAction,
    DatabaseInfo,
    MetaFlightAction::GetDatabase
);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropDatabaseAction {
    pub plan: DropDatabasePlan,
}
action_declare!(DropDatabaseAction, (), MetaFlightAction::DropDatabase);

// == table actions ==
// - create table
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CreateTableAction {
    pub plan: CreateTablePlan,
}
action_declare!(
    CreateTableAction,
    CreateTableReply,
    MetaFlightAction::CreateTable
);

// - drop table
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DropTableAction {
    pub plan: DropTablePlan,
}
action_declare!(DropTableAction, (), MetaFlightAction::DropTable);

// - get table
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableAction {
    pub db: String,
    pub table: String,
}

action_declare!(GetTableAction, Arc<TableInfo>, MetaFlightAction::GetTable);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableExtReq {
    pub tbl_id: MetaId,
    pub tbl_ver: Option<MetaVersion>,
}
action_declare!(
    GetTableExtReq,
    Arc<TableInfo>,
    MetaFlightAction::GetTableExt
);

// - get tables
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTablesAction {
    pub db: String,
}

action_declare!(
    GetTablesAction,
    Vec<Arc<TableInfo>>,
    MetaFlightAction::GetTables
);

// -get databases

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct GetDatabasesAction;

action_declare!(
    GetDatabasesAction,
    Vec<Arc<DatabaseInfo>>,
    MetaFlightAction::GetDatabases
);
