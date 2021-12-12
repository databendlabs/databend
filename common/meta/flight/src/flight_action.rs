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
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use common_arrow::arrow_format::flight::data::Action;
use common_exception::ErrorCode;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateTableReply;
use common_meta_types::CreateTableReq;
use common_meta_types::DatabaseInfo;
use common_meta_types::DropDatabaseReply;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetDatabaseReq;
use common_meta_types::GetKVActionReply;
use common_meta_types::GetTableReq;
use common_meta_types::ListDatabaseReq;
use common_meta_types::ListTableReq;
use common_meta_types::MGetKVActionReply;
use common_meta_types::MetaId;
use common_meta_types::PrefixListReply;
use common_meta_types::TableInfo;
use common_meta_types::UpsertKVAction;
use common_meta_types::UpsertKVActionReply;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use prost::Message;
use tonic::Request;

use crate::protobuf::FlightMetaRequest;

pub trait RequestFor {
    type Reply;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct FlightReq<T> {
    pub req: T,
}

// Action wrapper for do_action.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, derive_more::From)]
pub enum MetaFlightAction {
    CreateDatabase(CreateDatabaseReq),
    DropDatabase(FlightReq<DropDatabaseReq>),
    GetDatabase(FlightReq<GetDatabaseReq>),
    ListDatabases(FlightReq<ListDatabaseReq>),

    CreateTable(FlightReq<CreateTableReq>),
    DropTable(FlightReq<DropTableReq>),
    GetTable(FlightReq<GetTableReq>),
    GetTableExt(GetTableExtReq),
    ListTables(FlightReq<ListTableReq>),
    CommitTable(FlightReq<UpsertTableOptionReq>),

    UpsertKV(UpsertKVAction),
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

// - MGetKV

// Again, impl chooses to wrap it up
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct MGetKVAction {
    pub keys: Vec<String>,
}

// here we use a macro to simplify the declarations
impl RequestFor for MGetKVAction {
    type Reply = MGetKVActionReply;
}

// - prefix list
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct PrefixListReq(pub String);
impl RequestFor for PrefixListReq {
    type Reply = PrefixListReply;
}

impl RequestFor for UpsertKVAction {
    type Reply = UpsertKVActionReply;
}

// == database actions ==

impl RequestFor for CreateDatabaseReq {
    type Reply = CreateDatabaseReply;
}

impl RequestFor for FlightReq<GetDatabaseReq> {
    type Reply = Arc<DatabaseInfo>;
}

impl RequestFor for FlightReq<DropDatabaseReq> {
    type Reply = DropDatabaseReply;
}

impl RequestFor for FlightReq<CreateTableReq> {
    type Reply = CreateTableReply;
}

impl RequestFor for FlightReq<DropTableReq> {
    type Reply = DropTableReply;
}

impl RequestFor for FlightReq<GetTableReq> {
    type Reply = Arc<TableInfo>;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableExtReq {
    pub tbl_id: MetaId,
}
impl RequestFor for GetTableExtReq {
    type Reply = TableInfo;
}

impl RequestFor for FlightReq<UpsertTableOptionReq> {
    type Reply = UpsertTableOptionReply;
}

impl RequestFor for FlightReq<ListTableReq> {
    type Reply = Vec<Arc<TableInfo>>;
}

impl RequestFor for FlightReq<ListDatabaseReq> {
    type Reply = Vec<Arc<DatabaseInfo>>;
}
