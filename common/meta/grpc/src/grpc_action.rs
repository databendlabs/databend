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
use std::sync::Arc;

use common_meta_types::protobuf::RaftRequest;
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
use tonic::Request;

pub trait RequestFor {
    type Reply;
}

// Action wrapper for do_action.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, derive_more::From)]
pub enum MetaGrpcWriteReq {
    CreateDatabase(CreateDatabaseReq),
    DropDatabase(DropDatabaseReq),

    CreateTable(CreateTableReq),
    DropTable(DropTableReq),
    CommitTable(UpsertTableOptionReq),

    UpsertKV(UpsertKVAction),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, derive_more::From)]
pub enum MetaGrpcReadReq {
    GetDatabase(GetDatabaseReq),
    ListDatabases(ListDatabaseReq),

    GetTable(GetTableReq),
    GetTableExt(GetTableExtReq),
    ListTables(ListTableReq),

    GetKV(GetKVAction),
    MGetKV(MGetKVAction),
    PrefixListKV(PrefixListReq),
}

/// Try convert tonic::Request<RaftRequest> to DoActionAction.
impl TryInto<MetaGrpcWriteReq> for Request<RaftRequest> {
    type Error = tonic::Status;

    fn try_into(self) -> Result<MetaGrpcWriteReq, Self::Error> {
        let raft_request = self.into_inner();

        // Decode DoActionAction from flight request body.
        let json_str = raft_request.data.as_str();
        let action = serde_json::from_str::<MetaGrpcWriteReq>(json_str)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(action)
    }
}

impl tonic::IntoRequest<RaftRequest> for MetaGrpcWriteReq {
    fn into_request(self) -> Request<RaftRequest> {
        let raft_request = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(raft_request)
    }
}

/// Try convert DoActionAction to tonic::Request<RaftRequest>.
impl TryInto<Request<RaftRequest>> for MetaGrpcWriteReq {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<Request<RaftRequest>, Self::Error> {
        let raft_request = RaftRequest {
            data: serde_json::to_string(&self)?,
        };

        let request = tonic::Request::new(raft_request);
        Ok(request)
    }
}

impl TryInto<MetaGrpcReadReq> for Request<RaftRequest> {
    type Error = tonic::Status;

    fn try_into(self) -> Result<MetaGrpcReadReq, Self::Error> {
        let raft_req = self.into_inner();

        let json_str = raft_req.data.as_str();
        let action = serde_json::from_str::<MetaGrpcReadReq>(json_str)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(action)
    }
}

impl TryInto<Request<RaftRequest>> for MetaGrpcReadReq {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<Request<RaftRequest>, Self::Error> {
        let get_req = RaftRequest {
            data: serde_json::to_string(&self)?,
        };

        let request = tonic::Request::new(get_req);
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

impl RequestFor for GetDatabaseReq {
    type Reply = Arc<DatabaseInfo>;
}

impl RequestFor for DropDatabaseReq {
    type Reply = DropDatabaseReply;
}

impl RequestFor for CreateTableReq {
    type Reply = CreateTableReply;
}

impl RequestFor for DropTableReq {
    type Reply = DropTableReply;
}

impl RequestFor for GetTableReq {
    type Reply = Arc<TableInfo>;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct GetTableExtReq {
    pub tbl_id: MetaId,
}
impl RequestFor for GetTableExtReq {
    type Reply = TableInfo;
}

impl RequestFor for UpsertTableOptionReq {
    type Reply = UpsertTableOptionReply;
}

impl RequestFor for ListTableReq {
    type Reply = Vec<Arc<TableInfo>>;
}

impl RequestFor for ListDatabaseReq {
    type Reply = Vec<Arc<DatabaseInfo>>;
}
