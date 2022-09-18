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

use common_base::base::tokio::sync::oneshot::Sender;
use common_meta_types::protobuf::meta_service_client::MetaServiceClient;
use common_meta_types::protobuf::ClientInfo;
use common_meta_types::protobuf::ExportedChunk;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::GetKVReply;
use common_meta_types::GetKVReq;
use common_meta_types::KVAppError;
use common_meta_types::ListKVReply;
use common_meta_types::ListKVReq;
use common_meta_types::MGetKVReply;
use common_meta_types::MGetKVReq;
use common_meta_types::MetaClientError;
use common_meta_types::MetaError;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKVReply;
use common_meta_types::UpsertKVReq;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;

use crate::grpc_client::AuthInterceptor;

/// A request that is sent by a meta-client handle to its worker.
#[derive(Debug)]
pub struct ClientWorkerRequest {
    /// For sending back the response to the handle.
    pub(crate) resp_tx: Sender<Response>,

    /// Request body
    pub(crate) req: Request,
}

/// Meta-client handle-to-worker request body
#[derive(Debug, Clone, derive_more::From)]
pub enum Request {
    /// Get KV
    Get(GetKVReq),

    /// Get multiple KV
    MGet(MGetKVReq),

    /// List KVs by key prefix
    PrefixList(ListKVReq),

    /// Update or insert KV
    Upsert(UpsertKVReq),

    /// Run a transaction on remote
    Txn(TxnRequest),

    /// Watch KV changes, expecting a Stream that reports KV chnage events
    Watch(WatchRequest),

    /// Export all data
    Export(ExportReq),

    /// Get a initialized grpc-client
    MakeClient(MakeClient),

    /// Get endpoints, for test
    GetEndpoints(GetEndpoints),

    /// Get info about the client
    GetClientInfo(GetClientInfo),
}

/// Meta-client worker-to-handle response body
#[derive(Debug, derive_more::TryInto)]
pub enum Response {
    Get(Result<GetKVReply, KVAppError>),
    MGet(Result<MGetKVReply, KVAppError>),
    PrefixList(Result<ListKVReply, KVAppError>),
    Upsert(Result<UpsertKVReply, KVAppError>),
    Txn(Result<TxnReply, KVAppError>),
    Watch(Result<tonic::codec::Streaming<WatchResponse>, MetaError>),
    Export(Result<tonic::codec::Streaming<ExportedChunk>, MetaError>),
    MakeClient(
        Result<MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>, MetaClientError>,
    ),
    GetEndpoints(Result<Vec<String>, MetaError>),
    GetClientInfo(Result<ClientInfo, MetaError>),
}

impl Response {
    pub fn is_err(&self) -> bool {
        match self {
            Response::Get(res) => res.is_err(),
            Response::MGet(res) => res.is_err(),
            Response::PrefixList(res) => res.is_err(),
            Response::Upsert(res) => res.is_err(),
            Response::Txn(res) => res.is_err(),
            Response::Watch(res) => res.is_err(),
            Response::Export(res) => res.is_err(),
            Response::MakeClient(res) => res.is_err(),
            Response::GetEndpoints(res) => res.is_err(),
            Response::GetClientInfo(res) => res.is_err(),
        }
    }

    pub fn err(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let e = match self {
            Response::Get(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::MGet(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::PrefixList(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::Upsert(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::Txn(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::Watch(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::Export(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::MakeClient(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::GetEndpoints(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::GetClientInfo(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
        };
        e
    }
}

/// Export all data stored in metasrv
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ExportReq {}

/// Get a grpc-client that is initialized and has passed handshake
///
/// This request is only used internally or for testing purpose.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct MakeClient {}

/// Get all meta server endpoints
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetEndpoints {}

/// Get info about client
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetClientInfo {}
