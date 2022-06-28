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
use common_meta_types::protobuf::ExportedChunk;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::GetKVReply;
use common_meta_types::GetKVReq;
use common_meta_types::ListKVReply;
use common_meta_types::ListKVReq;
use common_meta_types::MGetKVReply;
use common_meta_types::MGetKVReq;
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
    pub(crate) resp_tx: Sender<Result<Response, MetaError>>,

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
}

/// Meta-client worker-to-handle response body
#[derive(Debug, derive_more::TryInto)]
pub enum Response {
    Get(GetKVReply),
    MGet(MGetKVReply),
    PrefixList(ListKVReply),
    Upsert(UpsertKVReply),
    Txn(TxnReply),
    Watch(tonic::codec::Streaming<WatchResponse>),
    Export(tonic::codec::Streaming<ExportedChunk>),
    MakeClient(MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>),
    GetEndpoints(Vec<String>),
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
