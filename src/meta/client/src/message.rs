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

use std::fmt;
use std::fmt::Formatter;

use databend_common_base::base::tokio::sync::oneshot::Sender;
use databend_common_base::runtime::TrackingPayload;
use databend_common_meta_kvapi::kvapi::ListKVReq;
use databend_common_meta_kvapi::kvapi::MGetKVReq;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::protobuf::ClientInfo;
use databend_common_meta_types::protobuf::ClusterStatus;
use databend_common_meta_types::protobuf::ExportedChunk;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use minitrace::Span;
use tonic::codegen::BoxStream;

use crate::established_client::EstablishedClient;

/// A request that is sent by a meta-client handle to its worker.
pub struct ClientWorkerRequest {
    pub(crate) request_id: u64,

    /// For sending back the response to the handle.
    pub(crate) resp_tx: Sender<Response>,

    /// Request body
    pub(crate) req: Request,

    /// Tracing span for this request
    pub(crate) span: Span,

    pub(crate) tracking_payload: Option<TrackingPayload>,
}

impl fmt::Debug for ClientWorkerRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ClientWorkerRequest")
            .field("request_id", &self.request_id)
            .field("req", &self.req)
            .finish()
    }
}

/// Mark an RPC to return a stream.
#[derive(Debug, Clone)]
pub struct Streamed<T>(pub T);

impl<T> Streamed<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Meta-client handle-to-worker request body
#[derive(Debug, Clone, derive_more::From)]
pub enum Request {
    /// Get multiple KV, returning a stream.
    StreamMGet(Streamed<MGetKVReq>),

    /// List KVs by key prefix, returning a stream.
    StreamList(Streamed<ListKVReq>),

    /// Update or insert KV
    Upsert(UpsertKVReq),

    /// Run a transaction on remote
    Txn(TxnRequest),

    /// Watch KV changes, expecting a Stream that reports KV change events
    Watch(WatchRequest),

    /// Export all data
    Export(ExportReq),

    /// Get a initialized grpc-client
    MakeEstablishedClient(MakeEstablishedClient),

    /// Get endpoints, for test
    GetEndpoints(GetEndpoints),

    /// Get cluster status, for metactl
    GetClusterStatus(GetClusterStatus),

    /// Get info about the client
    GetClientInfo(GetClientInfo),
}

impl Request {
    pub fn name(&self) -> &'static str {
        match self {
            Request::StreamMGet(_) => "StreamMGet",
            Request::StreamList(_) => "StreamList",
            Request::Upsert(_) => "Upsert",
            Request::Txn(_) => "Txn",
            Request::Watch(_) => "Watch",
            Request::Export(_) => "Export",
            Request::MakeEstablishedClient(_) => "MakeClient",
            Request::GetEndpoints(_) => "GetEndpoints",
            Request::GetClusterStatus(_) => "GetClusterStatus",
            Request::GetClientInfo(_) => "GetClientInfo",
        }
    }
}

/// Meta-client worker-to-handle response body
#[derive(derive_more::TryInto)]
pub enum Response {
    StreamMGet(Result<BoxStream<StreamItem>, MetaError>),
    StreamList(Result<BoxStream<StreamItem>, MetaError>),
    Upsert(Result<UpsertKVReply, MetaError>),
    Txn(Result<TxnReply, MetaError>),
    Watch(Result<tonic::codec::Streaming<WatchResponse>, MetaError>),
    Export(Result<tonic::codec::Streaming<ExportedChunk>, MetaError>),
    MakeEstablishedClient(Result<EstablishedClient, MetaClientError>),
    GetEndpoints(Result<Vec<String>, MetaError>),
    GetClusterStatus(Result<ClusterStatus, MetaError>),
    GetClientInfo(Result<ClientInfo, MetaError>),
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Response::StreamMGet(x) => {
                write!(f, "StreamMGet({:?})", x.as_ref().map(|_s| "<stream>"))
            }
            Response::StreamList(x) => {
                write!(f, "StreamList({:?})", x.as_ref().map(|_s| "<stream>"))
            }
            Response::Upsert(x) => {
                write!(f, "Upsert({:?})", x)
            }
            Response::Txn(x) => {
                write!(f, "Txn({:?})", x)
            }
            Response::Watch(x) => {
                write!(f, "Watch({:?})", x)
            }
            Response::Export(x) => {
                write!(f, "Export({:?})", x)
            }
            Response::MakeEstablishedClient(x) => {
                write!(f, "MakeClient({:?})", x)
            }
            Response::GetEndpoints(x) => {
                write!(f, "GetEndpoints({:?})", x)
            }
            Response::GetClusterStatus(x) => {
                write!(f, "GetClusterStatus({:?})", x)
            }
            Response::GetClientInfo(x) => {
                write!(f, "GetClientInfo({:?})", x)
            }
        }
    }
}

impl Response {
    pub fn err(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let e = match self {
            Response::StreamMGet(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::StreamList(res) => res
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
            Response::MakeEstablishedClient(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::GetEndpoints(res) => res
                .as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static)),
            Response::GetClusterStatus(res) => res
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
pub struct ExportReq {
    /// Number of json strings contained in a export stream item.
    ///
    /// By default meta-service use 32 for this field.
    pub chunk_size: Option<u64>,
}

/// Get a grpc-client that is initialized and has passed handshake
///
/// This request is only used internally or for testing purpose.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct MakeEstablishedClient {}

/// Get all meta server endpoints
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetEndpoints {}

/// Get cluster status
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetClusterStatus {}

/// Get info about client
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetClientInfo {}
