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

use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::protobuf::ClientInfo;
use databend_common_meta_types::protobuf::ClusterStatus;
use databend_common_meta_types::protobuf::ExportedChunk;
use databend_common_meta_types::protobuf::MemberListReply;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use fastrace::Span;
use tokio::sync::oneshot::Sender;
use tonic::codegen::BoxStream;

use crate::established_client::EstablishedClient;
use crate::grpc_action::ListKVReq;
use crate::grpc_action::MGetKVReq;

/// A request that is sent by a meta-client handle to its worker.
pub struct ClientWorkerRequest {
    pub(crate) request_id: u64,

    /// For sending back the response to the handle.
    pub(crate) resp_tx: Sender<Response>,

    /// Request body
    pub(crate) req: Request,

    /// Tracing span for this request
    pub(crate) span: Span,

    pub(crate) tracking_fn: Option<Box<dyn FnOnce() -> Box<dyn std::any::Any + Send> + Send>>,
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

/// A marker type to indicate that the watch response should return a flag event indicating the end of initialization.
#[derive(Debug, Clone)]
pub struct InitFlag;

/// Meta-client handle-to-worker request body
#[derive(Debug, Clone, derive_more::From)]
pub enum Request {
    /// Get multiple KV, returning a stream.
    // TODO: Add a new variant to support real stream input + stream output get-many
    StreamMGet(Streamed<MGetKVReq>),

    /// List KVs by key prefix, returning a stream.
    StreamList(Streamed<ListKVReq>),

    /// Run a transaction on remote
    Txn(TxnRequest),

    /// Watch KV changes, expecting a Stream that reports KV change events
    Watch(WatchRequest),

    /// Watch KV changes with optional initialization flush of all existing values.
    ///
    /// When `initial_flush` is set to true in the WatchRequest:
    /// - First sends all existing key-values with `is_initialization=true`
    /// - Then sends a special event with no data to mark the end of initialization
    /// - Finally streams real-time changes as they occur
    ///
    /// This provides a reliable way to initialize client-side caches and then
    /// keep them synchronized with server state.
    WatchWithInitialization((WatchRequest, InitFlag)),

    /// Export all data
    Export(ExportReq),

    /// Get an initialized grpc-client
    MakeEstablishedClient(MakeEstablishedClient),

    /// Get endpoints, for test
    GetEndpoints(GetEndpoints),

    /// Get cluster status, for metactl
    GetClusterStatus(GetClusterStatus),

    /// Get member list, for metactl
    GetMemberList(GetMemberList),

    /// Get info about the client
    GetClientInfo(GetClientInfo),
}

impl Request {
    pub fn name(&self) -> &'static str {
        match self {
            Request::StreamMGet(_) => "StreamMGet",
            Request::StreamList(_) => "StreamList",
            Request::Txn(_) => "Txn",
            Request::Watch(_) => "Watch",
            Request::WatchWithInitialization(_) => "WatchWithInitialization",
            Request::Export(_) => "Export",
            Request::MakeEstablishedClient(_) => "MakeClient",
            Request::GetEndpoints(_) => "GetEndpoints",
            Request::GetClusterStatus(_) => "GetClusterStatus",
            Request::GetMemberList(_) => "GetMemberList",
            Request::GetClientInfo(_) => "GetClientInfo",
        }
    }
}

/// Meta-client worker-to-handle response body
#[derive(derive_more::TryInto)]
pub enum Response {
    StreamMGet(Result<BoxStream<StreamItem>, MetaError>),
    StreamList(Result<BoxStream<StreamItem>, MetaError>),
    Txn(Result<TxnReply, MetaClientError>),
    Watch(Result<tonic::codec::Streaming<WatchResponse>, MetaClientError>),
    WatchWithInitialization(Result<tonic::codec::Streaming<WatchResponse>, MetaClientError>),
    Export(Result<tonic::codec::Streaming<ExportedChunk>, MetaClientError>),
    MakeEstablishedClient(Result<EstablishedClient, MetaClientError>),
    GetEndpoints(Result<Vec<String>, MetaError>),
    GetClusterStatus(Result<ClusterStatus, MetaClientError>),
    GetMemberList(Result<MemberListReply, MetaClientError>),
    GetClientInfo(Result<ClientInfo, MetaClientError>),
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
            Response::Txn(x) => {
                write!(f, "Txn({:?})", x)
            }
            Response::Watch(x) => {
                write!(f, "Watch({:?})", x)
            }
            Response::WatchWithInitialization(x) => {
                write!(f, "WatchWithInitialization({:?})", x)
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
            Response::GetMemberList(x) => {
                write!(f, "GetMemberList({:?})", x)
            }
            Response::GetClientInfo(x) => {
                write!(f, "GetClientInfo({:?})", x)
            }
        }
    }
}

impl Response {
    pub fn err(&self) -> Option<&(dyn std::error::Error + 'static)> {
        /// Extract the error from the Result in each variant.
        fn to_err<T, E>(res: &Result<T, E>) -> Option<&(dyn std::error::Error + 'static)>
        where E: std::error::Error + 'static {
            res.as_ref()
                .err()
                .map(|x| x as &(dyn std::error::Error + 'static))
        }

        match self {
            Response::StreamMGet(res) => to_err(res),
            Response::StreamList(res) => to_err(res),
            Response::Txn(res) => to_err(res),
            Response::Watch(res) => to_err(res),
            Response::WatchWithInitialization(res) => to_err(res),
            Response::Export(res) => to_err(res),
            Response::MakeEstablishedClient(res) => to_err(res),
            Response::GetEndpoints(res) => to_err(res),
            Response::GetClusterStatus(res) => to_err(res),
            Response::GetMemberList(res) => to_err(res),
            Response::GetClientInfo(res) => to_err(res),
        }
    }
}

/// Export all data stored in metasrv
#[derive(Clone, Debug)]
pub struct ExportReq {
    /// Number of json strings contained in a export stream item.
    ///
    /// By default, meta-service use 32 for this field.
    pub chunk_size: Option<u64>,
}

/// Get a grpc-client that is initialized and has passed handshake
///
/// This request is only used internally or for testing purpose.
#[derive(Clone, Debug)]
pub struct MakeEstablishedClient {}

/// Get all meta server endpoints
#[derive(Clone, Debug)]
pub struct GetEndpoints {}

/// Get cluster status
#[derive(Clone, Debug)]
pub struct GetClusterStatus {}

/// Get member list
#[derive(Clone, Debug)]
pub struct GetMemberList {}

/// Get info about client
#[derive(Clone, Debug)]
pub struct GetClientInfo {}
