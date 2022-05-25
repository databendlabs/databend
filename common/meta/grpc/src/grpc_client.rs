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

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use common_arrow::arrow_format::flight::data::BasicAuth;
use common_base::base::tokio::sync::mpsc;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::tokio::sync::oneshot;
use common_base::base::tokio::sync::RwLock;
use common_base::base::Runtime;
use common_base::base::TrySpawn;
use common_base::containers::ItemManager;
use common_base::containers::Pool;
use common_exception::ErrorCode;
use common_exception::Result;
use common_grpc::ConnectionFactory;
use common_grpc::GrpcConnectionError;
use common_grpc::RpcClientConf;
use common_grpc::RpcClientTlsConfig;
use common_meta_api::KVApi;
use common_meta_types::anyerror::AnyError;
use common_meta_types::protobuf::meta_service_client::MetaServiceClient;
use common_meta_types::protobuf::Empty;
use common_meta_types::protobuf::ExportedChunk;
use common_meta_types::protobuf::HandshakeRequest;
use common_meta_types::protobuf::MemberListRequest;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::ConnectionError;
use common_meta_types::MetaError;
use common_meta_types::MetaNetworkError;
use common_meta_types::MetaResultError;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_tracing::tracing;
use futures::stream::StreamExt;
use prost::Message;
use serde::de::DeserializeOwned;
use tonic::async_trait;
use tonic::client::GrpcService;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::Code;
use tonic::Request;
use tonic::Status;

use crate::grpc_action::MetaGrpcReadReq;
use crate::grpc_action::MetaGrpcWriteReq;
use crate::grpc_action::RequestFor;
use crate::message;

const AUTH_TOKEN_KEY: &str = "auth-token-bin";

#[derive(Debug)]
struct MetaChannelManager {
    timeout: Option<Duration>,
    conf: Option<RpcClientTlsConfig>,
}

impl MetaChannelManager {
    fn build_endpoint(&self, addr: &String) -> std::result::Result<Endpoint, MetaError> {
        let ch = ConnectionFactory::create_rpc_endpoint(addr, self.timeout, self.conf.clone())
            .map_err(|e| match e {
                GrpcConnectionError::InvalidUri { .. } => MetaNetworkError::BadAddressFormat(
                    AnyError::new(&e).add_context(|| "while creating rpc channel"),
                ),
                GrpcConnectionError::TLSConfigError { .. } => MetaNetworkError::TLSConfigError(
                    AnyError::new(&e).add_context(|| "while creating rpc channel"),
                ),
                GrpcConnectionError::CannotConnect { .. } => MetaNetworkError::ConnectionError(
                    ConnectionError::new(e, "while creating rpc channel"),
                ),
            })?;
        Ok(ch)
    }
}

#[async_trait]
impl ItemManager for MetaChannelManager {
    type Key = Vec<String>;
    type Item = Channel;
    type Error = MetaError;

    async fn build(&self, endpoints: &Self::Key) -> std::result::Result<Self::Item, Self::Error> {
        let channel_eps: std::result::Result<Vec<Endpoint>, MetaError> = (*endpoints)
            .iter()
            .map(|a| self.build_endpoint(a))
            .collect();
        let channel_eps = channel_eps?;
        let ch = Channel::balance_list(channel_eps.into_iter());
        Ok(ch)
    }

    async fn check(&self, mut ch: Self::Item) -> std::result::Result<Self::Item, Self::Error> {
        futures::future::poll_fn(|cx| ch.poll_ready(cx))
            .await
            .map_err(|e| {
                MetaNetworkError::ConnectionError(ConnectionError::new(e, "while check item"))
            })?;
        Ok(ch)
    }
}

pub struct MetaGrpcClient {
    conn_pool: Pool<MetaChannelManager>,
    endpoints: RwLock<Vec<String>>,
    username: String,
    password: String,
    token: RwLock<Option<Vec<u8>>>,

    /// Dedicated runtime to support meta client background tasks.
    ///
    /// In order not to let a blocking operation(such as calling the new PipelinePullingExecutor) in a tokio runtime block meta-client background tasks.
    /// If a background task is blocked, no meta-client will be able to proceed if meta-client is reused.
    ///
    /// Note that a thread_pool tokio runtime does not help: a scheduled tokio-task resides in `filo_slot` won't be stolen by other tokio-workers.
    /// TODO: dead code
    #[allow(dead_code)]
    rt: Arc<Runtime>,
}

/// A handle to access meta-client worker.
/// The worker will be actually running in a dedicated runtime: `MetaGrpcClient.rt`.
pub struct ClientHandle {
    /// For sending request to meta-client worker.
    pub(crate) req_tx: Sender<message::ClientWorkerRequest>,
}

impl ClientHandle {
    /// Send a request to the internal worker task, which may be running in another runtime.
    pub async fn request<Req, Resp>(&self, req: Req) -> std::result::Result<Resp, MetaError>
    where
        Req: RequestFor<Reply = Resp>,
        Req: Into<message::Request>,
        Resp: TryFrom<message::Response>,
        <Resp as TryFrom<message::Response>>::Error: std::fmt::Display,
    {
        let (tx, rx) = oneshot::channel();
        let req = message::ClientWorkerRequest {
            resp_tx: tx,
            req: req.into(),
        };

        self.req_tx.send(req).await.map_err(|e| {
            MetaError::Fatal(
                AnyError::new(&e).add_context(|| "when sending req to MetaGrpcClient worker"),
            )
        })?;

        let res = rx.await.map_err(|e| {
            MetaError::Fatal(
                AnyError::new(&e).add_context(|| "when recv resp from MetaGrpcClient worker"),
            )
        })?;

        let resp = res?;

        let r = Resp::try_from(resp).map_err(|e| {
            MetaError::MetaResultError(MetaResultError::InvalidType {
                expect: std::any::type_name::<Resp>().to_string(),
                got: e.to_string(),
            })
        })?;

        Ok(r)
    }

    pub async fn make_client(
        &self,
    ) -> std::result::Result<
        MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>,
        MetaError,
    > {
        self.request(message::MakeClient {}).await
    }
}

impl MetaGrpcClient {
    /// Create a new client of metasrv.
    ///
    /// It creates a new `Runtime` and spawn a background worker task in it that do all the RPC job.
    /// A client-handle is returned to communicate with the worker.
    ///
    /// Thus the real work is done in the dedicated runtime to avoid the client spawning tasks in the caller's runtime, which potentially leads to a deadlock if the caller has blocking calls to other components
    /// Because `tower` and `hyper` will spawn tasks when handling RPCs.
    ///
    /// The worker is a singleton and the returned handle is cheap to clone.
    /// When all handles are dropped the worker will quit, then the runtime will be destroyed.
    pub async fn try_new(
        conf: &RpcClientConf,
    ) -> std::result::Result<Arc<ClientHandle>, ErrorCode> {
        Self::try_create(
            conf.get_endpoints(),
            &conf.username,
            &conf.password,
            conf.timeout,
            conf.tls_conf.clone(),
        )
        .await
    }

    #[tracing::instrument(level = "debug", skip(password))]
    pub async fn try_create(
        endpoints: Vec<String>,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        conf: Option<RpcClientTlsConfig>,
    ) -> Result<Arc<ClientHandle>> {
        let mgr = MetaChannelManager {
            timeout,
            conf: conf.clone(),
        };

        let rt = Runtime::with_worker_threads(1, Some("meta-client-rt".to_string()))
            .map_err(|e| e.add_message_back("when creating meta-client"))?;
        let rt = Arc::new(rt);

        // Build the handle-worker pair

        let (tx, rx) = mpsc::channel(256);

        let handle = Arc::new(ClientHandle { req_tx: tx });

        let worker = Arc::new(Self {
            conn_pool: Pool::new(mgr, Duration::from_millis(50)),
            endpoints: RwLock::new(vec![]),
            username: username.to_string(),
            password: password.to_string(),
            token: RwLock::new(None),
            rt: rt.clone(),
        });
        worker.set_endpoints(endpoints).await?;

        rt.spawn(Self::worker_loop(worker, rx));

        Ok(handle)
    }

    /// A worker runs a receiving-loop to accept user-request to metasrv and deals with request in the dedicated runtime.
    #[tracing::instrument(level = "info", skip_all)]
    async fn worker_loop(self: Arc<Self>, mut req_rx: Receiver<message::ClientWorkerRequest>) {
        tracing::info!("MetaGrpcClient::worker spawned");

        loop {
            let t = req_rx.recv().await;
            let req = match t {
                None => {
                    tracing::info!("MetaGrpcClient handle closed. worker quit");
                    return;
                }
                Some(x) => x,
            };

            tracing::debug!(req = debug(&req), "MetaGrpcClient recv request");

            if req.resp_tx.is_closed() {
                tracing::debug!(
                    req = debug(&req),
                    "MetaGrpcClient request.resp_tx is closed, cancel handling this request"
                );
                continue;
            }

            let resp_tx = req.resp_tx;
            let req = req.req;

            let resp = match req {
                message::Request::Get(r) => {
                    let resp = self.do_read(r).await;
                    resp.map(message::Response::Get)
                }
                message::Request::MGet(r) => {
                    let resp = self.do_read(r).await;
                    resp.map(message::Response::MGet)
                }
                message::Request::PrefixList(r) => {
                    let resp = self.do_read(r).await;
                    resp.map(message::Response::PrefixList)
                }
                message::Request::Upsert(r) => {
                    let resp = self.do_write(r).await;
                    resp.map(message::Response::Upsert)
                }
                message::Request::Txn(r) => {
                    let resp = self.transaction(r).await;
                    resp.map(message::Response::Txn)
                }
                message::Request::Watch(r) => {
                    let resp = self.watch(r).await;
                    resp.map(message::Response::Watch)
                }
                message::Request::Export(r) => {
                    let resp = self.export(r).await;
                    resp.map(message::Response::Export)
                }
                message::Request::MakeClient(_) => {
                    let resp = self.make_client().await;
                    resp.map(message::Response::MakeClient)
                }
            };

            tracing::debug!(
                resp = debug(&resp),
                "MetaGrpcClient send response to the handle"
            );

            let res = resp_tx.send(resp);
            if let Err(err) = res {
                tracing::warn!(
                    err = debug(err),
                    "MetaGrpcClient failed to send response to the handle. recv-end closed"
                );
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn make_client(
        &self,
    ) -> std::result::Result<
        MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>,
        MetaError,
    > {
        let channel = {
            let eps = self.endpoints.read().await;
            debug_assert!(!eps.is_empty());

            self.conn_pool.get(&*eps).await?
        };

        let mut client = MetaServiceClient::new(channel.clone());

        let mut t = self.token.write().await;
        let token = match t.clone() {
            Some(t) => t,
            None => {
                let new_token =
                    MetaGrpcClient::handshake(&mut client, &self.username, &self.password).await?;
                *t = Some(new_token.clone());
                new_token
            }
        };

        let client = MetaServiceClient::with_interceptor(channel, AuthInterceptor { token });

        Ok(client)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn set_endpoints(
        &self,
        endpoints: Vec<String>,
    ) -> std::result::Result<(), MetaError> {
        if endpoints.is_empty() {
            return Err(MetaError::InvalidConfig("endpoints is empty".to_string()));
        }

        let mut eps = self.endpoints.write().await;
        *eps = endpoints;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn sync_endpoints(&self) -> Result<()> {
        let endpoints = {
            let eps = self.endpoints.read().await;
            (*eps).clone()
        };
        let channel = self.conn_pool.get(&endpoints).await?;

        let mut client = MetaServiceClient::new(channel.clone());
        let endpoints = client
            .member_list(Request::new(MemberListRequest {
                data: "".to_string(),
            }))
            .await?;
        let result: Vec<String> = endpoints.into_inner().data;
        self.set_endpoints(result).await?;
        Ok(())
    }

    /// Handshake with metasrv.
    #[tracing::instrument(level = "debug", skip(client, password))]
    async fn handshake(
        client: &mut MetaServiceClient<Channel>,
        username: &str,
        password: &str,
    ) -> std::result::Result<Vec<u8>, MetaError> {
        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        let mut payload = vec![];
        auth.encode(&mut payload)?;

        let req = Request::new(futures::stream::once(async {
            HandshakeRequest {
                protocol_version: 0,
                payload,
            }
        }));

        let rx = client.handshake(req).await?;
        let mut rx = rx.into_inner();

        let resp = rx.next().await.expect("Must respond from handshake")?;
        let token = resp.payload;
        Ok(token)
    }

    /// Create a watching stream that receives KV change events.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn watch(
        &self,
        watch_request: WatchRequest,
    ) -> std::result::Result<tonic::codec::Streaming<WatchResponse>, MetaError> {
        tracing::debug!(
            watch_request = debug(&watch_request),
            "MetaGrpcClient worker: handle watch request"
        );

        let mut client = self.make_client().await?;
        let res = client.watch(watch_request).await?;
        Ok(res.into_inner())
    }

    /// Export all data in json from metasrv.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn export(
        &self,
        export_request: message::ExportReq,
    ) -> std::result::Result<tonic::codec::Streaming<ExportedChunk>, MetaError> {
        tracing::debug!(
            export_request = debug(&export_request),
            "MetaGrpcClient worker: handle export request"
        );

        let mut client = self.make_client().await?;
        let res = client.export(Empty {}).await?;
        Ok(res.into_inner())
    }

    #[tracing::instrument(level = "debug", skip(self, v))]
    pub(crate) async fn do_write<T, R>(&self, v: T) -> std::result::Result<R, MetaError>
    where
        T: RequestFor<Reply = R> + Into<MetaGrpcWriteReq>,
        R: DeserializeOwned,
    {
        let act: MetaGrpcWriteReq = v.into();

        tracing::debug!(req = debug(&act), "MetaGrpcClient::do_write request");

        let req: Request<RaftRequest> = act.clone().try_into()?;

        tracing::debug!(
            req = debug(&req),
            "MetaGrpcClient::do_write serialized request"
        );

        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut client = self.make_client().await?;
        let result = client.write_msg(req).await;
        let result: std::result::Result<RaftReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    {
                        let mut token = self.token.write().await;
                        *token = None;
                    }
                    let mut client = self.make_client().await?;
                    let req: Request<RaftRequest> = act.try_into()?;
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    Ok(client.write_msg(req).await?.into_inner())
                } else {
                    Err(s)
                }
            }
        };

        let raft_reply = result?;

        let res: std::result::Result<R, MetaError> = raft_reply.into();

        res
    }

    #[tracing::instrument(level = "debug", skip(self, v))]
    pub(crate) async fn do_read<T, R>(&self, v: T) -> std::result::Result<R, MetaError>
    where
        T: RequestFor<Reply = R>,
        T: Into<MetaGrpcReadReq>,
        R: DeserializeOwned,
    {
        let act: MetaGrpcReadReq = v.into();

        tracing::debug!(req = debug(&act), "MetaGrpcClient::do_read request");

        let req: Request<RaftRequest> = act.clone().try_into()?;

        tracing::debug!(
            req = debug(&req),
            "MetaGrpcClient::do_read serialized request"
        );

        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut client = self.make_client().await?;
        let result = client.read_msg(req).await;

        tracing::debug!(reply = debug(&result), "MetaGrpcClient::do_read reply");

        let rpc_res: std::result::Result<RaftReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    {
                        let mut token = self.token.write().await;
                        *token = None;
                    }
                    let mut client = self.make_client().await?;
                    let req: Request<RaftRequest> = act.try_into()?;
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    Ok(client.read_msg(req).await?.into_inner())
                } else {
                    Err(s)
                }
            }
        };
        let raft_reply = rpc_res?;

        let res: std::result::Result<R, MetaError> = raft_reply.into();
        res
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    pub(crate) async fn transaction(
        &self,
        req: TxnRequest,
    ) -> std::result::Result<TxnReply, MetaError> {
        let txn: TxnRequest = req;

        tracing::debug!(req = display(&txn), "MetaGrpcClient::transaction request");

        let req: Request<TxnRequest> = Request::new(txn.clone());
        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut client = self.make_client().await?;
        let result = client.transaction(req).await;

        let result: std::result::Result<TxnReply, Status> = match result {
            Ok(r) => return Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    {
                        let mut token = self.token.write().await;
                        *token = None;
                    }
                    let mut client = self.make_client().await?;
                    let req: Request<TxnRequest> = Request::new(txn);
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    let ret = client.transaction(req).await?.into_inner();
                    return Ok(ret);
                } else {
                    Err(s)
                }
            }
        };

        let reply = result?;

        tracing::debug!(reply = display(&reply), "MetaGrpcClient::transaction reply");

        Ok(reply)
    }
}

fn status_is_retryable(status: &Status) -> bool {
    matches!(status.code(), Code::Unauthenticated | Code::Internal)
}

#[derive(Clone)]
pub struct AuthInterceptor {
    pub token: Vec<u8>,
}

impl Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        let metadata = req.metadata_mut();
        metadata.insert_bin(AUTH_TOKEN_KEY, MetadataValue::from_bytes(&self.token));
        Ok(req)
    }
}
