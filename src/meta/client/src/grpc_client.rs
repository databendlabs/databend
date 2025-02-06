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
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use arrow_flight::BasicAuth;
use databend_common_base::base::tokio::select;
use databend_common_base::base::tokio::sync::mpsc;
use databend_common_base::base::tokio::sync::mpsc::UnboundedReceiver;
use databend_common_base::base::tokio::sync::mpsc::UnboundedSender;
use databend_common_base::base::tokio::sync::oneshot;
use databend_common_base::base::tokio::sync::oneshot::Sender as OneSend;
use databend_common_base::base::tokio::time::sleep;
use databend_common_base::containers::ItemManager;
use databend_common_base::containers::Pool;
use databend_common_base::future::TimedFutureExt;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::runtime::UnlimitedFuture;
use databend_common_grpc::ConnectionFactory;
use databend_common_grpc::GrpcConnectionError;
use databend_common_grpc::RpcClientConf;
use databend_common_grpc::RpcClientTlsConfig;
use databend_common_meta_api::reply::reply_to_api_result;
use databend_common_meta_types::anyerror::AnyError;
use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::protobuf::meta_service_client::MetaServiceClient;
use databend_common_meta_types::protobuf::ClientInfo;
use databend_common_meta_types::protobuf::ClusterStatus;
use databend_common_meta_types::protobuf::Empty;
use databend_common_meta_types::protobuf::ExportedChunk;
use databend_common_meta_types::protobuf::HandshakeRequest;
use databend_common_meta_types::protobuf::MemberListReply;
use databend_common_meta_types::protobuf::MemberListRequest;
use databend_common_meta_types::protobuf::RaftRequest;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::ConnectionError;
use databend_common_meta_types::GrpcConfig;
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaHandshakeError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_metrics::count::Count;
use fastrace::func_name;
use fastrace::func_path;
use fastrace::future::FutureExt as MTFutureExt;
use fastrace::Span;
use futures::stream::StreamExt;
use futures::FutureExt;
use log::debug;
use log::error;
use log::info;
use log::warn;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use prost::Message;
use semver::Version;
use serde::de::DeserializeOwned;
use tonic::async_trait;
use tonic::codegen::BoxStream;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::Code;
use tonic::Request;
use tonic::Status;

use crate::endpoints::Endpoints;
use crate::established_client::EstablishedClient;
use crate::from_digit_ver;
use crate::grpc_action::RequestFor;
use crate::grpc_metrics;
use crate::message;
use crate::message::Response;
use crate::to_digit_ver;
use crate::ClientWorkerRequest;
use crate::MetaGrpcReadReq;
use crate::MetaGrpcReq;
use crate::METACLI_COMMIT_SEMVER;
use crate::MIN_METASRV_SEMVER;

const RPC_RETRIES: usize = 4;
const AUTH_TOKEN_KEY: &str = "auth-token-bin";

pub(crate) type RealClient = MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>;

#[derive(Debug)]
pub struct MetaChannelManager {
    username: String,
    password: String,
    timeout: Option<Duration>,
    tls_config: Option<RpcClientTlsConfig>,

    /// The endpoints of the meta-service cluster.
    ///
    /// The endpoints will be added to a built client item
    /// and will be updated when a error or successful response is received.
    endpoints: Arc<Mutex<Endpoints>>,
}

impl MetaChannelManager {
    pub fn new(
        username: impl ToString,
        password: impl ToString,
        timeout: Option<Duration>,
        tls_config: Option<RpcClientTlsConfig>,
        endpoints: Arc<Mutex<Endpoints>>,
    ) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            timeout,
            tls_config,
            endpoints,
        }
    }

    #[async_backtrace::framed]
    async fn new_established_client(
        &self,
        addr: &String,
    ) -> Result<EstablishedClient, MetaClientError> {
        let chan = self.build_channel(addr).await?;

        let (mut real_client, once) = Self::new_real_client(chan);

        info!(
            "MetaChannelManager done building RealClient to {}, start handshake",
            addr
        );

        let handshake_res = MetaGrpcClient::handshake(
            &mut real_client,
            &METACLI_COMMIT_SEMVER,
            &MIN_METASRV_SEMVER,
            &self.username,
            &self.password,
        )
        .await;

        info!(
            "MetaChannelManager done handshake to {}, result.err(): {:?}",
            addr,
            handshake_res.as_ref().err()
        );

        let (token, server_version) = handshake_res?;

        // Update the token for the client interceptor.
        // Safe unwrap(): it is the first time setting it.
        once.set(token).unwrap();

        Ok(EstablishedClient::new(
            real_client,
            server_version,
            addr,
            self.endpoints.clone(),
        ))
    }

    /// Create a MetaServiceClient with authentication interceptor
    ///
    /// The returned `OnceCell` is used to fill in a token for the interceptor.
    pub fn new_real_client(chan: Channel) -> (RealClient, Arc<OnceCell<Vec<u8>>>) {
        let once = Arc::new(OnceCell::new());

        let interceptor = AuthInterceptor {
            token: once.clone(),
        };

        let client = MetaServiceClient::with_interceptor(chan, interceptor)
            .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
            .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

        (client, once)
    }

    #[async_backtrace::framed]
    async fn build_channel(&self, addr: &String) -> Result<Channel, MetaNetworkError> {
        info!("MetaChannelManager::build_channel to {}", addr);

        let ch = ConnectionFactory::create_rpc_channel(addr, self.timeout, self.tls_config.clone())
            .await
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
    type Key = String;
    type Item = EstablishedClient;
    type Error = MetaClientError;

    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn build(&self, addr: &Self::Key) -> Result<Self::Item, Self::Error> {
        self.new_established_client(addr).await
    }

    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn check(&self, ch: Self::Item) -> Result<Self::Item, Self::Error> {
        // The underlying `tonic::transport::channel::Channel` reconnects when server is down.
        // But we still need to assert the readiness, e.g., when handshake token expires
        // If there was an error occurred, the channel will be closed.
        if let Some(e) = ch.take_error() {
            return Err(MetaNetworkError::from(e).into());
        }
        Ok(ch)
    }
}

/// A handle to access meta-client worker.
/// The worker will be actually running in a dedicated runtime: `MetaGrpcClient.rt`.
pub struct ClientHandle {
    /// For debug purpose only.
    pub endpoints: Vec<String>,
    /// For sending request to meta-client worker.
    pub(crate) req_tx: UnboundedSender<ClientWorkerRequest>,
    /// Notify auto sync to stop.
    /// `oneshot::Receiver` impl `Drop` by sending a closed notification to the `Sender` half.
    #[allow(dead_code)]
    cancel_auto_sync_tx: oneshot::Sender<()>,

    /// The reference to the dedicated runtime.
    ///
    /// If all ClientHandle are dropped, the runtime will be destroyed.
    ///
    /// In order not to let a blocking operation(such as calling the new PipelinePullingExecutor)
    /// in a tokio runtime block meta-client background tasks.
    /// If a background task is blocked, no meta-client will be able to proceed if meta-client is reused.
    ///
    /// Note that a thread_pool tokio runtime does not help:
    /// a scheduled tokio-task resides in `filo_slot` won't be stolen by other tokio-workers.
    ///
    /// This `rt` previously is stored in `MetaGrpcClient`, which leads to a deadlock:
    /// - When all `ClientHandle` are dropped, the two workers `worker_loop()` and `auto_sync_interval()`
    ///   will quit.
    /// - These two futures both held a reference to `MetaGrpcClient`.
    /// - The last of these(say, `F`) two will drop `MetaGrpcClient.rt` and `Runtime::_dropper`
    ///   will block waiting for the runtime to shut down.
    /// - But `F` is still held, deadlock occurs.
    _rt: Arc<Runtime>,
}

impl Display for ClientHandle {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ClientHandle({})", self.endpoints.join(","))
    }
}

impl Drop for ClientHandle {
    fn drop(&mut self) {
        info!("{} handle dropped", self);
    }
}

impl ClientHandle {
    /// Send a request to the internal worker task, which will be running in another runtime.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn request<Req, E>(&self, req: Req) -> Result<Req::Reply, E>
    where
        Req: RequestFor,
        Req: Into<message::Request>,
        Result<Req::Reply, E>: TryFrom<Response>,
        <Result<Req::Reply, E> as TryFrom<Response>>::Error: std::fmt::Display,
        E: From<MetaClientError> + Debug,
    {
        let rx = self.send_request_to_worker(req)?;
        UnlimitedFuture::create(async move {
            let _g = grpc_metrics::client_request_inflight.counter_guard();
            rx.await
        })
        .map(|recv_res| Self::parse_worker_result(recv_res))
        .await
    }

    /// Send a request to the internal worker task, which will be running in another runtime.
    #[fastrace::trace]
    pub fn request_sync<Req, E>(&self, req: Req) -> Result<Req::Reply, E>
    where
        Req: RequestFor,
        Req: Into<message::Request>,
        Result<Req::Reply, E>: TryFrom<Response>,
        <Result<Req::Reply, E> as TryFrom<Response>>::Error: std::fmt::Display,
        E: From<MetaClientError> + Debug,
    {
        let _g = grpc_metrics::client_request_inflight.counter_guard();

        let rx = self.send_request_to_worker(req)?;
        let recv_res = rx.blocking_recv();
        Self::parse_worker_result(recv_res)
    }

    /// Send request to client worker, return a receiver to receive the RPC response.
    fn send_request_to_worker<Req>(
        &self,
        req: Req,
    ) -> Result<oneshot::Receiver<Response>, MetaClientError>
    where
        Req: Into<message::Request>,
    {
        static META_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

        let (tx, rx) = oneshot::channel();
        let worker_request = ClientWorkerRequest {
            request_id: META_REQUEST_ID.fetch_add(1, Ordering::Relaxed),
            resp_tx: tx,
            req: req.into(),
            span: Span::enter_with_local_parent(std::any::type_name::<ClientWorkerRequest>()),
            tracking_payload: Some(ThreadTracker::new_tracking_payload()),
        };

        debug!(
            "{} send request to meta client worker: request: {:?}",
            self, worker_request
        );

        self.req_tx.send(worker_request).map_err(|e| {
            let req = e.0;

            let err = AnyError::error(format!(
                "Meta ClientHandle failed to send request(request_id={}, req_name={}) to worker",
                req.request_id,
                req.req.name()
            ));

            error!("{}", err);
            MetaClientError::ClientRuntimeError(err)
        })?;

        Ok(rx)
    }

    /// Parse the result returned from grpc client worker.
    fn parse_worker_result<Reply, E>(
        res: Result<Response, oneshot::error::RecvError>,
    ) -> Result<Reply, E>
    where
        Result<Reply, E>: TryFrom<Response>,
        <Result<Reply, E> as TryFrom<Response>>::Error: Display,
        E: From<MetaClientError> + Debug,
    {
        let response = res.map_err(|e| {
            error!(
                error :? =(&e);
                "Meta ClientHandle recv response from meta client worker failed"
            );
            MetaClientError::ClientRuntimeError(
                AnyError::new(&e).add_context(|| "when recv resp from MetaGrpcClient worker"),
            )
        })?;

        let res: Result<Reply, E> = response
            .try_into()
            .map_err(|e| format!("expect: {}, got: {}", std::any::type_name::<Reply>(), e))
            .unwrap();

        res
    }

    #[async_backtrace::framed]
    pub async fn get_cluster_status(&self) -> Result<ClusterStatus, MetaError> {
        self.request(message::GetClusterStatus {}).await
    }

    #[async_backtrace::framed]
    pub async fn get_client_info(&self) -> Result<ClientInfo, MetaError> {
        self.request(message::GetClientInfo {}).await
    }

    #[async_backtrace::framed]
    pub async fn make_established_client(&self) -> Result<EstablishedClient, MetaClientError> {
        self.request(message::MakeEstablishedClient {}).await
    }

    /// Return the endpoints list cached on this client.
    #[async_backtrace::framed]
    pub async fn get_cached_endpoints(&self) -> Result<Vec<String>, MetaError> {
        self.request(message::GetEndpoints {}).await
    }
}

// TODO: maybe it just needs a runtime, not a MetaGrpcClientWorker.
//
/// Meta grpc client has a internal worker task that deals with all traffic to remote meta service.
///
/// We expect meta-client should be cloneable.
/// But the underlying hyper client has a worker that runs in its creating tokio-runtime.
/// Thus a cloned meta client may try to talk to a destroyed hyper worker if the creating tokio-runtime is dropped.
/// Thus we have to guarantee that as long as there is a meta-client, the hyper worker runtime must not be dropped.
/// Thus a meta client creates a runtime then spawn a MetaGrpcClientWorker.
pub struct MetaGrpcClient {
    conn_pool: Pool<MetaChannelManager>,
    endpoints: Arc<Mutex<Endpoints>>,
    endpoints_str: Vec<String>,
    auto_sync_interval: Option<Duration>,
}

impl Debug for MetaGrpcClient {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut de = f.debug_struct("MetaGrpcClient");
        de.field("endpoints", &*self.endpoints.lock());
        de.field("auto_sync_interval", &self.auto_sync_interval);
        de.finish()
    }
}

impl fmt::Display for MetaGrpcClient {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MetaGrpcClient({})", self.endpoints_str.join(","))
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
    pub fn try_new(conf: &RpcClientConf) -> Result<Arc<ClientHandle>, MetaClientError> {
        Self::try_create(
            conf.get_endpoints(),
            &conf.username,
            &conf.password,
            conf.timeout,
            conf.auto_sync_interval,
            conf.tls_conf.clone(),
        )
    }

    #[fastrace::trace]
    pub fn try_create(
        endpoints_str: Vec<String>,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        auto_sync_interval: Option<Duration>,
        tls_config: Option<RpcClientTlsConfig>,
    ) -> Result<Arc<ClientHandle>, MetaClientError> {
        Self::endpoints_non_empty(&endpoints_str)?;

        let endpoints = Arc::new(Mutex::new(Endpoints::new(endpoints_str.clone())));

        let mgr =
            MetaChannelManager::new(username, password, timeout, tls_config, endpoints.clone());

        let rt = Runtime::with_worker_threads(
            1,
            Some(format!("meta-client-rt-{}", endpoints_str.join(","))),
        )
        .map_err(|e| {
            MetaClientError::ClientRuntimeError(
                AnyError::new(&e).add_context(|| "when creating meta-client"),
            )
        })?;
        let rt = Arc::new(rt);

        // Build the handle-worker pair

        let (tx, rx) = mpsc::unbounded_channel();
        let (one_tx, one_rx) = oneshot::channel::<()>();

        let handle = Arc::new(ClientHandle {
            endpoints: endpoints_str.clone(),
            req_tx: tx,
            cancel_auto_sync_tx: one_tx,
            _rt: rt.clone(),
        });

        let worker = Arc::new(Self {
            conn_pool: Pool::new(mgr, Duration::from_millis(50)),
            endpoints,
            endpoints_str,
            auto_sync_interval,
        });

        let worker_name = worker.to_string();

        rt.try_spawn(
            UnlimitedFuture::create(Self::worker_loop(worker.clone(), rx)),
            Some(format!("{}::worker_loop()", worker_name)),
        )
        .unwrap();

        rt.try_spawn(
            UnlimitedFuture::create(Self::auto_sync_endpoints(worker, one_rx)),
            Some(format!("{}::auto_sync_endpoints()", worker_name)),
        )
        .unwrap();

        Ok(handle)
    }

    /// A worker runs a receiving-loop to accept user-request to metasrv and deals with request in the dedicated runtime.
    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn worker_loop(self: Arc<Self>, mut req_rx: UnboundedReceiver<ClientWorkerRequest>) {
        info!("{}::worker spawned", self);

        loop {
            let recv_res = req_rx.recv().await;
            let Some(mut worker_request) = recv_res else {
                warn!("{} handle closed. worker quit", self);
                return;
            };

            debug!(worker_request :? =(&worker_request); "{} worker handle request", self);

            let _guard = ThreadTracker::tracking(worker_request.tracking_payload.take().unwrap());
            let span = Span::enter_with_parent(func_path!(), &worker_request.span);

            if worker_request.resp_tx.is_closed() {
                info!(
                    req :? =(&worker_request.req);
                    "{} request.resp_tx is closed, cancel handling this request", self
                );
                continue;
            }

            // Deal with non-RPC request
            #[allow(clippy::single_match)]
            match worker_request.req {
                message::Request::GetEndpoints(_) => {
                    let endpoints = self.get_all_endpoints();
                    let resp = Response::GetEndpoints(Ok(endpoints));
                    Self::send_response(
                        self.clone(),
                        worker_request.resp_tx,
                        worker_request.request_id,
                        resp,
                    );
                    continue;
                }
                _ => {}
            }

            databend_common_base::runtime::spawn_named(
                self.clone()
                    .handle_rpc_request(worker_request)
                    .in_span(span),
                format!("{}::handle_rpc_request()", self),
            );
        }
    }

    /// Handle a RPC request in a separate task.
    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn handle_rpc_request(self: Arc<Self>, worker_request: ClientWorkerRequest) {
        let request_id = worker_request.request_id;
        let resp_tx = worker_request.resp_tx;
        let req = worker_request.req;
        let req_name = req.name();
        let req_str = format!("{:?}", req);

        let start = Instant::now();
        let resp = match req {
            message::Request::StreamMGet(r) => {
                let strm = self
                    .kv_read_v1(MetaGrpcReadReq::MGetKV(r.into_inner()))
                    .with_timing_threshold(
                        threshold(),
                        info_spent("MetaGrpcClient::kv_read_v1(MGetKV)"),
                    )
                    .await;
                Response::StreamMGet(strm)
            }
            message::Request::StreamList(r) => {
                let strm = self
                    .kv_read_v1(MetaGrpcReadReq::ListKV(r.into_inner()))
                    .with_timing_threshold(
                        threshold(),
                        info_spent("MetaGrpcClient::kv_read_v1(ListKV)"),
                    )
                    .await;
                Response::StreamMGet(strm)
            }
            message::Request::Upsert(r) => {
                let resp = self
                    .kv_api(r)
                    .with_timing_threshold(threshold(), info_spent("MetaGrpcClient::kv_api"))
                    .await;
                Response::Upsert(resp)
            }
            message::Request::Txn(r) => {
                let resp = self
                    .transaction(r)
                    .with_timing_threshold(threshold(), info_spent("MetaGrpcClient::transaction"))
                    .await;
                Response::Txn(resp)
            }
            message::Request::Watch(r) => {
                let resp = self.watch(r).await;
                Response::Watch(resp)
            }
            message::Request::Export(r) => {
                let resp = self.export(r).await;
                Response::Export(resp)
            }
            message::Request::MakeEstablishedClient(_) => {
                let resp = self.get_established_client().await;
                Response::MakeEstablishedClient(resp)
            }
            message::Request::GetEndpoints(_) => {
                unreachable!("handled above");
            }
            message::Request::GetClusterStatus(_) => {
                let resp = self.get_cluster_status().await;
                Response::GetClusterStatus(resp)
            }
            message::Request::GetClientInfo(_) => {
                let resp = self.get_client_info().await;
                Response::GetClientInfo(resp)
            }
        };

        self.update_rpc_metrics(req_name, &req_str, request_id, start, resp.err());

        Self::send_response(self.clone(), resp_tx, request_id, resp);
    }

    fn send_response(self: Arc<Self>, tx: OneSend<Response>, request_id: u64, resp: Response) {
        debug!(
            "{} send response to the handle; request_id={}, resp={:?}",
            self, request_id, resp
        );

        let send_res = tx.send(resp);
        if let Err(err) = send_res {
            error!(
                "{} failed to send response to the handle. recv-end closed; request_id={}, error={:?}",
                self, request_id, err
            );
        }
    }

    fn update_rpc_metrics(
        &self,
        req_name: &'static str,
        req_str: &str,
        request_id: u64,
        start: Instant,
        resp_err: Option<&(dyn std::error::Error + 'static)>,
    ) {
        let current_endpoint = self.get_current_endpoint();

        let Some(endpoint) = current_endpoint else {
            return;
        };

        // Duration metrics
        {
            let elapsed = start.elapsed().as_millis() as f64;
            grpc_metrics::record_meta_grpc_client_request_duration_ms(&endpoint, req_name, elapsed);

            if elapsed > 1000_f64 {
                warn!(
                    "{} slow request {} to {} takes {} ms; request_id={}; request: {}",
                    self, req_name, endpoint, elapsed, request_id, req_str,
                );
            }
        }

        // Error metrics
        if let Some(err) = resp_err {
            grpc_metrics::incr_meta_grpc_client_request_failed(&endpoint, req_name, err);
            error!("{} request_id={} error: {:?}", self, request_id, err);
        } else {
            grpc_metrics::incr_meta_grpc_client_request_success(&endpoint, req_name);
        }
    }

    /// Return a client for communication, and a server version in form of `{major:03}.{minor:03}.{patch:03}`.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn get_established_client(&self) -> Result<EstablishedClient, MetaClientError> {
        let (endpoints_str, n) = {
            let eps = self.endpoints.lock();
            (eps.to_string(), eps.len())
        };
        debug_assert!(n > 0);

        debug!("{}::{}; endpoints: {}", self, func_name!(), endpoints_str);

        let mut last_err = None::<MetaClientError>;

        for _ith in 0..n {
            let addr = {
                let mut es = self.endpoints.lock();
                es.current_or_next().to_string()
            };

            debug!("{} get or build ReadClient to {}", self, addr);

            let res = self.conn_pool.get(&addr).await;

            match res {
                Ok(client) => {
                    return Ok(client);
                }
                Err(client_err) => {
                    error!(
                        "Failed to get or build RealClient to {}, err: {:?}",
                        addr, client_err
                    );
                    grpc_metrics::incr_meta_grpc_make_client_fail(&addr);
                    self.choose_next_endpoint();
                    last_err = Some(client_err);
                    continue;
                }
            }
        }

        if let Some(e) = last_err {
            return Err(e);
        }

        let conn_err =
            ConnectionError::new(AnyError::error(&endpoints_str), "no endpoints to connect");

        Err(MetaClientError::NetworkError(
            MetaNetworkError::ConnectionError(conn_err),
        ))
    }

    pub fn endpoints_non_empty(endpoints: &[String]) -> Result<(), MetaClientError> {
        if endpoints.is_empty() {
            return Err(MetaClientError::ConfigError(AnyError::error(
                "endpoints is empty",
            )));
        }
        Ok(())
    }

    fn get_all_endpoints(&self) -> Vec<String> {
        let eps = self.endpoints.lock();
        eps.nodes().cloned().collect()
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn set_endpoints(&self, endpoints: Vec<String>) -> Result<(), MetaError> {
        Self::endpoints_non_empty(&endpoints)?;

        // Older meta nodes may not store endpoint information and need to be filtered out.
        let distinct_cnt = endpoints.iter().filter(|n| !(*n).is_empty()).count();

        // If the fetched endpoints are less than the majority of the current cluster, no replacement should occur.
        if distinct_cnt < endpoints.len() / 2 + 1 {
            warn!(
                "distinct endpoints small than majority of meta cluster nodes {}<{}, endpoints: {:?}",
                distinct_cnt,
                endpoints.len(),
                endpoints
            );
            return Ok(());
        }

        let mut eps = self.endpoints.lock();
        eps.replace_nodes(endpoints);
        Ok(())
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn sync_endpoints(&self) -> Result<(), MetaError> {
        let mut client = self.get_established_client().await?;
        let result = client
            .member_list(Request::new(MemberListRequest {
                data: "".to_string(),
            }))
            .await;
        let endpoints: Result<MemberListReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if is_status_retryable(&s) {
                    self.choose_next_endpoint();
                    let mut client = self.get_established_client().await?;
                    let req = Request::new(MemberListRequest {
                        data: "".to_string(),
                    });
                    Ok(client.member_list(req).await?.into_inner())
                } else {
                    Err(s)
                }
            }
        };
        let result: Vec<String> = endpoints?.data;
        debug!("received meta endpoints: {:?}", result);

        self.set_endpoints(result).await?;
        Ok(())
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn auto_sync_endpoints(self: Arc<Self>, mut cancel_rx: oneshot::Receiver<()>) {
        info!(
            "{} start auto_sync_endpoints: interval: {:?}",
            self, self.auto_sync_interval
        );

        if let Some(interval) = self.auto_sync_interval {
            loop {
                debug!("{} auto_sync_endpoints loop start", self);

                select! {
                    _ = &mut cancel_rx => {
                        info!("{} auto_sync_endpoints received quit signal, quit", self);
                        return;
                    }
                    _ = sleep(interval) => {
                        let r = self.sync_endpoints().await;
                        if let Err(e) = r {
                            warn!("{} auto_sync_endpoints failed: {:?}", self, e);
                        }
                    }
                }
            }
        }
    }

    /// Handshake with metasrv.
    ///
    /// - Check whether the versions of this client(`C`) and the remote metasrv(`S`) are compatible.
    /// - Authorize this client.
    ///
    /// ## Check compatibility
    ///
    /// Both client `C` and  server `S` maintains two semantic-version:
    /// - `C` maintains the its own semver(`C.ver`) and the minimal compatible `S` semver(`C.min_srv_ver`).
    /// - `S` maintains the its own semver(`S.ver`) and the minimal compatible `S` semver(`S.min_cli_ver`).
    ///
    /// When handshaking:
    /// - `C` sends its ver `C.ver` to `S`,
    /// - When `S` receives handshake request, `S` asserts that `C.ver >= S.min_cli_ver`.
    /// - Then `S` replies handshake-reply with its `S.ver`.
    /// - When `C` receives the reply, `C` asserts that `S.ver >= C.min_srv_ver`.
    ///
    /// Handshake succeeds if both of these two assertions hold.
    ///
    /// E.g.:
    /// - `S: (ver=3, min_cli_ver=1)` is compatible with `C: (ver=3, min_srv_ver=2)`.
    /// - `S: (ver=4, min_cli_ver=4)` is **NOT** compatible with `C: (ver=3, min_srv_ver=2)`.
    ///   Because although `S.ver(4) >= C.min_srv_ver(3)` holds,
    ///   but `C.ver(3) >= S.min_cli_ver(4)` does not hold.
    ///
    /// ```text
    /// C.ver:    1             3      4
    /// C --------+-------------+------+------------>
    ///           ^      .------'      ^
    ///           |      |             |
    ///           '-------------.      |
    ///                  |      |      |
    ///                  v      |      |
    /// S ---------------+------+------+------------>
    /// S.ver:           2      3      4
    /// ```
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn handshake(
        client: &mut RealClient,
        client_ver: &Version,
        min_metasrv_ver: &Version,
        username: &str,
        password: &str,
    ) -> Result<(Vec<u8>, u64), MetaHandshakeError> {
        debug!(
            client_ver :% =(client_ver),
            min_metasrv_ver :% =(min_metasrv_ver);
            "client version"
        );

        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        let mut payload = vec![];

        // TODO: return MetaNetworkError
        auth.encode(&mut payload)
            .map_err(|e| MetaHandshakeError::new("encode auth payload", &e))?;

        let my_ver = to_digit_ver(client_ver);
        let req = Request::new(futures::stream::once(async move {
            HandshakeRequest {
                protocol_version: my_ver,
                payload,
            }
        }));

        // TODO: return MetaNetworkError
        let rx = client
            .handshake(req)
            .await
            .map_err(|e| MetaHandshakeError::new("when sending handshake rpc", &e))?;
        let mut rx = rx.into_inner();

        // TODO: return MetaNetworkError
        let res = rx.next().await.ok_or_else(|| {
            MetaHandshakeError::new(
                "when recv from handshake stream",
                &AnyError::error("handshake returns nothing"),
            )
        })?;

        let resp =
            res.map_err(|status| MetaHandshakeError::new("handshake is refused", &status))?;

        assert!(
            resp.protocol_version > 0,
            "talking to a very old databend-meta: upgrade databend-meta to at least 0.8"
        );

        let min_compatible = to_digit_ver(min_metasrv_ver);
        if resp.protocol_version < min_compatible {
            let invalid_err = AnyError::error(format!(
                "metasrv protocol_version({}) < meta-client min-compatible({})",
                from_digit_ver(resp.protocol_version),
                min_metasrv_ver,
            ));
            return Err(MetaHandshakeError::new(
                "incompatible protocol version",
                &invalid_err,
            ));
        }

        let token = resp.payload;
        let server_version = resp.protocol_version;

        Ok((token, server_version))
    }

    /// Create a watching stream that receives KV change events.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn watch(
        &self,
        watch_request: WatchRequest,
    ) -> Result<tonic::codec::Streaming<WatchResponse>, MetaError> {
        debug!("{}: handle watch request: {:?}", self, watch_request);

        let mut client = self.get_established_client().await?;
        let res = client.watch(watch_request).await?;
        Ok(res.into_inner())
    }

    /// Export all data in json from metasrv.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn export(
        &self,
        export_request: message::ExportReq,
    ) -> Result<tonic::codec::Streaming<ExportedChunk>, MetaError> {
        debug!(
            "{} worker: handle export request: {:?}",
            self, export_request
        );

        let mut client = self.get_established_client().await?;
        // TODO: since 1.2.315, export_v1() is added, via which chunk size can be specified.
        let res = if client.server_protocol_version() >= 1002315 {
            client
                .export_v1(pb::ExportRequest {
                    chunk_size: export_request.chunk_size,
                })
                .await?
        } else {
            client.export(Empty {}).await?
        };
        Ok(res.into_inner())
    }

    /// Get cluster status
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn get_cluster_status(&self) -> Result<ClusterStatus, MetaError> {
        debug!("{}::get_cluster_status", self);

        let mut client = self.get_established_client().await?;
        let res = client.get_cluster_status(Empty {}).await?;
        Ok(res.into_inner())
    }

    /// Export all data in json from metasrv.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn get_client_info(&self) -> Result<ClientInfo, MetaError> {
        debug!("{}::get_client_info", self);

        let mut client = self.get_established_client().await?;
        let res = client.get_client_info(Empty {}).await?;
        Ok(res.into_inner())
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn kv_api<T>(&self, v: T) -> Result<T::Reply, MetaError>
    where
        T: RequestFor,
        T: Into<MetaGrpcReq>,
        T::Reply: DeserializeOwned,
    {
        let grpc_req: MetaGrpcReq = v.into();

        debug!("{}::kv_api request: {:?}", self, grpc_req);

        let raft_req: RaftRequest = grpc_req.into();

        let mut failures = vec![];

        for i in 0..RPC_RETRIES {
            let mut client = self
                .get_established_client()
                .with_timing_threshold(threshold(), info_spent("MetaGrpcClient::make_client"))
                .await?;

            let req = traced_req(raft_req.clone());

            let result = client
                .kv_api(req)
                .with_timing_threshold(threshold(), info_spent("client::kv_api"))
                .await;

            debug!(
                result :? =(&result);
                "MetaGrpcClient::kv_api result, {}-th try", i
            );

            if let Err(ref e) = result {
                warn!(
                    req :? =(&raft_req),
                    error :? =(&e);
                    "MetaGrpcClient::kv_api error");

                if is_status_retryable(e) {
                    warn!(
                        req :? =(&raft_req),
                        error :? =(&e);
                        "MetaGrpcClient::kv_api error is retryable");

                    self.choose_next_endpoint();
                    failures.push(e.clone());
                    continue;
                }
            }

            let raft_reply = result?.into_inner();

            let resp: T::Reply = reply_to_api_result(raft_reply)?;
            return Ok(resp);
        }

        let net_err = MetaNetworkError::ConnectionError(ConnectionError::new(
            AnyError::error(format_args!(
                "failed after {} retries: {:?}",
                RPC_RETRIES, failures
            )),
            "failed to connect to meta-service",
        ));

        Err(net_err.into())
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn kv_read_v1(
        &self,
        grpc_req: MetaGrpcReadReq,
    ) -> Result<BoxStream<pb::StreamItem>, MetaError> {
        debug!("{}::kv_read_v1 request: {:?}", self, grpc_req);

        let mut failures = vec![];

        for i in 0..RPC_RETRIES {
            let mut established_client = self
                .get_established_client()
                .with_timing_threshold(
                    threshold(),
                    info_spent("MetaGrpcClient::get_established_client"),
                )
                .await?;

            let raft_req: RaftRequest = grpc_req.clone().into();
            let req = traced_req(raft_req.clone());

            let result = established_client
                .kv_read_v1(req)
                .with_timing_threshold(threshold(), info_spent("client::kv_read_v1"))
                .await;

            debug!(
                "{}::kv_read_v1 result, {}-th try; result: {:?}",
                self, i, result
            );

            if let Err(ref e) = result {
                warn!(
                    "{}::kv_read_v1 error, retryable: {}, target={}; error: {:?}; request: {:?}",
                    self,
                    is_status_retryable(e),
                    established_client.target_endpoint(),
                    e,
                    grpc_req
                );

                if is_status_retryable(e) {
                    self.choose_next_endpoint();
                    failures.push(e.clone());
                    continue;
                }
            }

            let strm = result?.into_inner();

            return Ok(strm.boxed());
        }

        let net_err = MetaNetworkError::ConnectionError(ConnectionError::new(
            AnyError::error(format_args!(
                "failed after {} retries: {:?}",
                RPC_RETRIES, failures
            )),
            "failed to connect to meta-service",
        ));

        Err(net_err.into())
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn transaction(&self, req: TxnRequest) -> Result<TxnReply, MetaError> {
        let txn: TxnRequest = req;

        debug!("{}::transaction request: {}", self, txn);

        let req = traced_req(txn.clone());

        let mut client = self.get_established_client().await?;
        let result = client.transaction(req).await;

        let result: Result<TxnReply, Status> = match result {
            Ok(r) => return Ok(r.into_inner()),
            Err(s) => {
                if is_status_retryable(&s) {
                    self.choose_next_endpoint();
                    let mut client = self.get_established_client().await?;
                    let req = traced_req(txn);
                    let ret = client.transaction(req).await?.into_inner();
                    return Ok(ret);
                } else {
                    Err(s)
                }
            }
        };

        let reply = result?;

        debug!("{}::transaction reply: {}", self, reply);

        Ok(reply)
    }

    fn get_current_endpoint(&self) -> Option<String> {
        let es = self.endpoints.lock();
        es.current().map(|x| x.to_string())
    }

    fn choose_next_endpoint(&self) {
        let next = {
            let mut es = self.endpoints.lock();
            es.choose_next().to_string()
        };

        info!("{} choose_next_endpoint: {}", self, next);
    }
}

/// Inject span into a tonic request, so that on the remote peer the tracing context can be restored.
fn traced_req<T>(t: T) -> Request<T> {
    let req = Request::new(t);
    let mut req = databend_common_tracing::inject_span_to_tonic_request(req);

    if let Some(query_id) = ThreadTracker::query_id() {
        let key = tonic::metadata::AsciiMetadataKey::from_str("QueryID");
        let value = tonic::metadata::AsciiMetadataValue::from_str(query_id);

        if let Some((key, value)) = key.ok().zip(value.ok()) {
            req.metadata_mut().insert(key, value);
        }
    }

    req
}

fn is_status_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unauthenticated | Code::Unavailable | Code::Internal | Code::Cancelled
    )
}

/// Fill in auth token into request metadata.
///
/// The token is stored in a `OnceCell`, which is fill in when handshake is done.
#[derive(Clone)]
pub struct AuthInterceptor {
    pub token: Arc<OnceCell<Vec<u8>>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let metadata = req.metadata_mut();

        // The handshake does not need token.
        // When the handshake is done, token is filled.
        let Some(token) = self.token.get() else {
            return Ok(req);
        };

        let meta_value = MetadataValue::from_bytes(token.as_ref());
        metadata.insert_bin(AUTH_TOKEN_KEY, meta_value);
        Ok(req)
    }
}

fn threshold() -> Duration {
    Duration::from_millis(300)
}

fn info_spent(msg: impl Display) -> impl Fn(Duration, Duration) {
    move |total, busy| {
        info!("{} spent: total: {:?}, busy: {:?}", msg, total, busy);
    }
}
