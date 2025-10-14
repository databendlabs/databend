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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use arrow_flight::BasicAuth;
use databend_common_base::base::tokio::select;
use databend_common_base::base::tokio::sync::mpsc;
use databend_common_base::base::tokio::sync::mpsc::UnboundedReceiver;
use databend_common_base::base::tokio::sync::oneshot;
use databend_common_base::base::tokio::sync::oneshot::Sender as OneSend;
use databend_common_base::base::tokio::time::sleep;
use databend_common_base::base::BuildInfoRef;
use databend_common_base::containers::Pool;
use databend_common_base::future::TimedFutureExt;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::runtime::UnlimitedFuture;
use databend_common_grpc::RpcClientConf;
use databend_common_grpc::RpcClientTlsConfig;
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
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaHandshakeError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use fastrace::func_path;
use fastrace::future::FutureExt as MTFutureExt;
use fastrace::Span;
use futures::stream::StreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use prost::Message;
use semver::Version;
use tonic::codegen::BoxStream;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::Code;
use tonic::Request;
use tonic::Status;
use tonic::Streaming;

use crate::endpoints::rotate_failing_endpoint;
use crate::endpoints::Endpoints;
use crate::errors::CreationError;
use crate::established_client::EstablishedClient;
use crate::from_digit_ver;
use crate::grpc_metrics;
use crate::message;
use crate::message::Response;
use crate::required::features;
use crate::required::std;
use crate::required::supported_features;
use crate::required::Features;
use crate::rpc_handler::ResponseAction;
use crate::rpc_handler::RpcHandler;
use crate::to_digit_ver;
use crate::ClientHandle;
use crate::ClientWorkerRequest;
use crate::FeatureSpec;
use crate::MetaChannelManager;
use crate::MetaGrpcReadReq;

const RPC_RETRIES: usize = 4;
const AUTH_TOKEN_KEY: &str = "auth-token-bin";

pub(crate) type RealClient = MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>;

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

    #[allow(dead_code)]
    required_features: &'static [FeatureSpec],
}

impl Debug for MetaGrpcClient {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut de = f.debug_struct("MetaGrpcClient");
        de.field("endpoints", &*self.endpoints.lock());
        de.field("auto_sync_interval", &self.auto_sync_interval);
        de.finish()
    }
}

impl Display for MetaGrpcClient {
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
    pub fn try_new(conf: &RpcClientConf) -> Result<Arc<ClientHandle>, CreationError> {
        Self::try_create(
            conf.get_endpoints(),
            conf.version,
            &conf.username,
            &conf.password,
            conf.timeout,
            conf.auto_sync_interval,
            conf.tls_conf.clone(),
        )
    }

    /// Create a new meta-client with exact required features.
    #[fastrace::trace]
    pub fn try_create(
        endpoints_str: Vec<String>,
        version: BuildInfoRef,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        auto_sync_interval: Option<Duration>,
        tls_config: Option<RpcClientTlsConfig>,
    ) -> Result<Arc<ClientHandle>, CreationError> {
        Self::try_create_with_features(
            endpoints_str,
            version,
            username,
            password,
            timeout,
            auto_sync_interval,
            tls_config,
            std(),
        )
    }

    /// Create a new meta-client with specific required features.
    ///
    /// This method enables version compatibility by allowing an older meta-client to
    /// connect to a newer meta-server with a limited feature set.
    /// During handshaking, the server verifies that it supports all features in `required_features`.
    ///
    /// Features not specified in `required_features` may still be available if the server supports them,
    /// but errors will only be returned when those features are actually accessed.
    ///
    /// For common feature sets, use the predefined collections in the `required` module:
    /// - `std()` - Standard features for general client usage
    /// - `business()` - Core business logic features (read, write, transaction)
    /// - `export()` - Features required for export operations
    #[fastrace::trace]
    pub fn try_create_with_features(
        endpoints_str: Vec<String>,
        version: BuildInfoRef,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        auto_sync_interval: Option<Duration>,
        tls_config: Option<RpcClientTlsConfig>,
        required_features: &'static [FeatureSpec],
    ) -> Result<Arc<ClientHandle>, CreationError> {
        Self::endpoints_non_empty(&endpoints_str)?;

        let endpoints = Arc::new(Mutex::new(Endpoints::new(endpoints_str.clone())));

        let mgr = MetaChannelManager::new(
            version,
            username,
            password,
            timeout,
            tls_config,
            required_features,
            endpoints.clone(),
        );

        let rt = Runtime::with_worker_threads(
            1,
            Some(format!("meta-client-rt-{}", endpoints_str.join(","))),
        )
        .map_err(|e| {
            CreationError::new_runtime_error(e.to_string()).context("when creating meta-client")
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
            required_features,
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
                info!("{} handle closed. worker quit", self);
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
                    .with_timing(|result, total, busy| {
                        log_future_result(
                            result,
                            total,
                            busy,
                            "MetaGrpcClient::kv_read_v1(MGetKV)",
                            &req_str,
                        )
                    })
                    .await;
                Response::StreamMGet(strm)
            }
            message::Request::StreamList(r) => {
                let strm = self
                    .kv_read_v1(MetaGrpcReadReq::ListKV(r.into_inner()))
                    .with_timing(|result, total, busy| {
                        log_future_result(
                            result,
                            total,
                            busy,
                            "MetaGrpcClient::kv_read_v1(ListKV)",
                            &req_str,
                        )
                    })
                    .await;
                Response::StreamMGet(strm)
            }
            message::Request::Txn(r) => {
                let resp = self
                    .transaction(r)
                    .with_timing(|result, total, busy| {
                        log_future_result(
                            result,
                            total,
                            busy,
                            "MetaGrpcClient::transaction",
                            &req_str,
                        )
                    })
                    .await;
                Response::Txn(resp)
            }
            message::Request::Watch(r) => {
                let resp = self.watch(r).await;
                Response::Watch(resp)
            }
            message::Request::WatchWithInitialization((r, _)) => {
                let resp = self.watch_with_initialization(r).await;
                Response::WatchWithInitialization(resp)
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
            message::Request::GetMemberList(_) => {
                let resp = self.get_member_list().await;
                Response::GetMemberList(resp)
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
        // TODO: this current endpoint is not stable, may be modified by other thread.
        //       The caller should passing the in use endpoint.
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

                    rotate_failing_endpoint(&self.endpoints, Some(&addr), self);

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

    pub fn endpoints_non_empty(endpoints: &[String]) -> Result<(), CreationError> {
        if endpoints.is_empty() {
            return Err(CreationError::new_config_error("endpoints is empty"));
        }
        Ok(())
    }

    fn get_all_endpoints(&self) -> Vec<String> {
        let eps = self.endpoints.lock();
        eps.nodes().cloned().collect()
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub fn set_endpoints(&self, endpoints: Vec<String>) {
        debug_assert!(!endpoints.is_empty());

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
            return;
        }

        let mut eps = self.endpoints.lock();
        eps.replace_nodes(endpoints);
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn sync_endpoints(&self) -> Result<(), MetaError> {
        let mut established_client = self.get_established_client().await?;

        let result = established_client
            .member_list(Request::new(MemberListRequest {
                data: "".to_string(),
            }))
            .await;

        let endpoints: Result<MemberListReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if is_status_retryable(&s) {
                    established_client.rotate_failing_target();

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

        if result.is_empty() {
            error!("Can not update local endpoints, the returned result is empty");
        } else {
            self.set_endpoints(result);
        }

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
        required_server_features: &'static [FeatureSpec],
        username: &str,
        password: &str,
    ) -> Result<(Vec<u8>, u64, Features), MetaHandshakeError> {
        debug!(
            "client version: {client_ver}, required server versions: {required_server_features:?}"
        );

        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        let mut payload = vec![];

        // TODO: return MetaNetworkError
        auth.encode(&mut payload).map_err(|e| {
            MetaHandshakeError::new("Fail to encode request payload").with_source(&e)
        })?;

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
            .map_err(|e| MetaHandshakeError::new("Connection Failure").with_source(&e))?;
        let mut rx = rx.into_inner();

        let res = rx
            .next()
            .await
            .ok_or_else(|| MetaHandshakeError::new("Server returned nothing"))?;

        let resp = res
            .map_err(|status| MetaHandshakeError::new("Connection Failure").with_source(&status))?;

        assert!(
            resp.protocol_version > 0,
            "talking to a very old databend-meta: upgrade databend-meta to at least 0.8"
        );

        let server_version = from_digit_ver(resp.protocol_version);
        let server_tuple = (
            server_version.major,
            server_version.minor,
            server_version.patch,
        );

        let server_provided = supported_features(server_tuple);

        for (feat, least_server_version) in required_server_features {
            if server_tuple < *least_server_version {
                return Err(MetaHandshakeError::new(format!(
                    "Invalid: server protocol_version({:?}) < client required({:?}) for feature {}",
                    server_tuple, least_server_version, feat
                )));
            }
        }

        let token = resp.payload;
        let server_version = resp.protocol_version;

        Ok((token, server_version, server_provided))
    }

    /// Create a watching stream that receives KV change events.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn watch(
        &self,
        watch_request: WatchRequest,
    ) -> Result<Streaming<WatchResponse>, MetaClientError> {
        debug!("{}: handle watch request: {:?}", self, watch_request);

        let mut client = self.get_established_client().await?;
        client.ensure_feature("watch")?;

        if watch_request.initial_flush {
            client.ensure_feature("watch/initial_flush")?;
        }
        let res = client.watch(watch_request).await?;
        Ok(res.into_inner())
    }

    /// Create a watching stream that receives KV change events.
    ///
    /// This method is similar to `watch`, but it also sends all existing key-values with `is_initialization=true`
    /// before starting the watch stream.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn watch_with_initialization(
        &self,
        watch_request: WatchRequest,
    ) -> Result<Streaming<WatchResponse>, MetaClientError> {
        debug!("{}: handle watch request: {:?}", self, watch_request);

        let mut client = self.get_established_client().await?;
        client.ensure_feature_spec(&features::WATCH)?;
        client.ensure_feature_spec(&features::WATCH_INITIAL_FLUSH)?;
        client.ensure_feature_spec(&features::WATCH_INIT_FLAG)?;

        let res = client.watch(watch_request).await?;
        Ok(res.into_inner())
    }

    /// Export all data in json from metasrv.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn export(
        &self,
        export_request: message::ExportReq,
    ) -> Result<Streaming<ExportedChunk>, MetaClientError> {
        debug!(
            "{} worker: handle export request: {:?}",
            self, export_request
        );

        let mut client = self.get_established_client().await?;
        let res = if client.has_feature("export_v1") {
            client
                .export_v1(pb::ExportRequest {
                    chunk_size: export_request.chunk_size,
                })
                .await?
        } else {
            client.ensure_feature("export")?;
            client.export(Empty {}).await?
        };
        Ok(res.into_inner())
    }

    /// Get cluster status
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn get_cluster_status(&self) -> Result<ClusterStatus, MetaClientError> {
        debug!("{}::get_cluster_status", self);

        let mut client = self.get_established_client().await?;
        client.ensure_feature("get_cluster_status")?;
        let res = client.get_cluster_status(Empty {}).await?;
        Ok(res.into_inner())
    }

    /// Get member list
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn get_member_list(&self) -> Result<MemberListReply, MetaClientError> {
        debug!("{}::get_member_list", self);
        let mut client = self.get_established_client().await?;
        let req = MemberListRequest {
            data: "".to_string(),
        };
        let res = client.member_list(req).await?;
        Ok(res.into_inner())
    }

    /// Export all data in json from metasrv.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn get_client_info(&self) -> Result<ClientInfo, MetaClientError> {
        debug!("{}::get_client_info", self);

        let mut client = self.get_established_client().await?;
        client.ensure_feature("get_client_info")?;
        let res = client.get_client_info(Empty {}).await?;
        Ok(res.into_inner())
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn kv_read_v1(
        &self,
        grpc_req: MetaGrpcReadReq,
    ) -> Result<BoxStream<pb::StreamItem>, MetaError> {
        let service_spec = features::KV_READ_V1;

        debug!("{}::kv_read_v1 request: {:?}", self, grpc_req);

        let raft_req: RaftRequest = grpc_req.clone().into();
        let mut rpc_handler = RpcHandler::new(self, service_spec);

        for _i in 0..RPC_RETRIES {
            let req = traced_req(raft_req.clone());

            let established = rpc_handler.new_established_client().await?;

            debug!("{}::kv_read_v1 established client: {:?}", self, established);

            let result = established
                .kv_read_v1(req)
                .with_timing_threshold(threshold(), info_spent(service_spec.0))
                .await;

            debug!("{self}::kv_read_v1 result: {:?}", result);

            let retryable = rpc_handler.process_response_result(&grpc_req, result)?;

            let response = match retryable {
                ResponseAction::Success(resp) => resp,
                ResponseAction::ShouldRetry => {
                    continue;
                }
            };

            let strm = response.into_inner();
            return Ok(strm.boxed());
        }

        let net_err = rpc_handler.create_network_error();
        Err(net_err.into())
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub(crate) async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, MetaClientError> {
        let service_spec = features::TRANSACTION;

        debug!("{self}::transaction request: {txn}");

        let mut rpc_handler = RpcHandler::new(self, service_spec);

        for _i in 0..RPC_RETRIES {
            let req = traced_req(txn.clone());

            let established = rpc_handler.new_established_client().await?;

            let result = established
                .transaction(req)
                .with_timing_threshold(threshold(), info_spent(service_spec.0))
                .await;

            let retryable = rpc_handler.process_response_result(&txn, result)?;

            let response = match retryable {
                ResponseAction::Success(resp) => resp,
                ResponseAction::ShouldRetry => {
                    continue;
                }
            };

            let txn_reply = response.into_inner();
            return Ok(txn_reply);
        }

        let net_err = rpc_handler.create_network_error();
        Err(net_err.into())
    }

    fn get_current_endpoint(&self) -> Option<String> {
        let es = self.endpoints.lock();
        es.current().map(|x| x.to_string())
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

fn info_spent<T>(msg: impl Display) -> impl Fn(&T, Duration, Duration)
where T: Debug {
    move |output, total, busy| {
        info!(
            "{} spent: total: {:?}, busy: {:?}; result: {:?}",
            msg, total, busy, output
        );
    }
}

fn log_future_result<T, E>(
    t: &Result<T, E>,
    total: Duration,
    busy: Duration,
    req_type: impl Display,
    req_str: impl Display,
) where
    E: Debug,
{
    if let Err(e) = t {
        warn!(
            "{req_type}: done with error: Elapsed: total: {:?}, busy: {:?}; error: {:?}; request: {}",
            total,
            busy,
            e,
            req_str

        );
    }

    if total > threshold() {
        warn!(
            "{req_type}: done slowly: Elapsed: total: {:?}, busy: {:?}; request: {}",
            total, busy, req_str
        );
    }
}
