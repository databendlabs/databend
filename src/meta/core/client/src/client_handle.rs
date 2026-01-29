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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use anyerror::AnyError;
use databend_base::counter::Counter;
use databend_common_base::runtime::ThreadTracker;
use databend_common_meta_kvapi::kvapi::ListKVReq;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_runtime_api::ClientMetricsApi;
use databend_common_meta_runtime_api::SpawnApi;
use databend_common_meta_types::ConnectionError;
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use databend_common_meta_types::protobuf::ClientInfo;
use databend_common_meta_types::protobuf::ClusterStatus;
use databend_common_meta_types::protobuf::MemberListReply;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use fastrace::Span;
use log::debug;
use log::error;
use log::info;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tonic::codegen::BoxStream;

use crate::ClientWorkerRequest;
use crate::InitFlag;
use crate::RequestFor;
use crate::Streamed;
use crate::established_client::EstablishedClient;
use crate::message;
use crate::message::Response;

/// A handle to access meta-client worker.
/// The worker will be actually running in a dedicated runtime: `MetaGrpcClient.rt`.
pub struct ClientHandle<RT: SpawnApi> {
    /// For debug purpose only.
    pub endpoints: Vec<String>,
    /// For sending request to meta-client worker.
    pub(crate) req_tx: UnboundedSender<ClientWorkerRequest>,
    /// Notify auto sync to stop.
    /// `oneshot::Receiver` impl `Drop` by sending a closed notification to the `Sender` half.
    #[allow(dead_code)]
    pub(crate) cancel_auto_sync_tx: oneshot::Sender<()>,

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
    #[allow(dead_code)]
    pub(crate) _rt: Arc<dyn Any + Send + Sync>,

    pub(crate) _phantom: PhantomData<RT>,
}

impl<RT: SpawnApi> Display for ClientHandle<RT> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ClientHandle({})", self.endpoints.join(","))
    }
}

impl<RT: SpawnApi> Drop for ClientHandle<RT> {
    fn drop(&mut self) {
        info!("{} handle dropped", self);
    }
}

impl<RT: SpawnApi> ClientHandle<RT> {
    pub async fn list(&self, prefix: &str) -> Result<BoxStream<StreamItem>, MetaError> {
        let strm = self
            .request(Streamed(ListKVReq {
                prefix: prefix.to_string(),
            }))
            .await?;

        Ok(strm)
    }

    /// Watch without requiring the initialization support
    pub async fn watch(
        &self,
        watch: WatchRequest,
    ) -> Result<tonic::codec::Streaming<WatchResponse>, MetaClientError> {
        self.request(watch).await
    }

    /// Watch with requiring the initialization support
    pub async fn watch_with_initialization(
        &self,
        watch: WatchRequest,
    ) -> Result<tonic::codec::Streaming<WatchResponse>, MetaClientError> {
        self.request((watch, InitFlag)).await
    }

    pub async fn upsert_via_txn(&self, upsert: UpsertKV) -> Result<UpsertKVReply, MetaClientError> {
        let txn = TxnRequest::from_upsert(upsert);

        let resp = self.request(txn).await?;

        let reply = resp.into_upsert_reply()?;
        Ok(reply)
    }

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
        let rx = self
            .send_request_to_worker(req)
            .map_err(MetaClientError::from)?;

        let recv_res = RT::unlimited_future(async move {
            let _g = request_inflight::<RT>().counted_guard();
            rx.await
        })
        .await;

        Self::parse_worker_result(recv_res)
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
        let _g = request_inflight::<RT>().counted_guard();

        let rx = self
            .send_request_to_worker(req)
            .map_err(MetaClientError::from)?;

        let recv_res = rx.blocking_recv();
        Self::parse_worker_result(recv_res)
    }

    /// Send request to client worker, return a receiver to receive the RPC response.
    fn send_request_to_worker<Req>(
        &self,
        req: Req,
    ) -> Result<oneshot::Receiver<Response>, ConnectionError>
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
            tracking_fn: Some({
                let payload = ThreadTracker::new_tracking_payload();
                Box::new(move || -> Box<dyn std::any::Any + Send> {
                    Box::new(ThreadTracker::tracking(payload))
                })
            }),
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
            ConnectionError::new(err, "Meta ClientHandle failed to send request to worker")
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
            let err = AnyError::new(&e).add_context(|| "when recv resp from MetaGrpcClient worker");
            error!(
                error :? =(&e);
                "Meta ClientHandle recv response from meta client worker failed"
            );
            let conn_err =
                ConnectionError::new(err, "Meta ClientHandle failed to receive from worker");
            MetaClientError::from(conn_err)
        })?;

        let res: Result<Reply, E> = response
            .try_into()
            .map_err(|e| format!("expect: {}, got: {}", std::any::type_name::<Reply>(), e))
            .unwrap();

        res
    }

    #[async_backtrace::framed]
    pub async fn get_cluster_status(&self) -> Result<ClusterStatus, MetaClientError> {
        self.request(message::GetClusterStatus {}).await
    }

    #[async_backtrace::framed]
    pub async fn get_member_list(&self) -> Result<MemberListReply, MetaClientError> {
        self.request(message::GetMemberList {}).await
    }

    #[async_backtrace::framed]
    pub async fn get_client_info(&self) -> Result<ClientInfo, MetaClientError> {
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

/// Create a counter function for tracking in-flight requests.
fn request_inflight<RT: SpawnApi>() -> impl FnMut(i64) {
    |delta: i64| RT::ClientMetrics::request_inflight(delta)
}
