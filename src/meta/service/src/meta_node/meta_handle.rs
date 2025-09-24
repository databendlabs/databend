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

use std::future;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::BuildInfoRef;
use databend_common_base::future::TimedFutureExt;
use databend_common_base::runtime::Runtime;
use databend_common_meta_client::MetaGrpcReadReq;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_types::protobuf::MemberListRequest;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::raft_types::Fatal;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::RaftMetrics;
use databend_common_meta_types::raft_types::Wait;
use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::MetaAPIError;
use databend_common_meta_types::Node;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;
use futures::stream::BoxStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tonic::Status;

use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::meta_node::errors::MetaNodeStopped;
use crate::meta_node::meta_node_status::MetaNodeStatus;
use crate::meta_service::MetaNode;

pub type BoxFuture<T = ()> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
pub type MetaFnOnce<T> = Box<dyn FnOnce(Arc<MetaNode>) -> BoxFuture<T> + Send + 'static>;

/// A handle to talk to MetaNode in another runtime.
#[derive(Clone)]
pub struct MetaHandle {
    pub id: NodeId,
    pub version: BuildInfoRef,
    tx: mpsc::Sender<MetaFnOnce<()>>,
    /// The runtime containing the meta node worker.
    ///
    /// When all handles are dropped, the runtime will be dropped
    _rt: Arc<Runtime>,
}

impl MetaHandle {
    pub fn new(
        id: NodeId,
        version: BuildInfoRef,
        tx: mpsc::Sender<MetaFnOnce<()>>,
        rt: Arc<Runtime>,
    ) -> Self {
        MetaHandle {
            id,
            version,
            tx,
            _rt: rt,
        }
    }

    /// Run a function in meta-node
    pub async fn request<T>(
        &self,
        f: impl FnOnce(Arc<MetaNode>) -> BoxFuture<T> + Send + 'static,
    ) -> Result<T, MetaNodeStopped>
    where
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        let box_fn = Box::new(move |meta_node: Arc<MetaNode>| {
            let fu = async move {
                let res = f(meta_node).await;
                tx.send(res).ok();
            };
            let fu: Pin<Box<dyn Future<Output = ()> + Send + 'static>> = Box::pin(fu);
            fu
        });

        self.tx
            .send(box_fn)
            .await
            .map_err(|_e| MetaNodeStopped::new().with_context("sending request"))?;

        let got = rx
            .await
            .map_err(|_| MetaNodeStopped::new().with_context("receiving response"))?;

        Ok(got)
    }

    pub async fn get_id(&self) -> Result<NodeId, MetaNodeStopped> {
        self.request(|meta_node| Box::pin(future::ready(meta_node.raft_store.id)))
            .await
    }

    pub async fn get_meta_node(&self) -> Result<Arc<MetaNode>, MetaNodeStopped> {
        self.request(|meta_node| Box::pin(future::ready(meta_node.clone())))
            .await
    }

    pub async fn handle_upsert_kv(
        &self,
        upsert: UpsertKV,
    ) -> Result<Result<UpsertKVReply, MetaAPIError>, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move {
                meta_node
                    .kv_api()
                    .upsert_kv(upsert.clone())
                    .log_elapsed_info(format!("UpsertKV: {:?}", upsert))
                    .await
            };

            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_kv_read_v1(
        &self,
        req: MetaGrpcReadReq,
    ) -> Result<
        Result<
            (
                Option<Endpoint>,
                BoxStream<'static, Result<StreamItem, Status>>,
            ),
            MetaAPIError,
        >,
        MetaNodeStopped,
    > {
        self.request(move |meta_node| {
            let req = ForwardRequest::new(1, req);

            let fu = async move {
                meta_node
                    .handle_forwardable_request::<MetaGrpcReadReq>(req.clone())
                    .log_elapsed_info(format!("ReadRequest: {:?}", req))
                    .await
            };

            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_transaction(
        &self,
        txn: TxnRequest,
    ) -> Result<Result<(Option<Endpoint>, TxnReply), MetaAPIError>, MetaNodeStopped> {
        self.request(move |meta_node| {
            let ent = LogEntry::new(Cmd::Transaction(txn.clone()));
            let forward_req = ForwardRequest::new(1, ForwardRequestBody::Write(ent));

            let fu = async move {
                let res = meta_node
                    .handle_forwardable_request(forward_req)
                    .log_elapsed_info(format!("TxnRequest: {:?}", txn))
                    .await;

                res.map(|(ep, forward_resp)| {
                    let applied_state: AppliedState =
                        forward_resp.try_into().expect("expect AppliedState");

                    let txn_reply: TxnReply = applied_state.try_into().expect("expect TxnReply");

                    (ep, txn_reply)
                })
            };

            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_write(
        &self,
        entry: LogEntry,
    ) -> Result<Result<AppliedState, MetaAPIError>, MetaNodeStopped> {
        let forward_req = ForwardRequest::new(1, ForwardRequestBody::Write(entry.clone()));

        let res = self
            .request(move |meta_node| {
                let fu = async move {
                    meta_node
                        .handle_forwardable_request(forward_req)
                        .log_elapsed_info(format!("WriteRequest: {:?}", entry))
                        .await
                };
                Box::pin(fu)
            })
            .await?;

        let res: Result<AppliedState, _> =
            res.map(|(_ep, forward_resp)| forward_resp.try_into().expect("expect AppliedState"));
        Ok(res)
    }

    pub async fn handle_export(
        &self,
    ) -> Result<BoxStream<'static, Result<String, io::Error>>, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move { meta_node.raft_store.clone().export() };

            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_watch(
        &self,
        watch: WatchRequest,
    ) -> Result<Result<BoxStream<'static, Result<WatchResponse, Status>>, Status>, MetaNodeStopped>
    {
        self.request(move |meta_node| {
            let fu = async move { meta_node.handle_watch(watch).await };

            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_member_list(
        &self,
        _request: MemberListRequest,
    ) -> Result<Vec<String>, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move {
                //
                meta_node.get_grpc_advertise_addrs().await
            };

            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_get_sys_data(&self) -> Result<SysData, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move { meta_node.raft_store.get_sm_v003().sys_data() };
            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_get_node(&self, node_id: NodeId) -> Result<Option<Node>, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move { meta_node.get_node(&node_id).await };
            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_get_nodes(&self) -> Result<Vec<Node>, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move { meta_node.get_nodes().await };
            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_get_status(&self) -> Result<MetaNodeStatus, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move { meta_node.get_status().await };

            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_raft_metrics(
        &self,
    ) -> Result<watch::Receiver<RaftMetrics>, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move { meta_node.raft.metrics() };

            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_get_leader(&self) -> Result<Option<NodeId>, MetaNodeStopped> {
        let res = self
            .request(move |meta_node| {
                let fu = async move {
                    // If no metrics channel is closed, it means MetaNode is stopped.
                    meta_node
                        .get_leader()
                        .await
                        .map_err(|_| MetaNodeStopped::new().with_context("get_leader"))
                };

                Box::pin(fu)
            })
            .await?;
        res
    }

    pub async fn handle_raft_metrics_wait(
        &self,
        timeout: Option<Duration>,
    ) -> Result<Wait, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move { meta_node.raft.wait(timeout) };

            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_trigger_snapshot(&self) -> Result<Result<(), Fatal>, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move { meta_node.raft.trigger().snapshot().await };

            Box::pin(fu)
        })
        .await
    }

    pub async fn handle_trigger_transfer_leader(
        &self,
        to: NodeId,
    ) -> Result<Result<(), Fatal>, MetaNodeStopped> {
        self.request(move |meta_node| {
            let fu = async move { meta_node.raft.trigger().transfer_leader(to).await };

            Box::pin(fu)
        })
        .await
    }
}
