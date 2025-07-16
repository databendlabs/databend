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

use std::collections::BTreeSet;
use std::time::Duration;
use std::time::SystemTime;

use anyerror::AnyError;
use databend_common_base::base::tokio::sync::RwLockReadGuard;
use databend_common_meta_client::MetaGrpcReadReq;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_raft_store::sm_v003::SMV003;
use databend_common_meta_sled_store::openraft::ChangeMembers;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::node::Node;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::raft_types::ClientWriteError;
use databend_common_meta_types::raft_types::MembershipNode;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::RaftError;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::MetaDataError;
use databend_common_meta_types::MetaDataReadError;
use databend_common_meta_types::MetaOperationError;
use databend_common_metrics::count::Count;
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
use log::info;
use maplit::btreemap;
use maplit::btreeset;
use tonic::codegen::BoxStream;
use tonic::Status;

use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::message::ForwardResponse;
use crate::message::JoinRequest;
use crate::message::LeaveRequest;
use crate::meta_service::meta_node::MetaRaft;
use crate::meta_service::MetaNode;
use crate::metrics::server_metrics;
use crate::metrics::ProposalPending;
use crate::request_handling::Handler;
use crate::store::RaftStore;

/// The container of APIs of the leader in a meta service cluster.
///
/// A leader does not imply it is actually the leader granted by the cluster.
/// It just means it believes it is the leader and have not yet perceived there is other newer leader.
pub struct MetaLeader<'a> {
    sto: &'a RaftStore,
    raft: &'a MetaRaft,
}

#[async_trait::async_trait]
impl Handler<ForwardRequestBody> for MetaLeader<'_> {
    #[fastrace::trace]
    async fn handle(
        &self,
        req: ForwardRequest<ForwardRequestBody>,
    ) -> Result<ForwardResponse, MetaOperationError> {
        debug!(req :? =(&req), target = req.forward_to_leader; "handle_forwardable_req");

        match req.body {
            ForwardRequestBody::Ping => Ok(ForwardResponse::Pong),

            ForwardRequestBody::Join(join_req) => {
                self.join(join_req).await?;
                Ok(ForwardResponse::Join(()))
            }
            ForwardRequestBody::Leave(leave_req) => {
                self.leave(leave_req).await?;
                Ok(ForwardResponse::Leave(()))
            }
            ForwardRequestBody::Write(entry) => {
                let res = self.write(entry.clone()).await?;
                Ok(ForwardResponse::AppliedState(res))
            }

            ForwardRequestBody::GetKV(req) => {
                let sm = self.get_state_machine().await;
                let res = sm.kv_api().get_kv(&req.key).await.unwrap();
                Ok(ForwardResponse::GetKV(res))
            }
            ForwardRequestBody::MGetKV(req) => {
                let sm = self.get_state_machine().await;
                let res = sm.kv_api().mget_kv(&req.keys).await.unwrap();
                Ok(ForwardResponse::MGetKV(res))
            }
            ForwardRequestBody::ListKV(req) => {
                let sm = self.get_state_machine().await;
                let res = sm.kv_api().list_kv_collect(&req.prefix).await.unwrap();
                Ok(ForwardResponse::ListKV(res))
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<MetaGrpcReadReq> for MetaLeader<'_> {
    #[fastrace::trace]
    async fn handle(
        &self,
        req: ForwardRequest<MetaGrpcReadReq>,
    ) -> Result<BoxStream<StreamItem>, MetaOperationError> {
        debug!(req :? =(&req); "handle(MetaGrpcReadReq)");

        let sm = self.get_state_machine().await;
        let kv_api = sm.kv_api();

        match req.body {
            MetaGrpcReadReq::GetKV(req) => {
                // safe unwrap(): Infallible
                let got = kv_api.get_kv(&req.key).await.unwrap();

                let item = StreamItem::from((req.key.clone(), got));
                let strm = futures::stream::iter([Ok(item)]);

                Ok(strm.boxed())
            }

            MetaGrpcReadReq::MGetKV(req) => {
                // safe unwrap(): Infallible
                let values = kv_api.mget_kv(&req.keys).await.unwrap();

                let kv_iter = req
                    .keys
                    .clone()
                    .into_iter()
                    .zip(values)
                    .map(|(k, v)| Ok(StreamItem::from((k, v))));

                let strm = futures::stream::iter(kv_iter);

                Ok(strm.boxed())
            }

            MetaGrpcReadReq::ListKV(req) => {
                let strm =
                    kv_api.list_kv(&req.prefix).await.map_err(|e| {
                        MetaOperationError::DataError(MetaDataError::ReadError(
                            MetaDataReadError::new("list_kv", &req.prefix, &e),
                        ))
                    })?;

                let strm = strm.map_err(|e| Status::internal(e.to_string()));

                Ok(strm.boxed())
            }
        }
    }
}

impl<'a> MetaLeader<'a> {
    pub fn new(meta_node: &'a MetaNode) -> MetaLeader<'a> {
        MetaLeader {
            sto: &meta_node.raft_store,
            raft: &meta_node.raft,
        }
    }

    /// Join a new node to the cluster.
    ///
    /// - Adds the node to cluster as a non-voter persistently and starts replication.
    /// - Adds the node to membership to let it become a voter.
    ///
    /// If the node is already in cluster membership, it still returns Ok.
    #[fastrace::trace]
    pub async fn join(&self, req: JoinRequest) -> Result<(), RaftError<ClientWriteError>> {
        let node_id = req.node_id;
        let endpoint = req.endpoint;
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();

        let voters = membership.voter_ids().collect::<BTreeSet<_>>();

        if voters.contains(&node_id) {
            return Ok(());
        }

        let ent = LogEntry {
            txid: None,
            time_ms: None,
            cmd: Cmd::AddNode {
                node_id,
                node: Node::new(node_id, endpoint)
                    .with_grpc_advertise_address(req.grpc_api_advertise_address),
                overriding: false,
            },
        };
        self.write(ent).await?;

        self.raft
            .change_membership(
                ChangeMembers::AddVoters(btreemap! {node_id=>MembershipNode{}}),
                false,
            )
            .await?;
        Ok(())
    }

    /// A node leave the cluster.
    ///
    /// - Remove the node from membership.
    /// - Stop replication.
    /// - Remove the node from cluster.
    ///
    /// If the node is not in cluster membership, it still returns Ok.
    #[fastrace::trace]
    pub async fn leave(&self, req: LeaveRequest) -> Result<(), MetaOperationError> {
        let node_id = req.node_id;

        if node_id == self.sto.id {
            return Err(MetaOperationError::DataError(MetaDataError::ReadError(
                MetaDataReadError::new(
                    "leave",
                    format!("can not leave id={} via itself", node_id),
                    &AnyError::error("leave-via-self"),
                ),
            )));
        }

        let can_res = self
            .can_leave(node_id)
            .await
            .map_err(|e| MetaDataError::ReadError(MetaDataReadError::new("can_leave()", "", &e)))?;

        if let Err(e) = can_res {
            info!("no need to leave: {}", e);
            return Ok(());
        }

        // 1. Remove it from membership if needed.
        self.raft
            .change_membership(ChangeMembers::RemoveVoters(btreeset! {node_id}), false)
            .await?;

        // 2. Remove node info
        let ent = LogEntry {
            txid: None,
            time_ms: None,
            cmd: Cmd::RemoveNode { node_id },
        };
        self.write(ent).await?;

        Ok(())
    }

    /// Write a log through local raft node and return the states before and after applying the log.
    ///
    /// If the raft node is not a leader, it returns MetaRaftError::ForwardToLeader.
    #[fastrace::trace]
    pub async fn write(
        &self,
        mut entry: LogEntry,
    ) -> Result<AppliedState, RaftError<ClientWriteError>> {
        // Add consistent clock time to log entry.
        entry.time_ms = Some(since_epoch().as_millis() as u64);

        // report metrics
        let _guard = ProposalPending::guard();

        info!("write LogEntry: {}", entry);
        let write_res = self.raft.client_write(entry).await;

        match write_res {
            Ok(resp) => {
                info!(
                    "raft.client_write res ok: log_id: {}, data: {}, membership: {:?}",
                    resp.log_id, resp.data, resp.membership
                );
                Ok(resp.data)
            }
            Err(raft_err) => {
                server_metrics::incr_proposals_failed();
                info!("raft.client_write res err: {:?}", raft_err);
                Err(raft_err)
            }
        }
    }

    /// Check if a node is allowed to leave the cluster.
    ///
    /// A cluster must have at least one node in it.
    async fn can_leave(&self, id: NodeId) -> Result<Result<(), String>, MetaStorageError> {
        let membership = {
            let sm = self.get_state_machine().await;
            sm.sys_data_ref().last_membership_ref().membership().clone()
        };
        info!("check can_leave: id: {}, membership: {:?}", id, membership);

        let last_config = membership.get_joint_config().last().unwrap();

        if last_config.contains(&id) && last_config.len() == 1 {
            return Ok(Err(format!(
                "can not remove the last node: {:?}",
                last_config
            )));
        }

        Ok(Ok(()))
    }

    async fn get_state_machine(&self) -> RwLockReadGuard<'_, SMV003> {
        self.sto.state_machine.read().await
    }
}

fn since_epoch() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}
