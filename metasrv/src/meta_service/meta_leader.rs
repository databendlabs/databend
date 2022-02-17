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

use std::collections::BTreeSet;

use common_meta_api::KVApi;
use common_meta_api::MetaApi;
use common_meta_sled_store::openraft;
use common_meta_sled_store::openraft::error::ClientWriteError;
use common_meta_sled_store::openraft::raft::EntryPayload;
use common_meta_types::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::ForwardRequest;
use common_meta_types::ForwardResponse;
use common_meta_types::ForwardToLeader;
use common_meta_types::LogEntry;
use common_meta_types::MetaError;
use common_meta_types::MetaRaftError;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_tracing::tracing;
use openraft::raft::ClientWriteRequest;

use crate::meta_service::ForwardRequestBody;
use crate::meta_service::JoinRequest;
use crate::meta_service::MetaNode;

/// The container of APIs of a metasrv leader in a metasrv cluster.
///
/// A meta leader does not imply it is actually the leader granted by the cluster.
/// It just means it believes it is the leader an have not yet perceived there is other newer leader.
pub struct MetaLeader<'a> {
    meta_node: &'a MetaNode,
}

impl<'a> MetaLeader<'a> {
    pub fn new(meta_node: &'a MetaNode) -> MetaLeader {
        MetaLeader { meta_node }
    }

    #[tracing::instrument(level = "debug", skip(self, req), fields(target=%req.forward_to_leader))]
    pub async fn handle_forwardable_req(
        &self,
        req: ForwardRequest,
    ) -> Result<ForwardResponse, MetaError> {
        tracing::debug!("handle_forwardable_req: {:?}", req);

        match req.body {
            ForwardRequestBody::Join(join_req) => {
                self.join(join_req).await?;
                Ok(ForwardResponse::Join(()))
            }
            ForwardRequestBody::Write(entry) => {
                let res = self.write(entry).await?;
                Ok(ForwardResponse::AppliedState(res))
            }

            ForwardRequestBody::ListDatabase(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.list_databases(req).await?;
                Ok(ForwardResponse::ListDatabase(res))
            }

            ForwardRequestBody::GetDatabase(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.get_database(req).await?;
                Ok(ForwardResponse::DatabaseInfo(res))
            }
            ForwardRequestBody::ListTable(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.list_tables(req).await?;
                Ok(ForwardResponse::ListTable(res))
            }
            ForwardRequestBody::GetTable(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.get_table(req).await?;
                Ok(ForwardResponse::TableInfo(res))
            }
            ForwardRequestBody::GetKV(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.get_kv(&req.key).await?;
                Ok(ForwardResponse::GetKV(res))
            }
            ForwardRequestBody::MGetKV(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.mget_kv(&req.keys).await?;
                Ok(ForwardResponse::MGetKV(res))
            }
            ForwardRequestBody::ListKV(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.prefix_list_kv(&req.prefix).await?;
                Ok(ForwardResponse::ListKV(res))
            }
        }
    }

    /// Join a new node to the cluster.
    ///
    /// - Adds the node to cluster as a non-voter persistently and starts replication.
    /// - Adds the node to membership to let it become a voter.
    ///
    /// If the node is already in cluster membership, it still returns Ok.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn join(&self, req: JoinRequest) -> Result<(), MetaError> {
        let node_id = req.node_id;
        let addr = req.address;
        let metrics = self.meta_node.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership.clone();

        if membership.contains(&node_id) {
            return Ok(());
        }

        // TODO(xp): deal with joint config
        assert!(membership.get_ith_config(1).is_none());

        // safe unwrap: if the first config is None, panic is the expected behavior here.
        let mut membership = membership.get_ith_config(0).unwrap().clone();

        membership.insert(node_id);

        let ent = LogEntry {
            txid: None,
            cmd: Cmd::AddNode {
                node_id,
                node: Node {
                    name: "".to_string(),
                    address: addr,
                },
            },
        };

        self.write(ent.clone()).await?;

        self.change_membership(membership).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn change_membership(&self, membership: BTreeSet<NodeId>) -> Result<(), MetaError> {
        let res = self
            .meta_node
            .raft
            .change_membership(membership, true)
            .await;

        let err = match res {
            Ok(_) => return Ok(()),
            Err(e) => e,
        };

        match err {
            ClientWriteError::ChangeMembershipError(e) => {
                Err(MetaRaftError::ChangeMembershipError(e).into())
            }
            // TODO(xp): enable MetaNode::RaftError when RaftError impl Serialized
            ClientWriteError::Fatal(fatal) => Err(MetaRaftError::RaftFatal(fatal).into()),
            ClientWriteError::ForwardToLeader(to_leader) => {
                Err(MetaRaftError::ForwardToLeader(ForwardToLeader {
                    leader_id: to_leader.leader_id,
                })
                .into())
            }
        }
    }

    /// Write a log through local raft node and return the states before and after applying the log.
    ///
    /// If the raft node is not a leader, it returns MetaRaftError::ForwardToLeader.
    /// If the leadership is lost during writing the log, it returns an UnknownError.
    /// TODO(xp): elaborate the UnknownError, e.g. LeaderLostError
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn write(&self, entry: LogEntry) -> Result<AppliedState, MetaError> {
        let write_rst = self
            .meta_node
            .raft
            .client_write(ClientWriteRequest::new(EntryPayload::Normal(entry)))
            .await;

        tracing::debug!("raft.client_write rst: {:?}", write_rst);

        match write_rst {
            Ok(resp) => {
                let data = resp.data;
                match data {
                    AppliedState::AppError(ae) => Err(MetaError::from(ae)),
                    _ => Ok(data),
                }
            }

            Err(cli_write_err) => match cli_write_err {
                // fatal error
                ClientWriteError::Fatal(fatal) => Err(MetaRaftError::RaftFatal(fatal).into()),
                // retryable error
                ClientWriteError::ForwardToLeader(to_leader) => {
                    Err(MetaRaftError::ForwardToLeader(ForwardToLeader {
                        leader_id: to_leader.leader_id,
                    })
                    .into())
                }
                ClientWriteError::ChangeMembershipError(_) => {
                    unreachable!("there should not be a ChangeMembershipError for client_write")
                }
            },
        }
    }
}
