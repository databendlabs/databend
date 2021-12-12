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

use async_raft::error::ResponseError;
use async_raft::raft::ClientWriteRequest;
use async_raft::ChangeConfigError;
use async_raft::ClientWriteError;
use common_meta_api::MetaApi;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_types::Cmd;
use common_meta_types::LogEntry;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_tracing::tracing;

use crate::errors::ForwardToLeader;
use crate::errors::InvalidMembership;
use crate::errors::MetaError;
use crate::meta_service::message::AdminResponse;
use crate::meta_service::message::ForwardRequest;
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn handle_admin_req(&self, req: ForwardRequest) -> Result<AdminResponse, MetaError> {
        match req.body {
            ForwardRequestBody::Join(join_req) => {
                self.join(join_req).await?;
                Ok(AdminResponse::Join(()))
            }
            ForwardRequestBody::Write(entry) => {
                let res = self.write(entry).await?;
                Ok(AdminResponse::AppliedState(res))
            }

            ForwardRequestBody::ListDatabase(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.list_databases(req).await?;
                Ok(AdminResponse::ListDatabase(res))
            }

            ForwardRequestBody::GetDatabase(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.get_database(req).await?;
                Ok(AdminResponse::DatabaseInfo(res))
            }
            ForwardRequestBody::ListTable(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.list_tables(req).await?;
                Ok(AdminResponse::ListTable(res))
            }
            ForwardRequestBody::GetTable(req) => {
                let sm = self.meta_node.get_state_machine().await;
                let res = sm.get_table(req).await?;
                Ok(AdminResponse::TableInfo(res))
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
        let metrics = self.meta_node.metrics_rx.borrow().clone();
        let mut membership = metrics.membership_config.members.clone();

        if membership.contains(&node_id) {
            return Ok(());
        }

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
        let res = self.meta_node.raft.change_membership(membership).await;

        let err = match res {
            Ok(_) => return Ok(()),
            Err(e) => e,
        };

        match err {
            ResponseError::ChangeConfig(e) => match e {
                // TODO(xp): enable MetaNode::RaftError when RaftError impl Serialized
                ChangeConfigError::RaftError(raft_error) => {
                    Err(MetaError::UnknownError(raft_error.to_string()))
                }
                ChangeConfigError::ConfigChangeInProgress => {
                    Err(MetaError::MembershipChangeInProgress)
                }
                ChangeConfigError::InoperableConfig => {
                    Err(MetaError::InvalidMembership(InvalidMembership {}))
                }
                ChangeConfigError::NodeNotLeader(leader) => {
                    Err(MetaError::ForwardToLeader(ForwardToLeader { leader }))
                }
                ChangeConfigError::Noop => Ok(()),
                _ => Err(MetaError::UnknownError("uncovered error".to_string())),
            },
            // TODO(xp): enable MetaNode::RaftError when RaftError impl Serialized
            ResponseError::Raft(raft_error) => Err(MetaError::UnknownError(raft_error.to_string())),
            _ => Err(MetaError::UnknownError("uncovered error".to_string())),
        }
    }

    /// Write a log through local raft node and return the states before and after applying the log.
    ///
    /// If the raft node is not a leader, it returns MetaError::ForwardToLeader.
    /// If the leadership is lost during writing the log, it returns an UnknownError.
    /// TODO(xp): elaborate the UnknownError, e.g. LeaderLostError
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn write(&self, entry: LogEntry) -> Result<AppliedState, MetaError> {
        let write_rst = self
            .meta_node
            .raft
            .client_write(ClientWriteRequest::new(entry))
            .await;

        tracing::debug!("raft.client_write rst: {:?}", write_rst);

        match write_rst {
            Ok(resp) => Ok(resp.data),
            Err(cli_write_err) => match cli_write_err {
                // fatal error
                ClientWriteError::RaftError(raft_err) => {
                    Err(MetaError::UnknownError(raft_err.to_string()))
                }
                // retryable error
                ClientWriteError::ForwardToLeader(_, leader) => {
                    Err(MetaError::ForwardToLeader(ForwardToLeader { leader }))
                }
            },
        }
    }
}
