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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_types::Change;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::NodeInfo;

/// Databend-query cluster management API
#[async_trait::async_trait]
pub trait ClusterApi: Sync + Send {
    /// Add or update a node info to /tenant/cluster_id/node-name.
    ///
    /// - To update, use `SeqMatch::GE(1)` to match any present record.
    /// - To add, use `SeqMatch::Exact(0)` to match no present record.
    async fn upsert_node(&self, node: NodeInfo, seq: MatchSeq) -> Result<Change<Vec<u8>>>;

    /// Get the tenant's cluster all nodes.
    async fn get_nodes(&self) -> Result<Vec<NodeInfo>>;

    /// Drop the tenant's cluster one node by node.id.
    async fn drop_node(&self, node_id: String, seq: MatchSeq) -> Result<()>;

    async fn get_local_addr(&self) -> Result<Option<String>>;

    /// Add a new node.
    async fn add_node(&self, node: NodeInfo) -> Result<u64> {
        let res = self.upsert_node(node.clone(), MatchSeq::Exact(0)).await?;

        let res_seq = res.added_seq_or_else(|_v| {
            ErrorCode::ClusterNodeAlreadyExists(format!(
                "Node with ID '{}' already exists in the cluster.",
                node.id
            ))
        })?;

        Ok(res_seq)
    }

    /// Keep the tenant's cluster node alive.
    async fn heartbeat(&self, node: &NodeInfo) -> Result<u64> {
        // Update or insert the node with GE(0).
        let transition = self.upsert_node(node.clone(), MatchSeq::GE(0)).await?;

        let Some(res) = transition.result else {
            return Err(ErrorCode::MetaServiceError(format!(
                "Unexpected None result returned when upsert heartbeat node {}",
                node.id
            )));
        };

        Ok(res.seq)
    }
}
