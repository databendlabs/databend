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

use std::time::Duration;

use databend_common_base::base::escape_for_key;
use databend_common_base::base::unescape_for_key;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_store::MetaStore;
use databend_common_meta_types::protobuf::SeqV;
use databend_common_meta_types::txn_op_response::Response;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::NodeInfo;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnOpResponse;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;

use crate::warehouse::ClusterApi;

pub static CLUSTER_API_KEY_PREFIX: &str = "__fd_clusters_v5";

pub struct ClusterMgr {
    metastore: MetaStore,
    lift_time: Duration,
    node_key_prefix: String,
    meta_key_prefix: String,
}

impl ClusterMgr {
    pub fn create(metastore: MetaStore, tenant: &str, lift_time: Duration) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while cluster mgr create)",
            ));
        }

        Ok(ClusterMgr {
            metastore,
            lift_time,
            // all online node of tenant
            node_key_prefix: format!(
                "{}/{}/online_nodes",
                CLUSTER_API_KEY_PREFIX,
                escape_for_key(tenant)?
            ),
            // all computing cluster of tenant
            meta_key_prefix: format!(
                "{}/{}/online_clusters",
                CLUSTER_API_KEY_PREFIX,
                escape_for_key(tenant)?
            ),
        })
    }
}

fn map_condition(k: &str, seq: MatchSeq) -> TxnCondition {
    match seq {
        MatchSeq::Any => TxnCondition::match_seq(k.to_owned(), ConditionResult::Ge, 0),
        MatchSeq::GE(v) => TxnCondition::match_seq(k.to_owned(), ConditionResult::Ge, v),
        MatchSeq::Exact(v) => TxnCondition::match_seq(k.to_owned(), ConditionResult::Eq, v),
    }
}

fn map_response(res: Option<&TxnOpResponse>) -> Option<&SeqV> {
    res.and_then(|response| response.response.as_ref())
        .and_then(|response| match response {
            Response::Put(v) => v.prev_value.as_ref(),
            Response::Delete(v) => v.prev_value.as_ref(),
            _ => unreachable!(),
        })
}

impl ClusterMgr {
    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn upsert_node(&self, mut node: NodeInfo, seq: MatchSeq) -> Result<TxnReply> {
        let mut txn = TxnRequest::default();

        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(&node.id)?);
        txn.if_then.push(TxnOp::put_with_ttl(
            node_key.clone(),
            serde_json::to_vec(&node)?,
            Some(self.lift_time),
        ));

        if !node.cluster_id.is_empty() && !node.warehouse_id.is_empty() {
            let cluster_key = format!(
                "{}/{}/{}/{}",
                self.meta_key_prefix,
                escape_for_key(&node.warehouse_id)?,
                escape_for_key(&node.cluster_id)?,
                escape_for_key(&node.id)?
            );

            txn.condition.push(map_condition(&cluster_key, seq));
            node.cluster_id = String::new();
            node.warehouse_id = String::new();
            txn.if_then.push(TxnOp::put_with_ttl(
                cluster_key,
                serde_json::to_vec(&node)?,
                Some(self.lift_time),
            ));
        }

        Ok(self.metastore.transaction(txn).await?)
    }
}

#[async_trait::async_trait]
impl ClusterApi for ClusterMgr {
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn add_node(&self, node: NodeInfo) -> Result<()> {
        assert!(!node.cluster_id.is_empty());
        assert!(!node.warehouse_id.is_empty());

        let res = self.upsert_node(node.clone(), MatchSeq::Exact(0)).await?;

        if res.success && map_response(res.responses.get(1)).is_none() {
            return Ok(());
        }

        Err(ErrorCode::ClusterNodeAlreadyExists(format!(
            "Node with ID '{}' already exists in the cluster.",
            node.id
        )))
    }

    async fn heartbeat(&self, node: &NodeInfo) -> Result<()> {
        assert!(!node.cluster_id.is_empty());
        assert!(!node.warehouse_id.is_empty());

        // Update or insert the node with GE(0).
        let transition = self.upsert_node(node.clone(), MatchSeq::GE(0)).await?;

        match transition.success {
            true => Ok(()),
            false => Err(ErrorCode::MetaServiceError(format!(
                "Unexpected None result returned when upsert heartbeat node {}",
                node.id
            ))),
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_nodes(&self, warehouse: &str, cluster: &str) -> Result<Vec<NodeInfo>> {
        let cluster_key = format!(
            "{}/{}/{}",
            self.meta_key_prefix,
            escape_for_key(warehouse)?,
            escape_for_key(cluster)?
        );

        let values = self.metastore.prefix_list_kv(&cluster_key).await?;

        let mut nodes_info = Vec::with_capacity(values.len());
        for (node_key, value) in values {
            let mut node_info = serde_json::from_slice::<NodeInfo>(&value.data)?;

            node_info.id = unescape_for_key(&node_key[cluster_key.len() + 1..])?;
            node_info.cluster_id = cluster.to_string();
            node_info.warehouse_id = warehouse.to_string();
            nodes_info.push(node_info);
        }

        Ok(nodes_info)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn drop_node(&self, node_id: String) -> Result<()> {
        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(&node_id)?);

        if let Some(info) = self.metastore.get_kv(&node_key).await? {
            let node_info: NodeInfo = serde_json::from_slice(&info.data)?;

            let mut txn = TxnRequest::default();

            txn.if_then.push(TxnOp::delete(node_key));

            if !node_info.cluster_id.is_empty() && !node_info.warehouse_id.is_empty() {
                txn.if_then.push(TxnOp::delete(format!(
                    "{}/{}/{}/{}",
                    self.meta_key_prefix,
                    escape_for_key(&node_info.warehouse_id)?,
                    escape_for_key(&node_info.cluster_id)?,
                    escape_for_key(&node_info.id)?
                )));
            }

            let res = self.metastore.transaction(txn).await?;

            if res.success
                && map_response(res.responses.first()).is_some()
                && map_response(res.responses.last()).is_some()
            {
                return Ok(());
            }
        }

        Err(ErrorCode::ClusterUnknownNode(format!(
            "Node with ID '{}' does not exist in the cluster.",
            node_id
        )))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_local_addr(&self) -> Result<Option<String>> {
        Ok(self.metastore.get_local_addr().await?)
    }
}
