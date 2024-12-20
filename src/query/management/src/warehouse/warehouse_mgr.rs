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

use std::collections::HashMap;
use std::time::Duration;

use databend_common_base::base::escape_for_key;
use databend_common_base::base::unescape_for_key;
use databend_common_base::base::GlobalUniqName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_store::MetaStore;
use databend_common_meta_types::txn_op_response::Response;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::NodeInfo;
use databend_common_meta_types::NodeType;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnGetResponse;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnOpResponse;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;

use crate::warehouse::warehouse_api::SelectedNode;
use crate::warehouse::warehouse_api::SelectedNodes;
use crate::warehouse::warehouse_api::SystemManagedCluster;
use crate::warehouse::warehouse_api::SystemManagedInfo;
use crate::warehouse::warehouse_api::WarehouseInfo;
use crate::warehouse::WarehouseApi;

pub static WAREHOUSE_API_KEY_PREFIX: &str = "__fd_clusters_v5";
pub static WAREHOUSE_META_KEY_PREFIX: &str = "__fd_warehouses";

pub struct WarehouseMgr {
    metastore: MetaStore,
    lift_time: Duration,
    node_key_prefix: String,
    meta_key_prefix: String,
    warehouse_key_prefix: String,
}

impl WarehouseMgr {
    pub fn create(metastore: MetaStore, tenant: &str, lift_time: Duration) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while cluster mgr create)",
            ));
        }

        Ok(WarehouseMgr {
            metastore,
            lift_time,
            // warehouses:
            // all online node of tenant
            node_key_prefix: format!(
                "{}/{}/online_nodes",
                WAREHOUSE_API_KEY_PREFIX,
                escape_for_key(tenant)?
            ),
            // all computing cluster of tenant
            meta_key_prefix: format!(
                "{}/{}/online_clusters",
                WAREHOUSE_API_KEY_PREFIX,
                escape_for_key(tenant)?
            ),
            // all warehouse of tenant(required compatible with all versions)
            warehouse_key_prefix: format!(
                "{}/v1/{}",
                WAREHOUSE_META_KEY_PREFIX,
                escape_for_key(tenant)?,
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

struct ConsistentNodeInfo {
    node_seq: u64,
    cluster_seq: u64,
    node_info: NodeInfo,
}

struct ConsistentWarehouseInfo {
    info_seq: u64,
    warehouse_info: WarehouseInfo,
    consistent_nodes: Vec<ConsistentNodeInfo>,
}

impl WarehouseMgr {
    fn node_key(&self, node: &NodeInfo) -> Result<String> {
        Ok(format!(
            "{}/{}",
            self.node_key_prefix,
            escape_for_key(&node.id)?
        ))
    }

    fn cluster_key(&self, node: &NodeInfo) -> Result<String> {
        Ok(format!(
            "{}/{}/{}/{}",
            self.meta_key_prefix,
            escape_for_key(&node.warehouse_id)?,
            escape_for_key(&node.cluster_id)?,
            escape_for_key(&node.id)?
        ))
    }

    async fn upsert_self_managed(&self, mut node: NodeInfo, seq: MatchSeq) -> Result<TxnReply> {
        if node.warehouse_id.is_empty() || node.cluster_id.is_empty() {
            return Err(ErrorCode::InvalidWarehouse(
                "The warehouse_id and cluster_id for self managed node must not be empty.",
            ));
        }

        let mut txn = TxnRequest::default();
        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(&node.id)?);

        txn.condition.push(map_condition(&node_key, seq));

        txn.if_then.push(TxnOp::put_with_ttl(
            node_key.clone(),
            serde_json::to_vec(&node)?,
            Some(self.lift_time),
        ));

        let warehouse_node_key = self.cluster_key(&node)?;
        let warehouse_info = WarehouseInfo::SelfManaged(node.warehouse_id.clone());
        let warehouse_info_key = format!(
            "{}/{}",
            self.warehouse_key_prefix,
            escape_for_key(&node.warehouse_id)?
        );

        node.cluster_id = String::new();
        node.warehouse_id = String::new();
        txn.if_then.push(TxnOp::put_with_ttl(
            warehouse_node_key,
            serde_json::to_vec(&node)?,
            Some(self.lift_time),
        ));

        // upsert warehouse info if self-managed.

        txn.if_then.push(TxnOp::put_with_ttl(
            warehouse_info_key.clone(),
            serde_json::to_vec(&warehouse_info)?,
            Some(self.lift_time),
        ));

        txn.if_then.push(TxnOp::get(node_key.clone()));
        txn.else_then.push(TxnOp::get(node_key.clone()));

        if seq != MatchSeq::Exact(0) {
            return Ok(self.metastore.transaction(txn).await?);
        }

        let mut exact_seq = 0;
        let mut retry_count = 0;

        loop {
            let mut warehouse_txn = txn.clone();

            // insert if warehouse info is not exists or SelfManaged
            warehouse_txn
                .condition
                .push(TxnCondition::eq_seq(warehouse_info_key.clone(), exact_seq));
            warehouse_txn
                .else_then
                .push(TxnOp::get(warehouse_info_key.clone()));

            return match self.metastore.transaction(warehouse_txn).await? {
                mut response if !response.success => {
                    return match response.responses.pop().and_then(|x| x.response) {
                        Some(Response::Get(data)) => match data.value {
                            None => Ok(response),
                            Some(value) if value.seq == 0 => Ok(response),
                            Some(value) => match serde_json::from_slice(&value.data)? {
                                WarehouseInfo::SystemManaged(_) => {
                                    Err(ErrorCode::WarehouseAlreadyExists(
                                        "Already exists same name system-managed warehouse.",
                                    ))
                                }
                                WarehouseInfo::SelfManaged(_) => match response.responses.first() {
                                    // already exists node.
                                    Some(TxnOpResponse {
                                        response:
                                            Some(Response::Get(TxnGetResponse {
                                                value: Some(value),
                                                ..
                                            })),
                                    }) if value.seq != 0 => Ok(response),
                                    _ => {
                                        log::info!("Self-managed warehouse has already been created by other nodes; attempt to join it. Retry count: {}", retry_count);
                                        retry_count += 1;
                                        exact_seq = value.seq;
                                        continue;
                                    }
                                },
                            },
                        },
                        _ => Ok(response),
                    };
                }
                response => Ok(response),
            };
        }
    }

    async fn upsert_system_managed(&self, mut node: NodeInfo, seq: MatchSeq) -> Result<TxnReply> {
        let mut txn = TxnRequest::default();
        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(&node.id)?);

        txn.condition.push(map_condition(&node_key, seq));

        txn.if_then.push(TxnOp::put_with_ttl(
            node_key.clone(),
            serde_json::to_vec(&node)?,
            Some(self.lift_time),
        ));

        // If the warehouse has already been assigned.
        if !node.cluster_id.is_empty() && !node.warehouse_id.is_empty() {
            let cluster_key = format!(
                "{}/{}/{}/{}",
                self.meta_key_prefix,
                escape_for_key(&node.warehouse_id)?,
                escape_for_key(&node.cluster_id)?,
                escape_for_key(&node.id)?
            );

            node.cluster_id = String::new();
            node.warehouse_id = String::new();
            txn.if_then.push(TxnOp::put_with_ttl(
                cluster_key,
                serde_json::to_vec(&node)?,
                Some(self.lift_time),
            ));
        }

        txn.if_then.push(TxnOp::get(node_key.clone()));
        txn.else_then.push(TxnOp::get(node_key.clone()));
        Ok(self.metastore.transaction(txn).await?)
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn upsert_node(&self, node: NodeInfo, seq: MatchSeq) -> Result<TxnReply> {
        match node.node_type {
            NodeType::SelfManaged => self.upsert_self_managed(node, seq).await,
            NodeType::SystemManaged => self.upsert_system_managed(node, seq).await,
        }
    }

    async fn leave_cluster(&self, node_info: &mut NodeInfo, seq: u64) -> Result<u64> {
        let mut cluster_id = String::new();
        let mut warehouse_id = String::new();

        std::mem::swap(&mut node_info.cluster_id, &mut cluster_id);
        std::mem::swap(&mut node_info.warehouse_id, &mut warehouse_id);

        let upsert_node = self.upsert_node(node_info.clone(), MatchSeq::Exact(seq));
        match upsert_node.await {
            Err(err) => {
                // rollback
                std::mem::swap(&mut node_info.cluster_id, &mut cluster_id);
                std::mem::swap(&mut node_info.warehouse_id, &mut warehouse_id);
                Err(err)
            }
            Ok(response) if !response.success => {
                // rollback
                std::mem::swap(&mut node_info.cluster_id, &mut cluster_id);
                std::mem::swap(&mut node_info.warehouse_id, &mut warehouse_id);
                Ok(seq)
            }
            Ok(response) => match response.responses.last() {
                Some(TxnOpResponse {
                    response: Some(Response::Get(TxnGetResponse { value: Some(v), .. })),
                }) => Ok(v.seq),
                _ => Err(ErrorCode::MetaServiceError("Meta insert failure.")),
            },
        }
    }

    async fn join_cluster(&self, node_info: &mut NodeInfo, seq: u64) -> Result<u64> {
        let upsert_node = self.upsert_node(node_info.clone(), MatchSeq::Exact(seq));

        match upsert_node.await {
            Err(err) => {
                // rollback
                // std::mem::swap(&mut node_info.cluster_id, &mut cluster_id);
                // std::mem::swap(&mut node_info.warehouse_id, &mut warehouse_id);
                Err(err)
            }
            Ok(response) if !response.success => {
                // rollback
                // std::mem::swap(&mut node_info.cluster_id, &mut cluster_id);
                // std::mem::swap(&mut node_info.warehouse_id, &mut warehouse_id);
                Ok(seq)
            }
            Ok(response) => match response.responses.last() {
                Some(TxnOpResponse {
                    response: Some(Response::Get(TxnGetResponse { value: Some(v), .. })),
                }) => Ok(v.seq),
                _ => Err(ErrorCode::MetaServiceError("Meta insert failure.")),
            },
        }
    }

    async fn resolve_conflicts(&self, reply: TxnReply, node: &mut NodeInfo) -> Result<u64> {
        match reply.responses.first() {
            None => self.leave_cluster(node, 0).await,
            Some(TxnOpResponse {
                response: Some(Response::Get(res)),
            }) => match &res.value {
                None => self.leave_cluster(node, 0).await,
                Some(value) => {
                    let node_info = serde_json::from_slice::<NodeInfo>(&value.data)?;

                    // Removed this node from the cluster in other nodes
                    if !node.cluster_id.is_empty()
                        && !node.warehouse_id.is_empty()
                        && node_info.cluster_id.is_empty()
                        && node_info.warehouse_id.is_empty()
                    {
                        return self.leave_cluster(node, value.seq).await;
                    }

                    // Added this node to the cluster in other nodes
                    node.cluster_id = node_info.cluster_id;
                    node.warehouse_id = node_info.warehouse_id;
                    self.join_cluster(node, value.seq).await
                }
            },
            _ => Err(ErrorCode::Internal("Miss type while in meta response")),
        }
    }

    async fn consistent_warehouse_info(&self, id: &str) -> Result<ConsistentWarehouseInfo> {
        let warehouse_key = format!("{}/{}", self.warehouse_key_prefix, escape_for_key(id)?);

        let nodes_prefix = format!("{}/{}", self.meta_key_prefix, escape_for_key(id)?);

        'retry: for _idx in 0..64 {
            let Some(before_info) = self.metastore.get_kv(&warehouse_key).await? else {
                return Err(ErrorCode::UnknownWarehouse(format!(
                    "Unknown warehouse or self managed warehouse {:?}",
                    id
                )));
            };

            let values = self.metastore.prefix_list_kv(&nodes_prefix).await?;

            let mut after_txn = TxnRequest::default();
            let mut cluster_node_seq = Vec::with_capacity(values.len());

            for (node_key, value) in values {
                let suffix = &node_key[nodes_prefix.len() + 1..];

                if let Some((_cluster, node)) = suffix.split_once('/') {
                    let node_key = format!("{}/{}", self.node_key_prefix, node);
                    after_txn.if_then.push(TxnOp::get(node_key));
                    cluster_node_seq.push(value.seq);
                    continue;
                }

                return Err(ErrorCode::InvalidWarehouse(format!(
                    "Node key is invalid {:?}",
                    node_key
                )));
            }

            let condition = map_condition(&warehouse_key, MatchSeq::Exact(before_info.seq));
            after_txn.condition.push(condition);

            match self.metastore.transaction(after_txn).await? {
                response if response.success => {
                    let mut consistent_nodes = Vec::with_capacity(response.responses.len());
                    for (idx, response) in response.responses.into_iter().enumerate() {
                        match response.response {
                            // TODO: maybe ignore none(not need retry)
                            Some(Response::Get(response)) => match response.value {
                                Some(value) => {
                                    let node_info =
                                        serde_json::from_slice::<NodeInfo>(&value.data)?;

                                    assert_eq!(node_info.warehouse_id, id);
                                    assert!(!node_info.cluster_id.is_empty());

                                    consistent_nodes.push(ConsistentNodeInfo {
                                        node_seq: value.seq,
                                        cluster_seq: cluster_node_seq[idx],
                                        node_info,
                                    });
                                }
                                _ => {
                                    continue 'retry;
                                }
                            },
                            _ => {
                                continue 'retry;
                            }
                        }
                    }

                    if consistent_nodes.len() == cluster_node_seq.len() {
                        return Ok(ConsistentWarehouseInfo {
                            info_seq: before_info.seq,
                            warehouse_info: serde_json::from_slice(&before_info.data)?,
                            consistent_nodes,
                        });
                    }
                }
                _ => {
                    continue 'retry;
                }
            }
        }

        Err(ErrorCode::Internal(
            "Get consistent warehouse info failure(tried 64 times)",
        ))
    }

    async fn shutdown_self_managed_node(&self, node_info: &NodeInfo) -> Result<()> {
        for _idx in 0..10 {
            let consistent_info = self
                .consistent_warehouse_info(&node_info.warehouse_id)
                .await?;

            let mut txn = TxnRequest::default();

            let node_key = self.node_key(node_info)?;
            let cluster_key = self.cluster_key(node_info)?;

            if consistent_info.consistent_nodes.len() == 1
                && consistent_info.consistent_nodes[0].node_info.id == node_info.id
            {
                let warehouse_key = format!(
                    "{}/{}",
                    self.warehouse_key_prefix,
                    escape_for_key(&node_info.warehouse_id)?
                );

                txn.condition.push(map_condition(
                    &warehouse_key,
                    MatchSeq::Exact(consistent_info.info_seq),
                ));
                txn.if_then.push(TxnOp::delete(warehouse_key));

                for consistent_node in consistent_info.consistent_nodes {
                    txn.condition.push(map_condition(
                        &node_key,
                        MatchSeq::Exact(consistent_node.node_seq),
                    ));
                    txn.condition.push(map_condition(
                        &cluster_key,
                        MatchSeq::Exact(consistent_node.cluster_seq),
                    ));
                }
            }

            txn.if_then.push(TxnOp::delete(node_key));
            txn.if_then.push(TxnOp::delete(cluster_key));

            if self.metastore.transaction(txn).await?.success {
                return Ok(());
            }
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times).",
        ))
    }
}

#[async_trait::async_trait]
impl WarehouseApi for WarehouseMgr {
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn start_node(&self, node: NodeInfo) -> Result<u64> {
        let res = self.upsert_node(node.clone(), MatchSeq::Exact(0)).await?;

        if res.success {
            let Some(Response::Get(response)) =
                res.responses.last().and_then(|x| x.response.as_ref())
            else {
                return Err(ErrorCode::Internal("Unknown get response"));
            };

            let Some(node_info) = &response.value else {
                return Err(ErrorCode::MetaServiceError("Add node info failure."));
            };

            return Ok(node_info.seq);
        }

        Err(ErrorCode::ClusterNodeAlreadyExists(format!(
            "Node with ID '{}' already exists in the cluster.",
            node.id
        )))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn shutdown_node(&self, node_id: String) -> Result<()> {
        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(&node_id)?);

        if let Some(info) = self.metastore.get_kv(&node_key).await? {
            let node_info: NodeInfo = serde_json::from_slice(&info.data)?;

            return match node_info.node_type {
                NodeType::SelfManaged => self.shutdown_self_managed_node(&node_info).await,
                NodeType::SystemManaged => {
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

                    match self.metastore.transaction(txn).await?.success {
                        true => Ok(()),
                        false => Err(ErrorCode::ClusterUnknownNode(format!(
                            "Node with ID '{}' does not exist in the cluster.",
                            node_id
                        ))),
                    }
                }
            };
        }

        Err(ErrorCode::ClusterUnknownNode(format!(
            "Node with ID '{}' does not exist in the cluster.",
            node_id
        )))
    }

    async fn heartbeat(&self, node: &mut NodeInfo, seq: u64) -> Result<u64> {
        if node.node_type == NodeType::SelfManaged {
            assert!(!node.cluster_id.is_empty());
            assert!(!node.warehouse_id.is_empty());
        }

        let res = self.upsert_node(node.clone(), MatchSeq::Exact(seq)).await?;

        match res.success {
            true => {
                let Some(Response::Get(response)) =
                    res.responses.last().and_then(|x| x.response.as_ref())
                else {
                    return Err(ErrorCode::Internal("Unknown get response"));
                };

                let Some(node_info) = &response.value else {
                    return Err(ErrorCode::MetaServiceError("Add node info failure."));
                };

                Ok(node_info.seq)
            }
            false => self.resolve_conflicts(res, node).await,
        }
    }

    async fn drop_warehouse(&self, warehouse: String) -> Result<()> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        for _idx in 0..10 {
            let consistent_info = self.consistent_warehouse_info(&warehouse).await?;

            if let WarehouseInfo::SelfManaged(_) = consistent_info.warehouse_info {
                return Err(ErrorCode::InvalidWarehouse(
                    "Cannot drop self-managed warehouse",
                ));
            }

            let mut delete_txn = TxnRequest::default();

            let warehouse_key = format!(
                "{}/{}",
                self.warehouse_key_prefix,
                escape_for_key(&warehouse)?
            );

            delete_txn.condition.push(map_condition(
                &warehouse_key,
                MatchSeq::Exact(consistent_info.info_seq),
            ));
            delete_txn.if_then.push(TxnOp::delete(warehouse_key));

            for mut consistent_node in consistent_info.consistent_nodes {
                let node_key = self.node_key(&consistent_node.node_info)?;
                let cluster_key = self.cluster_key(&consistent_node.node_info)?;

                delete_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(consistent_node.node_seq),
                ));
                delete_txn.condition.push(map_condition(
                    &cluster_key,
                    MatchSeq::Exact(consistent_node.cluster_seq),
                ));

                delete_txn.if_then.push(TxnOp::delete(cluster_key));
                consistent_node.node_info.cluster_id = String::new();
                consistent_node.node_info.warehouse_id = String::new();
                delete_txn.if_then.push(TxnOp::put_with_ttl(
                    node_key,
                    serde_json::to_vec(&consistent_node.node_info)?,
                    Some(self.lift_time * 4),
                ));
            }

            if !self.metastore.transaction(delete_txn).await?.success {
                // seq is changed, will retry
                continue;
            }

            return Ok(());
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times).",
        ))
    }

    async fn create_warehouse(&self, warehouse: String, nodes: Vec<SelectedNode>) -> Result<()> {
        assert!(nodes.iter().all(|x| matches!(x, SelectedNode::Random(_))));

        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        if nodes.is_empty() {
            return Err(ErrorCode::EmptyNodesForWarehouse(
                "Cannot create warehouse with empty nodes.",
            ));
        }

        loop {
            let mut selected_nodes = Vec::with_capacity(nodes.len());

            // get online nodes
            let online_nodes = self.metastore.prefix_list_kv(&self.node_key_prefix).await?;

            let mut select_queue = nodes.clone();
            for (_, v) in &online_nodes {
                match select_queue.last() {
                    None => {
                        break;
                    }
                    Some(SelectedNode::Random(Some(_))) => {
                        return Err(ErrorCode::Unimplemented(
                            "Custom instance types are not supported.",
                        ));
                    }
                    Some(SelectedNode::Random(None)) => {
                        // select random node

                        let mut node_info = serde_json::from_slice::<NodeInfo>(&v.data)?;

                        if node_info.warehouse_id.is_empty() && node_info.cluster_id.is_empty() {
                            node_info.warehouse_id = warehouse.clone();
                            node_info.cluster_id = String::from("default");
                            selected_nodes.push((v.seq, node_info));
                            select_queue.pop();
                        }
                    }
                }
            }

            if !select_queue.is_empty() {
                return Err(ErrorCode::NoResourcesAvailable(
                    "Failed to create warehouse, reason: no resources available",
                ));
            }

            let mut txn = TxnRequest::default();

            for (seq, mut node) in selected_nodes {
                let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(&node.id)?);

                txn.condition
                    .push(map_condition(&node_key, MatchSeq::Exact(seq)));
                txn.if_then.push(TxnOp::put_with_ttl(
                    node_key,
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time * 4),
                ));

                let cluster_key = format!(
                    "{}/{}/{}/{}",
                    self.meta_key_prefix,
                    escape_for_key(&node.warehouse_id)?,
                    escape_for_key(&node.cluster_id)?,
                    escape_for_key(&node.id)?
                );

                node.cluster_id = String::new();
                node.warehouse_id = String::new();
                txn.condition
                    .push(map_condition(&cluster_key, MatchSeq::Exact(0)));
                txn.if_then.push(TxnOp::put_with_ttl(
                    cluster_key,
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time * 4),
                ));
            }

            let warehouse_key = format!(
                "{}/{}",
                self.warehouse_key_prefix,
                escape_for_key(&warehouse)?
            );

            txn.condition
                .push(map_condition(&warehouse_key, MatchSeq::Exact(0)));
            txn.if_then.push(TxnOp::put(
                warehouse_key.clone(),
                serde_json::to_vec(&WarehouseInfo::SystemManaged(SystemManagedInfo {
                    id: GlobalUniqName::unique(),
                    status: "Running".to_string(),
                    display_name: warehouse.clone(),
                    clusters: HashMap::from([(String::from("default"), SystemManagedCluster {
                        nodes: nodes.clone(),
                    })]),
                }))?,
            ));
            txn.else_then.push(TxnOp::get(warehouse_key));

            return match self.metastore.transaction(txn).await? {
                res if res.success => Ok(()),
                res => match res.responses.last() {
                    Some(TxnOpResponse {
                        response: Some(Response::Get(res)),
                    }) => {
                        if matches!(&res.value, Some(v) if v.seq != 0) {
                            return Err(ErrorCode::WarehouseAlreadyExists(
                                "Warehouse already exists",
                            ));
                        }

                        // retry
                        continue;
                    }
                    _ => Err(ErrorCode::MetaServiceError(
                        "Missing type for meta response",
                    )),
                },
            };
        }
    }

    async fn list_warehouses(&self) -> Result<Vec<WarehouseInfo>> {
        let values = self
            .metastore
            .prefix_list_kv(&self.warehouse_key_prefix)
            .await?;

        let mut warehouses = Vec::with_capacity(values.len());
        for (_warehouse_key, value) in values {
            warehouses.push(serde_json::from_slice::<WarehouseInfo>(&value.data)?);
        }

        Ok(warehouses)
    }

    async fn rename_warehouse(&self, current: String, to: String) -> Result<()> {
        if current.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        if to.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        for _idx in 0..10 {
            let mut consistent_info = self.consistent_warehouse_info(&current).await?;

            consistent_info.warehouse_info = match consistent_info.warehouse_info {
                WarehouseInfo::SelfManaged(_) => Err(ErrorCode::InvalidWarehouse(
                    "Cannot rename self-managed warehouse",
                )),
                WarehouseInfo::SystemManaged(mut info) => {
                    info.display_name = to.clone();
                    Ok(WarehouseInfo::SystemManaged(info))
                }
            }?;

            let mut rename_txn = TxnRequest::default();

            let old_warehouse_key = format!(
                "{}/{}",
                self.warehouse_key_prefix,
                escape_for_key(&current)?
            );

            let new_warehouse_key =
                format!("{}/{}", self.warehouse_key_prefix, escape_for_key(&to)?);

            rename_txn.condition.push(map_condition(
                &old_warehouse_key,
                MatchSeq::Exact(consistent_info.info_seq),
            ));

            rename_txn
                .condition
                .push(map_condition(&new_warehouse_key, MatchSeq::Exact(0)));
            rename_txn
                .else_then
                .push(TxnOp::get(new_warehouse_key.clone()));

            rename_txn.if_then.push(TxnOp::delete(old_warehouse_key));
            rename_txn.if_then.push(TxnOp::put(
                new_warehouse_key,
                serde_json::to_vec(&consistent_info.warehouse_info)?,
            ));

            for mut consistent_node in consistent_info.consistent_nodes {
                let node_key = self.node_key(&consistent_node.node_info)?;
                let old_cluster_key = self.cluster_key(&consistent_node.node_info)?;

                consistent_node.node_info.warehouse_id = to.clone();

                let new_cluster_key = self.cluster_key(&consistent_node.node_info)?;

                rename_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(consistent_node.node_seq),
                ));
                rename_txn.condition.push(map_condition(
                    &old_cluster_key,
                    MatchSeq::Exact(consistent_node.cluster_seq),
                ));

                rename_txn
                    .condition
                    .push(map_condition(&new_cluster_key, MatchSeq::Exact(0)));

                rename_txn.if_then.push(TxnOp::put_with_ttl(
                    node_key,
                    serde_json::to_vec(&consistent_node.node_info)?,
                    Some(self.lift_time * 4),
                ));
                consistent_node.node_info.cluster_id = String::new();
                consistent_node.node_info.warehouse_id = String::new();
                rename_txn.if_then.push(TxnOp::delete(old_cluster_key));
                rename_txn.if_then.push(TxnOp::put_with_ttl(
                    new_cluster_key.clone(),
                    serde_json::to_vec(&consistent_node.node_info)?,
                    Some(self.lift_time * 4),
                ));
            }

            return match self.metastore.transaction(rename_txn).await? {
                response if response.success => Ok(()),
                response => match response.responses.last() {
                    Some(TxnOpResponse {
                        response: Some(Response::Get(TxnGetResponse { value: Some(v), .. })),
                    }) if v.seq != 0 => Err(ErrorCode::WarehouseAlreadyExists(format!(
                        "Warehouse {} already exists.",
                        to
                    ))),
                    _ => {
                        continue;
                    }
                },
            };
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times while in rename warehouse).",
        ))
    }

    async fn add_warehouse_cluster(
        &self,
        warehouse: String,
        cluster: String,
        nodes: SelectedNodes,
    ) -> Result<()> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        if cluster.is_empty() {
            return Err(ErrorCode::InvalidWarehouse(
                "Warehouse cluster name is empty.",
            ));
        }

        if nodes.is_empty() {
            return Err(ErrorCode::EmptyNodesForWarehouse(
                "Cannot create warehouse cluster with empty nodes.",
            ));
        }

        for _idx in 0..10 {
            let mut selected_nodes = Vec::with_capacity(nodes.len());

            // get online nodes
            let online_nodes = self.metastore.prefix_list_kv(&self.node_key_prefix).await?;

            let mut select_queue = nodes.clone();
            for (_, v) in &online_nodes {
                match select_queue.last() {
                    None => {
                        break;
                    }
                    Some(SelectedNode::Random(Some(_))) => {
                        return Err(ErrorCode::Unimplemented(
                            "Custom instance types are not supported.",
                        ));
                    }
                    Some(SelectedNode::Random(None)) => {
                        // select random node

                        let mut node_info = serde_json::from_slice::<NodeInfo>(&v.data)?;

                        if node_info.warehouse_id.is_empty() && node_info.cluster_id.is_empty() {
                            node_info.warehouse_id = warehouse.clone();
                            node_info.cluster_id = cluster.clone();
                            selected_nodes.push((v.seq, node_info));
                            select_queue.pop();
                        }
                    }
                }
            }

            if !select_queue.is_empty() {
                return Err(ErrorCode::NoResourcesAvailable(
                    "Failed to create warehouse cluster, reason: no resources available",
                ));
            }

            let mut create_cluster_txn = TxnRequest::default();

            let mut consistent_info = self.consistent_warehouse_info(&warehouse).await?;

            consistent_info.warehouse_info = match consistent_info.warehouse_info {
                WarehouseInfo::SelfManaged(_) => Err(ErrorCode::InvalidWarehouse(format!(
                    "Cannot add cluster for warehouse {:?}, because it's self-managed warehouse.",
                    warehouse
                ))),
                WarehouseInfo::SystemManaged(mut info) => {
                    match info.clusters.contains_key(&cluster) {
                        true => Err(ErrorCode::WarehouseClusterAlreadyExists(format!(
                            "Warehouse cluster {:?}.{:?} already exists",
                            warehouse, cluster
                        ))),
                        false => {
                            info.clusters.insert(cluster.clone(), SystemManagedCluster {
                                nodes: nodes.clone(),
                            });
                            Ok(WarehouseInfo::SystemManaged(SystemManagedInfo {
                                id: info.id,
                                status: info.status,
                                display_name: info.display_name,
                                clusters: info.clusters,
                            }))
                        }
                    }
                }
            }?;

            let warehouse_key = format!(
                "{}/{}",
                self.warehouse_key_prefix,
                escape_for_key(&warehouse)?
            );

            create_cluster_txn.condition.push(map_condition(
                &warehouse_key,
                MatchSeq::Exact(consistent_info.info_seq),
            ));

            create_cluster_txn.if_then.push(TxnOp::put(
                warehouse_key.clone(),
                serde_json::to_vec(&consistent_info.warehouse_info)?,
            ));

            // lock all cluster state
            for consistent_node in consistent_info.consistent_nodes {
                let node_key = self.node_key(&consistent_node.node_info)?;
                let cluster_key = self.cluster_key(&consistent_node.node_info)?;

                create_cluster_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(consistent_node.node_seq),
                ));
                create_cluster_txn.condition.push(map_condition(
                    &cluster_key,
                    MatchSeq::Exact(consistent_node.cluster_seq),
                ));
            }

            for (seq, mut node) in selected_nodes {
                let node_key = self.node_key(&node)?;
                let cluster_key = self.cluster_key(&node)?;

                create_cluster_txn
                    .condition
                    .push(map_condition(&node_key, MatchSeq::Exact(seq)));
                create_cluster_txn.if_then.push(TxnOp::put_with_ttl(
                    node_key,
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time * 4),
                ));

                node.cluster_id = String::new();
                node.warehouse_id = String::new();
                create_cluster_txn
                    .condition
                    .push(map_condition(&cluster_key, MatchSeq::Exact(0)));
                create_cluster_txn.if_then.push(TxnOp::put_with_ttl(
                    cluster_key,
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time * 4),
                ));
            }

            return match self.metastore.transaction(create_cluster_txn).await? {
                res if res.success => Ok(()),
                _res => {
                    continue;
                }
            };
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times while in create warehouse cluster).",
        ))
    }

    async fn drop_warehouse_cluster(&self, warehouse: String, cluster: String) -> Result<()> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        if cluster.is_empty() {
            return Err(ErrorCode::InvalidWarehouse(
                "Warehouse cluster name is empty.",
            ));
        }

        for _idx in 0..10 {
            let mut drop_cluster_txn = TxnRequest::default();

            let mut consistent_info = self.consistent_warehouse_info(&warehouse).await?;

            consistent_info.warehouse_info = match consistent_info.warehouse_info {
                WarehouseInfo::SelfManaged(_) => Err(ErrorCode::InvalidWarehouse(format!(
                    "Cannot add cluster for warehouse {:?}, because it's self-managed warehouse.",
                    warehouse
                ))),
                WarehouseInfo::SystemManaged(mut info) => {
                    match info.clusters.contains_key(&cluster) {
                        false => Err(ErrorCode::WarehouseClusterNotExists(format!(
                            "Warehouse cluster {:?}.{:?} not exists",
                            warehouse, cluster
                        ))),
                        true => match info.clusters.len() == 1 {
                            true => Err(ErrorCode::EmptyNodesForWarehouse(format!(
                                "Warehouse {:?} only has {:?} one cluster, cannot drop it.",
                                warehouse, cluster
                            ))),
                            false => {
                                info.clusters.remove(&cluster);
                                Ok(WarehouseInfo::SystemManaged(SystemManagedInfo {
                                    id: info.id,
                                    status: info.status,
                                    display_name: info.display_name,
                                    clusters: info.clusters,
                                }))
                            }
                        },
                    }
                }
            }?;

            let warehouse_key = format!(
                "{}/{}",
                self.warehouse_key_prefix,
                escape_for_key(&warehouse)?
            );

            drop_cluster_txn.condition.push(map_condition(
                &warehouse_key,
                MatchSeq::Exact(consistent_info.info_seq),
            ));

            drop_cluster_txn.if_then.push(TxnOp::put(
                warehouse_key.clone(),
                serde_json::to_vec(&consistent_info.warehouse_info)?,
            ));

            // lock all cluster state
            for mut consistent_node in consistent_info.consistent_nodes {
                let node_key = self.node_key(&consistent_node.node_info)?;
                let cluster_key = self.cluster_key(&consistent_node.node_info)?;

                drop_cluster_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(consistent_node.node_seq),
                ));
                drop_cluster_txn.condition.push(map_condition(
                    &cluster_key,
                    MatchSeq::Exact(consistent_node.cluster_seq),
                ));

                if consistent_node.node_info.cluster_id == cluster {
                    // Remove node
                    consistent_node.node_info.cluster_id = String::new();
                    consistent_node.node_info.warehouse_id = String::new();

                    drop_cluster_txn.if_then.push(TxnOp::delete(cluster_key));
                    drop_cluster_txn.if_then.push(TxnOp::put_with_ttl(
                        node_key,
                        serde_json::to_vec(&consistent_node.node_info)?,
                        Some(self.lift_time * 4),
                    ))
                }
            }

            return match self.metastore.transaction(drop_cluster_txn).await? {
                res if res.success => Ok(()),
                _ => {
                    continue;
                }
            };
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times while in drop warehouse cluster).",
        ))
    }

    async fn rename_warehouse_cluster(
        &self,
        warehouse: String,
        cur: String,
        to: String,
    ) -> Result<()> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        if cur.is_empty() {
            return Err(ErrorCode::InvalidWarehouse(
                "Warehouse cluster name is empty.",
            ));
        }

        if to.is_empty() {
            return Err(ErrorCode::InvalidWarehouse(
                "Warehouse cluster name is empty.",
            ));
        }

        for _idx in 0..10 {
            let mut rename_cluster_txn = TxnRequest::default();

            let mut consistent_info = self.consistent_warehouse_info(&warehouse).await?;

            consistent_info.warehouse_info = match consistent_info.warehouse_info {
                WarehouseInfo::SelfManaged(_) => Err(ErrorCode::InvalidWarehouse(format!("Cannot rename cluster for warehouse {:?}, because it's self-managed warehouse.", warehouse))),
                WarehouseInfo::SystemManaged(mut info) => match info.clusters.contains_key(&cur) {
                    false => Err(ErrorCode::WarehouseClusterNotExists(format!("Warehouse cluster {:?}.{:?} not exists", warehouse, cur))),
                    true => {
                        let cluster_info = info.clusters.remove(&cur);
                        info.clusters.insert(to.clone(), cluster_info.unwrap());
                        Ok(WarehouseInfo::SystemManaged(SystemManagedInfo {
                            id: info.id,
                            status: info.status,
                            display_name: info.display_name,
                            clusters: info.clusters,
                        }))
                    }
                }
            }?;

            let warehouse_key = format!(
                "{}/{}",
                self.warehouse_key_prefix,
                escape_for_key(&warehouse)?
            );

            rename_cluster_txn.condition.push(map_condition(
                &warehouse_key,
                MatchSeq::Exact(consistent_info.info_seq),
            ));

            rename_cluster_txn.if_then.push(TxnOp::put(
                warehouse_key.clone(),
                serde_json::to_vec(&consistent_info.warehouse_info)?,
            ));

            // lock all cluster state
            for mut consistent_node in consistent_info.consistent_nodes {
                let node_key = self.node_key(&consistent_node.node_info)?;
                let old_cluster_key = self.cluster_key(&consistent_node.node_info)?;

                rename_cluster_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(consistent_node.node_seq),
                ));
                rename_cluster_txn.condition.push(map_condition(
                    &old_cluster_key,
                    MatchSeq::Exact(consistent_node.cluster_seq),
                ));

                if consistent_node.node_info.cluster_id == cur {
                    // rename node
                    consistent_node.node_info.cluster_id = to.clone();
                    let new_cluster_key = self.cluster_key(&consistent_node.node_info)?;

                    rename_cluster_txn.if_then.push(TxnOp::put_with_ttl(
                        node_key,
                        serde_json::to_vec(&consistent_node.node_info)?,
                        Some(self.lift_time * 4),
                    ));

                    consistent_node.node_info.cluster_id = String::new();
                    rename_cluster_txn
                        .condition
                        .push(map_condition(&new_cluster_key, MatchSeq::Exact(0)));
                    rename_cluster_txn
                        .if_then
                        .push(TxnOp::delete(old_cluster_key));
                    rename_cluster_txn.if_then.push(TxnOp::put_with_ttl(
                        new_cluster_key,
                        serde_json::to_vec(&consistent_node.node_info)?,
                        Some(self.lift_time * 4),
                    ));
                }
            }

            return match self.metastore.transaction(rename_cluster_txn).await? {
                res if res.success => Ok(()),
                _ => {
                    continue;
                }
            };
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times while in rename warehouse cluster).",
        ))
    }

    // async fn add_warehouse_cluster_node(&self, warehouse: &str, cluster: &str, nodes: SelectedNodes) -> Result<()> {
    //     // Move node to warehouse
    //     // self.consistent_warehouse_info()
    //     todo!()
    // }
    //
    // async fn remove_warehouse_cluster_node(&self, warehouse: &str, cluster: &str, nodes: RemoveNodes) -> Result<()> {
    //     todo!()
    // }

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
    async fn get_local_addr(&self) -> Result<Option<String>> {
        Ok(self.metastore.get_local_addr().await?)
    }

    async fn get_node_info(&self, node_id: &str) -> Result<NodeInfo> {
        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(node_id)?);
        let node_info = self.metastore.get_kv(&node_key).await?;
        match node_info {
            None => Err(ErrorCode::NotFoundClusterNode("")),
            Some(v) => Ok(serde_json::from_slice(&v.data)?),
        }
    }
}
