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

use std::collections::HashSet;
use std::time::Duration;

use databend_common_base::base::escape_for_key;
use databend_common_base::base::unescape_for_key;
use databend_common_base::base::GlobalUniqName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_store::MetaStore;
use databend_common_meta_types::protobuf::SeqV;
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

fn get_prev_value(res: Option<&TxnOpResponse>) -> Option<&SeqV> {
    res.and_then(|response| response.response.as_ref())
        .and_then(|response| match response {
            Response::Put(v) => v.prev_value.as_ref(),
            Response::Delete(v) => v.prev_value.as_ref(),
            _ => unreachable!(),
        })
}

impl WarehouseMgr {
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
            serde_json::to_vec(&WarehouseInfo::SelfManaged)?,
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
                                    Err(ErrorCode::WarehouseAlreadyExists(""))
                                }
                                WarehouseInfo::SelfManaged => match response.responses.first() {
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
}

#[async_trait::async_trait]
impl WarehouseApi for WarehouseMgr {
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn add_node(&self, node: NodeInfo) -> Result<u64> {
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

    async fn heartbeat(&self, node: &mut NodeInfo, seq: u64) -> Result<u64> {
        assert!(!node.cluster_id.is_empty());
        assert!(!node.warehouse_id.is_empty());

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
        let drop_key = format!("{}/{}", self.meta_key_prefix, escape_for_key(&warehouse)?);

        loop {
            let mut delete_txn = TxnRequest::default();
            let values = self.metastore.prefix_list_kv(&drop_key).await?;

            let mut txn = TxnRequest::default();
            for (node_key, _value) in values {
                txn.if_then.push(TxnOp::get(format!(
                    "{}/{}",
                    self.node_key_prefix,
                    &node_key[drop_key.len() + 1..]
                )));

                delete_txn.if_then.push(TxnOp::delete(node_key));
            }

            let warehouse_key = format!(
                "{}/{}",
                self.warehouse_key_prefix,
                escape_for_key(&warehouse)?
            );

            txn.if_then.push(TxnOp::get(warehouse_key.clone()));
            let mut fetch_reply = self.metastore.transaction(txn).await?;

            if let Some(response) = fetch_reply.responses.pop() {
                let Some(Response::Get(response)) = response.response else {
                    return Err(ErrorCode::UnknownWarehouse(format!(
                        "Unknown warehouse or self managed warehouse {:?}",
                        warehouse
                    )));
                };

                if response.key != warehouse_key {
                    return Err(ErrorCode::UnknownWarehouse(format!(
                        "Unknown warehouse or self managed warehouse {:?}",
                        warehouse
                    )));
                }

                let Some(value) = &response.value else {
                    return Err(ErrorCode::UnknownWarehouse(format!(
                        "Unknown warehouse or self managed warehouse {:?}",
                        warehouse
                    )));
                };

                if fetch_reply.responses.len() != delete_txn.if_then.len() {
                    // TODO: maybe auto retry?
                    return Err(ErrorCode::WarehouseOperateConflict("Missing node info in online nodes list. It's possible that some nodes offline during the drop warehouse. You may try the operation again."));
                }

                for response in fetch_reply.responses {
                    let Some(Response::Get(response)) = response.response else {
                        // TODO: maybe auto retry?
                        return Err(ErrorCode::WarehouseOperateConflict("Missing node info in online nodes list. It's possible that some nodes offline during the drop warehouse. You may try the operation again."));
                    };

                    let Some(value) = &response.value else {
                        // TODO: maybe auto retry?
                        return Err(ErrorCode::WarehouseOperateConflict("Missing node info in online nodes list. It's possible that some nodes offline during the drop warehouse. You may try the operation again."));
                    };

                    let mut node_info = serde_json::from_slice::<NodeInfo>(&value.data)?;

                    if node_info.warehouse_id != warehouse {
                        return Err(ErrorCode::WarehouseOperateConflict(
                            "Warehouse operate is conflict.",
                        ));
                    }

                    delete_txn
                        .condition
                        .push(map_condition(&response.key, MatchSeq::Exact(value.seq)));
                    node_info.cluster_id = String::new();
                    node_info.warehouse_id = String::new();
                    delete_txn.if_then.push(TxnOp::put_with_ttl(
                        response.key,
                        serde_json::to_vec(&node_info)?,
                        Some(self.lift_time * 4),
                    ));
                }

                delete_txn
                    .condition
                    .push(map_condition(&warehouse_key, MatchSeq::Exact(value.seq)));
                delete_txn.if_then.push(TxnOp::delete(warehouse_key));

                if !self.metastore.transaction(delete_txn).await?.success {
                    // seq is changed, will retry
                    continue;
                }

                return Ok(());
            }

            return Err(ErrorCode::UnknownWarehouse(format!(
                "Unknown warehouse or self managed warehouse {:?}",
                warehouse
            )));
        }
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
            let mut allocated_nodes = HashSet::with_capacity(nodes.len());

            // get online nodes
            let online_nodes = self.metastore.prefix_list_kv(&self.node_key_prefix).await?;

            for select_node in &nodes {
                match select_node {
                    SelectedNode::Random(Some(_)) => {
                        return Err(ErrorCode::Unimplemented(
                            "Custom instance types are not supported.",
                        ));
                    }
                    // select random node
                    SelectedNode::Random(None) => {
                        for (_, v) in &online_nodes {
                            let mut node_info = serde_json::from_slice::<NodeInfo>(&v.data)?;

                            if node_info.warehouse_id.is_empty()
                                && node_info.cluster_id.is_empty()
                                && !allocated_nodes.contains(&node_info.id)
                            {
                                node_info.cluster_id = warehouse.clone();
                                node_info.warehouse_id = warehouse.clone();
                                allocated_nodes.insert(node_info.id.clone());
                                selected_nodes.push((v.seq, node_info));
                                break;
                            }
                        }
                    }
                }
            }

            if selected_nodes.len() != nodes.len() {
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
                    clusters: vec![nodes.clone()],
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
                && get_prev_value(res.responses.first()).is_some()
                && get_prev_value(res.responses.last()).is_some()
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

    async fn get_node_info(&self, node_id: &str) -> Result<NodeInfo> {
        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(&node_id)?);
        let node_info = self.metastore.get_kv(&node_key).await?;
        match node_info {
            None => Err(ErrorCode::NotFoundClusterNode("")),
            Some(v) => Ok(serde_json::from_slice(&v.data)?),
        }
    }
}
