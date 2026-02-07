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
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::time::Duration;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::BuildInfoRef;
use databend_common_base::base::escape_for_key;
use databend_common_base::base::unescape_for_key;
use databend_common_base::vec_ext::VecExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_store::MetaStore;
use databend_meta_kvapi::kvapi::KVApi;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::ConditionResult;
use databend_meta_types::InvalidReply;
use databend_meta_types::MatchSeq;
use databend_meta_types::MatchSeqExt;
use databend_meta_types::MetaError;
use databend_meta_types::NodeInfo;
use databend_meta_types::NodeType;
use databend_meta_types::SeqV;
use databend_meta_types::TxnCondition;
use databend_meta_types::TxnGetResponse;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnOpResponse;
use databend_meta_types::TxnReply;
use databend_meta_types::TxnRequest;
use databend_meta_types::anyerror::AnyError;
use databend_meta_types::txn_op_response::Response;
use log::error;
use log::info;

use crate::errors::meta_service_error;
use crate::warehouse::WarehouseApi;
use crate::warehouse::warehouse_api::SelectedNode;
use crate::warehouse::warehouse_api::SystemManagedCluster;
use crate::warehouse::warehouse_api::SystemManagedWarehouse;
use crate::warehouse::warehouse_api::WarehouseInfo;

static DEFAULT_CLUSTER_ID: &str = "default";

pub static WAREHOUSE_API_KEY_PREFIX: &str = "__fd_clusters_v6";
pub static WAREHOUSE_META_KEY_PREFIX: &str = "__fd_warehouses";

type LostNodes = Vec<SelectedNode>;

// example:
// __fd_clusters_v6/test_tenant/online_nodes
//      |- /RUV9DQArNnP4Hej4A74f07: NodeInfo { id: "RUV9DQArNnP4Hej4A74f07", secret: "", cpu_nums: 0, version: 0, http_address: "", flight_address: "", discovery_address: "", binary_version: "", node_type: SystemManaged, node_group: None, cluster_id: "", warehouse_id: "", runtime_node_group: None }
// 		|- /9a9DU1KufVmSEIDSOnFLZ: NodeInfo { id: "9a9DU1KufVmSEIDSOnFLZ", secret: "", cpu_nums: 0, version: 0, http_address: "", flight_address: "", discovery_address: "", binary_version: "", node_type: SystemManaged, node_group: Some("test"), cluster_id: "test_cluster", warehouse_id: "test_warehouse", runtime_node_group: Some("test") }
// 		|- /5jTQoMBGJb4TLHeD4pjoa: NodeInfo { id: "5jTQoMBGJb4TLHeD4pjoa", secret: "", cpu_nums: 0, version: 0, http_address: "", flight_address: "", discovery_address: "", binary_version: "", node_type: SystemManaged, node_group: None, cluster_id: "test_cluster", warehouse_id: "test_warehouse", runtime_node_group: None }
//
// __fd_clusters_v6/test_tenant/online_clusters
//      |- /test_warehouse/test_cluster/9a9DU1KufVmSEIDSOnFLZ: NodeInfo { id: "9a9DU1KufVmSEIDSOnFLZ", secret: "", cpu_nums: 0, version: 0, http_address: "", flight_address: "", discovery_address: "", binary_version: "", node_type: SystemManaged, node_group: None, cluster_id: "", warehouse_id: "", runtime_node_group: None }
// 		|- /test_warehouse/test_cluster/5jTQoMBGJb4TLHeD4pjoa: NodeInfo { id: "5jTQoMBGJb4TLHeD4pjoa", secret: "", cpu_nums: 0, version: 0, http_address: "", flight_address: "", discovery_address: "", binary_version: "", node_type: SystemManaged, node_group: None, cluster_id: "", warehouse_id: "", runtime_node_group: None }
// __fd_warehouses/v1/test_tenant
// 		|- /test_warehouse: SystemManaged(SystemManagedWarehouse { id: "udcQQnniiBBFmfP9jziSy4", status: "Running", display_name: "test_warehouse", clusters: {"test_cluster": SystemManagedCluster { nodes: [Random(Some("test")), Random(None)] }} })
pub struct WarehouseMgr {
    metastore: MetaStore,
    lift_time: Duration,
    node_key_prefix: String,
    cluster_node_key_prefix: String,
    warehouse_info_key_prefix: String,
    version: BuildInfoRef,
}

impl WarehouseMgr {
    pub fn create(
        metastore: MetaStore,
        tenant: &str,
        lift_time: Duration,
        version: BuildInfoRef,
    ) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while cluster mgr create)",
            ));
        }

        Ok(WarehouseMgr {
            metastore,
            lift_time,
            version,
            // Prefix for all online nodes of the tenant
            node_key_prefix: format!(
                "{}/{}/online_nodes",
                WAREHOUSE_API_KEY_PREFIX,
                escape_for_key(tenant)?
            ),
            // Prefix for all online computing clusters of the tenant
            cluster_node_key_prefix: format!(
                "{}/{}/online_clusters",
                WAREHOUSE_API_KEY_PREFIX,
                escape_for_key(tenant)?
            ),
            // Prefix for all warehouses of the tenant (must ensure compatibility across all versions)
            warehouse_info_key_prefix: format!(
                "{}/v1/{}",
                WAREHOUSE_META_KEY_PREFIX,
                escape_for_key(tenant)?,
            ),
        })
    }

    async fn shutdown_system_managed(&self, node_info: &NodeInfo) -> Result<()> {
        let node_key = self.node_key(node_info)?;

        let mut txn = TxnRequest::default();

        txn.if_then.push(TxnOp::delete(node_key));

        if node_info.assigned_warehouse() {
            let cluster_node_key = self.cluster_node_key(node_info)?;
            txn.if_then.push(TxnOp::delete(cluster_node_key));
        }

        match self
            .metastore
            .transaction(txn)
            .await
            .map_err(meta_service_error)?
            .success
        {
            true => Ok(()),
            false => Err(ErrorCode::ClusterUnknownNode(format!(
                "Node with ID '{}' does not exist in the cluster.",
                node_info.id
            ))),
        }
    }
}

fn map_condition(k: &str, seq: MatchSeq) -> TxnCondition {
    match seq {
        MatchSeq::Any => TxnCondition::match_seq(k.to_owned(), ConditionResult::Ge, 0),
        MatchSeq::GE(v) => TxnCondition::match_seq(k.to_owned(), ConditionResult::Ge, v),
        MatchSeq::Exact(v) => TxnCondition::match_seq(k.to_owned(), ConditionResult::Eq, v),
    }
}

pub struct NodeInfoSnapshot {
    node_seq: u64,
    cluster_seq: u64,
    node_info: NodeInfo,
}

struct WarehouseSnapshot {
    info_seq: u64,
    warehouse_info: WarehouseInfo,
    snapshot_nodes: Vec<NodeInfoSnapshot>,
}

impl WarehouseMgr {
    fn node_key(&self, node: &NodeInfo) -> Result<String> {
        Ok(format!(
            "{}/{}",
            self.node_key_prefix,
            escape_for_key(&node.id)?
        ))
    }

    fn cluster_node_key(&self, node: &NodeInfo) -> Result<String> {
        Ok(format!(
            "{}/{}/{}/{}",
            self.cluster_node_key_prefix,
            escape_for_key(&node.warehouse_id)?,
            escape_for_key(&node.cluster_id)?,
            escape_for_key(&node.id)?
        ))
    }

    fn warehouse_info_key(&self, warehouse: &str) -> Result<String> {
        Ok(format!(
            "{}/{}",
            self.warehouse_info_key_prefix,
            escape_for_key(warehouse)?
        ))
    }

    async fn start_self_managed(&self, node_with_wh: NodeInfo) -> Result<TxnReply> {
        if node_with_wh.warehouse_id.is_empty() || node_with_wh.cluster_id.is_empty() {
            return Err(ErrorCode::InvalidWarehouse(
                "The warehouse_id and cluster_id for self managed node must not be empty.",
            ));
        }

        let node_without_wh = node_with_wh.unload_warehouse_info();

        let node_key = self.node_key(&node_with_wh)?;
        let cluster_node_key = self.cluster_node_key(&node_with_wh)?;
        let warehouse_key = self.warehouse_info_key(&node_with_wh.warehouse_id)?;
        let warehouse = WarehouseInfo::SelfManaged(node_with_wh.warehouse_id.clone());

        let mut txn = TxnRequest::default();

        txn.condition
            .push(map_condition(&node_key, MatchSeq::Exact(0)));

        let ttl = Some(self.lift_time);
        txn.if_then = vec![
            TxnOp::put_with_ttl(&node_key, serde_json::to_vec(&node_with_wh)?, ttl),
            TxnOp::put_with_ttl(
                &cluster_node_key,
                serde_json::to_vec(&node_without_wh)?,
                ttl,
            ),
            TxnOp::put_with_ttl(&warehouse_key, serde_json::to_vec(&warehouse)?, ttl),
            TxnOp::get(&node_key),
        ];

        txn.else_then = vec![TxnOp::get(&node_key), TxnOp::get(&warehouse_key)];

        let mut wh_seq = 0;
        let max_retry = 20;

        for retry_count in 0..max_retry {
            let mut warehouse_txn = txn.clone();

            // insert if warehouse info is not exists or SelfManaged
            warehouse_txn
                .condition
                .push(TxnCondition::eq_seq(&warehouse_key, wh_seq));

            let mut txn_reply = self
                .metastore
                .transaction(warehouse_txn)
                .await
                .map_err(meta_service_error)?;

            if txn_reply.success {
                return Ok(txn_reply);
            }

            // txn_reply includes: 1. get node response 2. get warehouse response

            // 1. Check node state
            {
                let first = txn_reply.responses.first();

                let node_get_resp = first.map(|r| r.as_get());
                let node_get_resp = node_get_resp
                    .ok_or_else(|| invalid_get_resp(first))
                    .map_err(meta_service_error)?;

                let exact_seq = MatchSeq::Exact(0);
                if exact_seq.match_seq(&node_get_resp.value).is_err() {
                    // Node changed, no more retry is needed.
                    return Ok(txn_reply);
                }
            }

            // 2. Check warehouse state
            {
                // The last response is not meant to be returned.
                let last = txn_reply.responses.pop();

                let wh_get_resp = last.as_ref().and_then(|r| r.try_as_get());
                let wh_get_resp = wh_get_resp
                    .ok_or_else(|| invalid_get_resp(last.as_ref()))
                    .map_err(meta_service_error)?;

                if let Some(wh_seqv) = &wh_get_resp.value {
                    let wh: WarehouseInfo = serde_json::from_slice(&wh_seqv.data)?;

                    match wh {
                        WarehouseInfo::SystemManaged(_) => {
                            return Err(ErrorCode::WarehouseAlreadyExists(
                                "Already exists same name system-managed warehouse.",
                            ));
                        }
                        WarehouseInfo::SelfManaged(_) => {
                            // Safe to retry with the updated seq of warehouse
                        }
                    };

                    info!(
                        "Self-managed warehouse has already been created by other nodes;\
                         attempt to join it. Retry count: {}",
                        retry_count
                    );
                };

                // Retry with the updated seq of warehouse
                wh_seq = wh_get_resp.value.as_ref().map_or(0, |v| v.seq);
            }

            // upon retry, fallback a little while.
            tokio::time::sleep(Duration::from_millis(30 * retry_count)).await;
        }

        Err(ErrorCode::WarehouseOperateConflict(format!(
            "Warehouse operate conflict(tried {max_retry} times)."
        )))
    }

    async fn heartbeat_self_managed(&self, node: NodeInfo, seq: u64) -> Result<TxnReply> {
        let node_with_wh = node.clone();
        let node_without_wh = node.unload_warehouse_info();

        let node_key = self.node_key(&node_with_wh)?;
        let cluster_node_key = self.cluster_node_key(&node_with_wh)?;
        let warehouse_key = self.warehouse_info_key(&node_with_wh.warehouse_id)?;
        let warehouse = WarehouseInfo::SelfManaged(node_with_wh.warehouse_id.clone());

        let mut txn = TxnRequest::default();

        txn.condition
            .push(map_condition(&node_key, MatchSeq::Exact(seq)));

        let ttl = Some(self.lift_time);
        txn.if_then = vec![
            TxnOp::put_with_ttl(&node_key, serde_json::to_vec(&node_with_wh)?, ttl),
            TxnOp::put_with_ttl(
                &cluster_node_key,
                serde_json::to_vec(&node_without_wh)?,
                ttl,
            ),
            TxnOp::put_with_ttl(&warehouse_key, serde_json::to_vec(&warehouse)?, ttl),
            TxnOp::get(&node_key),
        ];

        txn.else_then = vec![TxnOp::get(&node_key), TxnOp::get(&warehouse_key)];

        let mut txn_reply = self
            .metastore
            .transaction(txn)
            .await
            .map_err(meta_service_error)?;

        if !txn_reply.success {
            // txn_reply includes: 1. get node response 2. get warehouse response

            // Check warehouse state
            // The last response is not meant to be returned.
            let last = txn_reply.responses.pop();

            let wh_get_resp = last.as_ref().and_then(|r| r.try_as_get());
            let wh_get_resp = wh_get_resp
                .ok_or_else(|| invalid_get_resp(last.as_ref()))
                .map_err(meta_service_error)?;

            if let Some(wh_seqv) = &wh_get_resp.value {
                let wh: WarehouseInfo = serde_json::from_slice(&wh_seqv.data)?;

                if let WarehouseInfo::SystemManaged(_) = wh {
                    // an unknown error caused all nodes of the warehouse to fail the heartbeat with the meta service. If someone creates a warehouse with the same name during this period.
                    return Err(ErrorCode::WarehouseAlreadyExists(
                        "Already exists same name system-managed warehouse.",
                    ));
                }
            };
        }

        Ok(txn_reply)
    }

    // return list of unhealthy warehouse clusters.
    // return type: HashMap<(WarehouseId, ClusterId), LostNodes>
    async fn unhealthy_warehouses(&self) -> Result<HashMap<(String, String), LostNodes>> {
        let online_nodes = self.list_online_nodes().await?;
        let mut cluster_online_nodes = HashMap::with_capacity(online_nodes.len());

        for node in online_nodes {
            match cluster_online_nodes.entry((node.warehouse_id.clone(), node.cluster_id.clone())) {
                Entry::Vacant(v) => {
                    v.insert(vec![node]);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().push(node);
                }
            }
        }

        let list_reply = self
            .metastore
            .list_kv_collect(ListOptions::unlimited(&self.warehouse_info_key_prefix))
            .await
            .map_err(meta_service_error)?;

        let mut unhealthy_warehouses = HashMap::with_capacity(list_reply.len());

        for (_key, seq_v) in list_reply {
            let wh: WarehouseInfo = serde_json::from_slice(&seq_v.data)?;

            // skip if self-managed warehouse
            let WarehouseInfo::SystemManaged(wh) = wh else {
                continue;
            };

            if wh.status.to_uppercase() != "RUNNING" {
                continue;
            }

            for (cluster_id, cluster) in wh.clusters {
                let mut lost_nodes = cluster.nodes;

                let key = (wh.id.clone(), cluster_id.clone());
                if let Some(online_nodes) = cluster_online_nodes.remove(&key) {
                    for online_node in online_nodes {
                        lost_nodes.remove_first(&SelectedNode::Random(
                            online_node.runtime_node_group.clone(),
                        ));
                    }
                }

                if !lost_nodes.is_empty() {
                    unhealthy_warehouses.insert((wh.id.clone(), cluster_id.clone()), lost_nodes);
                }
            }
        }

        Ok(unhealthy_warehouses)
    }

    async fn recovery_system_managed_warehouse(&self, node: &mut NodeInfo) -> Result<bool> {
        for retry_count in 0..20 {
            let unhealthy_warehouses = self.unhealthy_warehouses().await?;

            if unhealthy_warehouses.is_empty() {
                // All warehouses are healthy.
                return Ok(false);
            }

            let mut runtime_node_group = None;
            let mut warehouse_and_cluster = None;
            let match_group = SelectedNode::Random(node.node_group.clone());
            for ((warehouse, cluster), lost_nodes) in unhealthy_warehouses {
                if lost_nodes.iter().any(|x| x == &match_group) {
                    runtime_node_group = node.node_group.clone();
                    warehouse_and_cluster = Some((warehouse, cluster));
                    break;
                } else if warehouse_and_cluster.is_none() {
                    if !lost_nodes.iter().any(|x| x == &SelectedNode::Random(None)) {
                        continue;
                    }

                    runtime_node_group = None;
                    warehouse_and_cluster = Some((warehouse, cluster));
                }
            }

            let Some((warehouse, cluster)) = warehouse_and_cluster else {
                // No warehouse needs to fixed
                return Ok(false);
            };

            let Ok(warehouse_snapshot) = self.warehouse_snapshot(&warehouse).await else {
                // The warehouse may have been dropped.
                log::warn!("The warehouse {} may have been dropped.", warehouse);
                continue;
            };

            let WarehouseInfo::SystemManaged(wh) = warehouse_snapshot.warehouse_info else {
                // The warehouse may have been dropped and started self-managed warehouse with same name.
                log::warn!(
                    "The warehouse({}) may have been dropped and started self-managed warehouse with same name.",
                    warehouse
                );
                continue;
            };

            let Some(recovery_cluster) = wh.clusters.get(&cluster) else {
                // The cluster may have been dropped
                log::warn!(
                    "The cluster({}/{}) may have been dropped",
                    warehouse,
                    cluster
                );
                continue;
            };

            // Check the node list again; if conflicts occur, skip this correction.
            {
                let mut lost_nodes = recovery_cluster.nodes.clone();

                for node_snapshot in warehouse_snapshot.snapshot_nodes {
                    if node_snapshot.node_info.cluster_id != cluster {
                        continue;
                    }

                    lost_nodes.remove_first(&SelectedNode::Random(
                        node_snapshot.node_info.runtime_node_group.clone(),
                    ));
                }

                if !lost_nodes
                    .iter()
                    .any(|x| x == &SelectedNode::Random(runtime_node_group.clone()))
                {
                    continue;
                }
            }

            let mut txn = TxnRequest::default();

            let mut node_with_wh = node.clone();

            node_with_wh.cluster_id = cluster.clone();
            node_with_wh.warehouse_id = warehouse.clone();
            node.runtime_node_group = runtime_node_group.clone();
            node_with_wh.runtime_node_group = runtime_node_group;

            let node_key = self.node_key(&node_with_wh)?;
            let cluster_node_key = self.cluster_node_key(&node_with_wh)?;
            let warehouse_info_key = self.warehouse_info_key(&warehouse)?;

            txn.condition = vec![
                map_condition(&node_key, MatchSeq::Exact(0)),
                map_condition(&cluster_node_key, MatchSeq::Exact(0)),
                map_condition(
                    &warehouse_info_key,
                    MatchSeq::Exact(warehouse_snapshot.info_seq),
                ),
            ];

            txn.if_then = vec![
                TxnOp::put_with_ttl(
                    node_key,
                    serde_json::to_vec(&node_with_wh)?,
                    Some(self.lift_time),
                ),
                TxnOp::put_with_ttl(
                    cluster_node_key,
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time),
                ),
                TxnOp::put(
                    warehouse_info_key,
                    serde_json::to_vec(&WarehouseInfo::SystemManaged(wh))?,
                ),
            ];

            if self
                .metastore
                .transaction(txn)
                .await
                .map_err(meta_service_error)?
                .success
            {
                return Ok(true);
            }

            // upon retry, fallback a little while.
            tokio::time::sleep(Duration::from_millis(30 * retry_count)).await;
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 20 times).".to_string(),
        ))
    }

    async fn start_system_managed(&self, mut node: NodeInfo) -> Result<TxnReply> {
        let mut txn = TxnRequest::default();
        let node_key = self.node_key(&node)?;

        if !self.recovery_system_managed_warehouse(&mut node).await? {
            txn.else_then.push(TxnOp::get(node_key.clone()));
            txn.condition
                .push(map_condition(&node_key, MatchSeq::Exact(0)));

            txn.if_then = vec![
                TxnOp::put_with_ttl(
                    node_key.clone(),
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time),
                ),
                TxnOp::get(node_key.clone()),
            ];

            return self
                .metastore
                .transaction(txn)
                .await
                .map_err(meta_service_error);
        }

        txn.if_then = vec![TxnOp::get(node_key.clone())];
        self.metastore
            .transaction(txn)
            .await
            .map_err(meta_service_error)
    }

    async fn update_system_managed_node(&self, node: NodeInfo, seq: u64) -> Result<TxnReply> {
        let mut txn = TxnRequest::default();
        let node_key = self.node_key(&node)?;

        txn.condition
            .push(map_condition(&node_key, MatchSeq::Exact(seq)));

        txn.if_then.push(TxnOp::put_with_ttl(
            node_key.clone(),
            serde_json::to_vec(&node)?,
            Some(self.lift_time),
        ));

        // If the warehouse has already been assigned.
        if node.assigned_warehouse() {
            let cluster_node_key = self.cluster_node_key(&node)?;

            let node = node.unload_warehouse_info();
            txn.if_then.push(TxnOp::put_with_ttl(
                cluster_node_key,
                serde_json::to_vec(&node)?,
                Some(self.lift_time),
            ));
        }

        txn.if_then.push(TxnOp::get(node_key.clone()));
        txn.else_then.push(TxnOp::get(node_key.clone()));
        self.metastore
            .transaction(txn)
            .await
            .map_err(meta_service_error)
    }

    async fn heartbeat_system_managed(&self, node: NodeInfo, seq: u64) -> Result<TxnReply> {
        let mut txn = TxnRequest::default();
        let node_key = self.node_key(&node)?;

        txn.condition
            .push(map_condition(&node_key, MatchSeq::Exact(seq)));

        txn.if_then.push(TxnOp::put_with_ttl(
            node_key.clone(),
            serde_json::to_vec(&node)?,
            Some(self.lift_time),
        ));

        // If the warehouse has already been assigned.
        if node.assigned_warehouse() {
            let cluster_node_key = self.cluster_node_key(&node)?;

            let node = node.unload_warehouse_info();
            txn.if_then.push(TxnOp::put_with_ttl(
                cluster_node_key,
                serde_json::to_vec(&node)?,
                Some(self.lift_time),
            ));
        }

        txn.if_then.push(TxnOp::get(node_key.clone()));
        txn.else_then.push(TxnOp::get(node_key.clone()));
        self.metastore
            .transaction(txn)
            .await
            .map_err(meta_service_error)
    }

    async fn leave_warehouse(&self, node_info: &mut NodeInfo, seq: u64) -> Result<u64> {
        let leave_node = node_info.leave_warehouse();

        let reply = self
            .update_system_managed_node(leave_node.clone(), seq)
            .await?;

        if !reply.success {
            return Err(ErrorCode::WarehouseOperateConflict(
                "Node's status changed while in leave cluster.",
            ));
        }

        *node_info = leave_node;

        let get_node_response = reply.responses.last();
        let get_node_response = get_node_response.and_then(TxnOpResponse::try_as_get);

        match get_node_response {
            Some(TxnGetResponse { value: Some(v), .. }) => Ok(v.seq),
            _ => Err(ErrorCode::MetaServiceError(
                "Invalid response while leave cluster, expect get response",
            )),
        }
    }

    async fn join_warehouse(&self, node_info: NodeInfo, seq: u64) -> Result<u64> {
        let reply = self.update_system_managed_node(node_info, seq).await?;

        if !reply.success {
            return Err(ErrorCode::WarehouseOperateConflict(
                "Node's status changed while in join cluster.",
            ));
        }

        let get_node_response = reply.responses.last();
        let get_node_response = get_node_response.and_then(TxnOpResponse::try_as_get);

        match get_node_response {
            Some(TxnGetResponse { value: Some(v), .. }) => Ok(v.seq),
            _ => Err(ErrorCode::MetaServiceError(
                "Invalid response while join cluster, expect get response",
            )),
        }
    }

    async fn resolve_conflicts(&self, reply: TxnReply, node: &mut NodeInfo) -> Result<u64> {
        // system-managed heartbeat_node reply include 'get node response' at last response
        let get_node_response = reply.responses.last();
        let get_node_response = get_node_response.and_then(TxnOpResponse::try_as_get);

        match get_node_response {
            None => self.leave_warehouse(node, 0).await,
            Some(TxnGetResponse { value: None, .. }) => self.leave_warehouse(node, 0).await,
            Some(TxnGetResponse { value: Some(v), .. }) => {
                let node_from_meta = serde_json::from_slice::<NodeInfo>(&v.data)?;

                // Removed this node from the cluster in other nodes
                if node.assigned_warehouse() && !node_from_meta.assigned_warehouse() {
                    return self.leave_warehouse(node, v.seq).await;
                }

                // Move this node to new warehouse and cluster
                let seq = self.join_warehouse(node_from_meta.clone(), v.seq).await?;
                *node = node_from_meta;
                Ok(seq)
            }
        }
    }

    async fn warehouse_snapshot(&self, id: &str) -> Result<WarehouseSnapshot> {
        // Obtain a consistent snapshot of the warehouse nodes using 3 RPC calls
        // TODO: meta does not support using list within transactions, which will be optimized in later version
        let warehouse_info_key = self.warehouse_info_key(id)?;

        let nodes_prefix = format!("{}/{}/", self.cluster_node_key_prefix, escape_for_key(id)?);

        'get_warehouse_snapshot: for _idx in 0..64 {
            let Some(before_info) = self
                .metastore
                .get_kv(&warehouse_info_key)
                .await
                .map_err(meta_service_error)?
            else {
                return Err(ErrorCode::UnknownWarehouse(format!(
                    "Unknown warehouse or self managed warehouse {:?}",
                    id
                )));
            };

            let values = self
                .metastore
                .list_kv_collect(ListOptions::unlimited(&nodes_prefix))
                .await
                .map_err(meta_service_error)?;

            let mut after_txn = TxnRequest::default();
            let mut cluster_node_seq = Vec::with_capacity(values.len());

            for (node_key, value) in values {
                let suffix = &node_key[nodes_prefix.len()..];

                let Some((_cluster, node)) = suffix.split_once('/') else {
                    return Err(ErrorCode::InvalidWarehouse(format!(
                        "Node key is invalid {:?}",
                        node_key
                    )));
                };

                let node_key = format!("{}/{}", self.node_key_prefix, node);
                // Fetch the seq of node under `node_prefix/**`
                after_txn.if_then.push(TxnOp::get(node_key));
                // While we always assert the seq of `node_key_prefix/**` nodes during updates to ensure a consistent snapshot view,
                // the seq of `cluster_node_key_prefix/**` nodes is also asserted as a defensive measure.
                cluster_node_seq.push(value.seq);
            }

            let condition = map_condition(&warehouse_info_key, MatchSeq::Exact(before_info.seq));
            after_txn.condition.push(condition);

            let txn_reply = self
                .metastore
                .transaction(after_txn)
                .await
                .map_err(meta_service_error)?;

            if !txn_reply.success {
                // The snapshot version status has changed; need to retry obtaining the snapshot.
                continue 'get_warehouse_snapshot;
            }

            let mut snapshot_nodes = Vec::with_capacity(txn_reply.responses.len());
            for (idx, response) in txn_reply.responses.into_iter().enumerate() {
                let get_node_info = response.as_get();

                let Some(seq_v) = get_node_info.value.as_ref() else {
                    // The node went offline during the get snapshot.
                    continue 'get_warehouse_snapshot;
                };

                let node_info = serde_json::from_slice::<NodeInfo>(&seq_v.data)?;

                assert_eq!(node_info.warehouse_id, id);
                assert!(!node_info.cluster_id.is_empty());

                snapshot_nodes.push(NodeInfoSnapshot {
                    node_seq: seq_v.seq,
                    cluster_seq: cluster_node_seq[idx],
                    node_info,
                });
            }

            return Ok(WarehouseSnapshot {
                info_seq: before_info.seq,
                warehouse_info: serde_json::from_slice(&before_info.data)?,
                snapshot_nodes,
            });
        }

        Err(ErrorCode::Internal(
            "Get consistent warehouse info failure(tried 64 times)",
        ))
    }

    async fn shutdown_self_managed_node(&self, node_info: &NodeInfo) -> Result<()> {
        for _idx in 0..10 {
            let warehouse_snapshot = self.warehouse_snapshot(&node_info.warehouse_id).await?;

            let mut txn = TxnRequest::default();

            let node_key = self.node_key(node_info)?;
            let cluster_node_key = self.cluster_node_key(node_info)?;

            // Attempt to delete the warehouse info if the last node is shutting down.
            if warehouse_snapshot.snapshot_nodes.len() == 1
                && warehouse_snapshot.snapshot_nodes[0].node_info.id == node_info.id
            {
                let warehouse_info_key = self.warehouse_info_key(&node_info.warehouse_id)?;

                txn.condition.push(map_condition(
                    &warehouse_info_key,
                    MatchSeq::Exact(warehouse_snapshot.info_seq),
                ));
                txn.if_then.push(TxnOp::delete(warehouse_info_key));

                for node_info_snapshot in warehouse_snapshot.snapshot_nodes {
                    txn.condition.push(map_condition(
                        &node_key,
                        MatchSeq::Exact(node_info_snapshot.node_seq),
                    ));

                    txn.condition.push(map_condition(
                        &cluster_node_key,
                        MatchSeq::Exact(node_info_snapshot.cluster_seq),
                    ));
                }
            }

            txn.if_then.push(TxnOp::delete(node_key));
            txn.if_then.push(TxnOp::delete(cluster_node_key));

            if self
                .metastore
                .transaction(txn)
                .await
                .map_err(meta_service_error)?
                .success
            {
                return Ok(());
            }
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times).",
        ))
    }

    async fn unassigned_nodes(&self) -> Result<HashMap<Option<String>, Vec<(u64, NodeInfo)>>> {
        let online_nodes = self
            .metastore
            .list_kv_collect(ListOptions::unlimited(&self.node_key_prefix))
            .await
            .map_err(meta_service_error)?;
        let mut unassigned_nodes = HashMap::with_capacity(online_nodes.len());

        for (_, seq_data) in online_nodes {
            let node_info = serde_json::from_slice::<NodeInfo>(&seq_data.data)?;

            if !node_info.assigned_warehouse() {
                match unassigned_nodes.entry(node_info.node_group.clone()) {
                    Entry::Vacant(v) => {
                        v.insert(vec![(seq_data.seq, node_info)]);
                    }
                    Entry::Occupied(mut v) => {
                        v.get_mut().push((seq_data.seq, node_info));
                    }
                }
            }
        }

        Ok(unassigned_nodes)
    }

    async fn pick_assign_warehouse_node(
        &self,
        warehouse: &str,
        nodes_matcher: &HashMap<String, Vec<SelectedNode>>,
    ) -> Result<HashMap<String, Vec<(u64, NodeInfo)>>> {
        let mut selected_nodes = HashMap::with_capacity(nodes_matcher.len());

        let mut grouped_nodes = self.unassigned_nodes().await?;

        let mut after_assign_node = HashMap::new();

        for (cluster, cluster_node_matcher) in nodes_matcher {
            let mut cluster_selected_nodes = Vec::with_capacity(cluster_node_matcher.len());
            for selected_node in cluster_node_matcher {
                match selected_node {
                    SelectedNode::Random(None) => {
                        let Some(nodes_list) = grouped_nodes.get_mut(&None) else {
                            match after_assign_node.entry(cluster.clone()) {
                                Entry::Vacant(v) => {
                                    v.insert(1);
                                }
                                Entry::Occupied(mut v) => {
                                    *v.get_mut() += 1;
                                }
                            };

                            continue;
                        };

                        let Some((seq, mut node)) = nodes_list.pop() else {
                            grouped_nodes.remove(&None);
                            match after_assign_node.entry(cluster.clone()) {
                                Entry::Vacant(v) => {
                                    v.insert(1);
                                }
                                Entry::Occupied(mut v) => {
                                    *v.get_mut() += 1;
                                }
                            };

                            continue;
                        };

                        node.runtime_node_group = None;
                        node.cluster_id = cluster.clone();
                        node.warehouse_id = warehouse.to_string();
                        cluster_selected_nodes.push((seq, node));
                    }
                    SelectedNode::Random(Some(node_group)) => {
                        let key = Some(node_group.clone());
                        let Some(nodes_list) = grouped_nodes.get_mut(&key) else {
                            return Err(ErrorCode::NoResourcesAvailable(format!(
                                "Failed to create warehouse, reason: no resources available for {} group",
                                node_group
                            )));
                        };

                        let Some((seq, mut node)) = nodes_list.pop() else {
                            grouped_nodes.remove(&key);
                            return Err(ErrorCode::NoResourcesAvailable(format!(
                                "Failed to create warehouse, reason: no resources available for {} group",
                                node_group
                            )));
                        };

                        node.cluster_id = cluster.clone();
                        node.warehouse_id = warehouse.to_string();
                        node.runtime_node_group = Some(node_group.clone());
                        cluster_selected_nodes.push((seq, node));
                    }
                }
            }

            selected_nodes.insert(cluster.clone(), cluster_selected_nodes);
        }

        if !after_assign_node.is_empty() {
            let mut remain_nodes = Vec::new();

            let mut processed_data = true;
            while processed_data {
                processed_data = false;
                for nodes in grouped_nodes.values_mut() {
                    if let Some((seq, node)) = nodes.pop() {
                        processed_data = true;
                        remain_nodes.push((seq, node));
                    }
                }
            }

            for (cluster, remain_node) in after_assign_node {
                for _idx in 0..remain_node {
                    let Some((seq, mut node)) = remain_nodes.pop() else {
                        return Err(ErrorCode::NoResourcesAvailable(
                            "Failed to create warehouse, reason: no resources available.",
                        ));
                    };

                    node.cluster_id = cluster.clone();
                    node.warehouse_id = warehouse.to_string();
                    node.runtime_node_group = None;
                    selected_nodes.get_mut(&cluster).unwrap().push((seq, node));
                }
            }
        }

        Ok(selected_nodes)
    }
}

#[async_trait::async_trait]
impl WarehouseApi for WarehouseMgr {
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn start_node(&self, node: NodeInfo) -> Result<SeqV<NodeInfo>> {
        let start_reply = match node.node_type {
            NodeType::SelfManaged => self.start_self_managed(node.clone()).await?,
            NodeType::SystemManaged => self.start_system_managed(node.clone()).await?,
        };

        if !start_reply.success {
            return Err(ErrorCode::ClusterNodeAlreadyExists(format!(
                "Node with ID '{}' already exists in the cluster.",
                node.id
            )));
        }

        match start_reply.responses.last() {
            Some(TxnOpResponse {
                response:
                    Some(Response::Get(TxnGetResponse {
                        value: Some(seq_v), ..
                    })),
            }) => Ok(SeqV::new(seq_v.seq, serde_json::from_slice(&seq_v.data)?)),
            _ => Err(ErrorCode::MetaServiceError("Add node info failure.")),
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn shutdown_node(&self, node_id: String) -> Result<()> {
        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(&node_id)?);

        if let Some(info) = self
            .metastore
            .get_kv(&node_key)
            .await
            .map_err(meta_service_error)?
        {
            let node_info: NodeInfo = serde_json::from_slice(&info.data)?;

            return match node_info.node_type {
                NodeType::SelfManaged => self.shutdown_self_managed_node(&node_info).await,
                NodeType::SystemManaged => self.shutdown_system_managed(&node_info).await,
            };
        }

        Err(ErrorCode::ClusterUnknownNode(format!(
            "Node with ID '{}' does not exist in the cluster.",
            node_id
        )))
    }

    async fn heartbeat_node(&self, node: &mut NodeInfo, exact: u64) -> Result<u64> {
        let heartbeat_reply = match node.node_type {
            NodeType::SelfManaged => {
                assert!(!node.cluster_id.is_empty());
                assert!(!node.warehouse_id.is_empty());

                self.heartbeat_self_managed(node.clone(), exact).await?
            }
            NodeType::SystemManaged => {
                let reply = self.heartbeat_system_managed(node.clone(), exact).await?;

                if !reply.success {
                    return self.resolve_conflicts(reply, node).await;
                }

                reply
            }
        };

        match heartbeat_reply.responses.last() {
            Some(TxnOpResponse {
                response:
                    Some(Response::Get(TxnGetResponse {
                        value: Some(seq_v), ..
                    })),
            }) => Ok(seq_v.seq),
            // compatibility
            // After network fail, nodes may become expired due to failed heartbeats.
            // For system-managed nodes, this situation has already been handled in resolve_conflicts.
            // For self-managed nodes, we need to return seq = 0 so that the next heartbeat can proceed normally.
            _ if matches!(node.node_type, NodeType::SelfManaged) => Ok(0),
            _ => Err(ErrorCode::MetaServiceError("Heartbeat node info failure.")),
        }
    }

    async fn drop_warehouse(&self, warehouse: String) -> Result<WarehouseInfo> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        for _idx in 0..10 {
            let warehouse_snapshot = self.warehouse_snapshot(&warehouse).await?;

            if let WarehouseInfo::SelfManaged(_) = warehouse_snapshot.warehouse_info {
                return Err(ErrorCode::InvalidWarehouse(
                    "Cannot drop self-managed warehouse",
                ));
            }

            let mut delete_txn = TxnRequest::default();

            let warehouse_info_key = self.warehouse_info_key(&warehouse)?;

            delete_txn.condition.push(map_condition(
                &warehouse_info_key,
                MatchSeq::Exact(warehouse_snapshot.info_seq),
            ));
            delete_txn.if_then.push(TxnOp::delete(warehouse_info_key));

            for node_snapshot in warehouse_snapshot.snapshot_nodes {
                let node_key = self.node_key(&node_snapshot.node_info)?;
                let cluster_node_key = self.cluster_node_key(&node_snapshot.node_info)?;

                delete_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(node_snapshot.node_seq),
                ));
                delete_txn.condition.push(map_condition(
                    &cluster_node_key,
                    MatchSeq::Exact(node_snapshot.cluster_seq),
                ));

                delete_txn.if_then.push(TxnOp::delete(cluster_node_key));
                let leave_node = node_snapshot.node_info.leave_warehouse();
                delete_txn.if_then.push(TxnOp::put_with_ttl(
                    node_key,
                    serde_json::to_vec(&leave_node)?,
                    Some(self.lift_time * 4),
                ));
            }

            if !self
                .metastore
                .transaction(delete_txn)
                .await
                .map_err(meta_service_error)?
                .success
            {
                // seq is changed, will retry
                continue;
            }

            return Ok(warehouse_snapshot.warehouse_info);
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times).",
        ))
    }

    async fn create_warehouse(
        &self,
        warehouse: String,
        nodes: Vec<SelectedNode>,
    ) -> Result<WarehouseInfo> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        if nodes.is_empty() {
            return Err(ErrorCode::EmptyNodesForWarehouse(
                "Cannot create warehouse with empty nodes.",
            ));
        }

        let nodes_map = HashMap::from([(String::from(DEFAULT_CLUSTER_ID), nodes.clone())]);

        loop {
            let mut selected_nodes = self
                .pick_assign_warehouse_node(&warehouse, &nodes_map)
                .await?;
            let selected_nodes = selected_nodes.remove(DEFAULT_CLUSTER_ID).unwrap();

            let mut txn = TxnRequest::default();

            for (seq, node) in selected_nodes {
                let node_key = self.node_key(&node)?;

                txn.condition
                    .push(map_condition(&node_key, MatchSeq::Exact(seq)));
                txn.if_then.push(TxnOp::put_with_ttl(
                    node_key,
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time * 4),
                ));

                let cluster_node_key = self.cluster_node_key(&node)?;

                let node = node.unload_warehouse_info();
                txn.condition
                    .push(map_condition(&cluster_node_key, MatchSeq::Exact(0)));
                txn.if_then.push(TxnOp::put_with_ttl(
                    cluster_node_key,
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time * 4),
                ));
            }

            let warehouse_info_key = self.warehouse_info_key(&warehouse)?;

            let warehouse_info = WarehouseInfo::SystemManaged(SystemManagedWarehouse {
                role_id: GlobalUniq::unique(),
                status: "Running".to_string(),
                id: warehouse.clone(),
                clusters: HashMap::from([(
                    String::from(DEFAULT_CLUSTER_ID),
                    SystemManagedCluster {
                        nodes: nodes.clone(),
                    },
                )]),
            });

            txn.condition
                .push(map_condition(&warehouse_info_key, MatchSeq::Exact(0)));
            txn.if_then.push(TxnOp::put(
                warehouse_info_key.clone(),
                serde_json::to_vec(&warehouse_info)?,
            ));
            txn.else_then.push(TxnOp::get(warehouse_info_key));

            return match self
                .metastore
                .transaction(txn)
                .await
                .map_err(meta_service_error)?
            {
                res if res.success => Ok(warehouse_info),
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

    async fn resume_warehouse(&self, warehouse: String) -> Result<()> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        for _idx in 0..10 {
            let mut warehouse_info = self.warehouse_snapshot(&warehouse).await?;

            let mut need_schedule_cluster = HashMap::new();
            warehouse_info.warehouse_info = match warehouse_info.warehouse_info {
                WarehouseInfo::SelfManaged(_) => Err(ErrorCode::InvalidWarehouse(
                    "Cannot resume self-managed warehouse.",
                )),
                WarehouseInfo::SystemManaged(warehouse) => {
                    if warehouse.status.to_uppercase() != "SUSPENDED" {
                        return Err(ErrorCode::InvalidWarehouse(format!(
                            "Cannot resume warehouse {:?}, because warehouse state is not suspend",
                            warehouse
                        )));
                    }

                    // TODO: support cluster resume?
                    need_schedule_cluster = warehouse.clusters.clone();
                    Ok(WarehouseInfo::SystemManaged(SystemManagedWarehouse {
                        role_id: warehouse.role_id.clone(),
                        status: "Running".to_string(),
                        id: warehouse.id,
                        clusters: warehouse.clusters,
                    }))
                }
            }?;

            // get online nodes
            let online_nodes = self
                .metastore
                .list_kv_collect(ListOptions::unlimited(&self.node_key_prefix))
                .await
                .map_err(meta_service_error)?;

            let mut unassign_online_nodes = Vec::with_capacity(online_nodes.len());

            for (_key, v) in online_nodes {
                let node_info = serde_json::from_slice::<NodeInfo>(&v.data)?;

                if !node_info.assigned_warehouse() {
                    assert_eq!(node_info.node_type, NodeType::SystemManaged);
                    unassign_online_nodes.push((v.seq, node_info));
                }
            }

            let mut resume_txn = TxnRequest::default();
            for (cluster, info) in need_schedule_cluster {
                for selected_node in info.nodes {
                    match selected_node {
                        SelectedNode::Random(Some(_)) => {
                            return Err(ErrorCode::Unimplemented(
                                "Custom instance types are not supported.",
                            ));
                        }
                        SelectedNode::Random(None) => match unassign_online_nodes.pop() {
                            None => {
                                return Err(ErrorCode::NoResourcesAvailable(
                                    "Failed to create warehouse, reason: no resources available",
                                ));
                            }
                            Some((seq, mut node)) => {
                                node.cluster_id = cluster.clone();
                                node.warehouse_id = warehouse.clone();

                                let node_key = self.node_key(&node)?;
                                let cluster_node_key = self.cluster_node_key(&node)?;

                                resume_txn
                                    .condition
                                    .push(map_condition(&node_key, MatchSeq::Exact(seq)));

                                resume_txn.if_then.push(TxnOp::put_with_ttl(
                                    node_key,
                                    serde_json::to_vec(&node)?,
                                    Some(self.lift_time * 4),
                                ));

                                let node = node.unload_warehouse_info();
                                resume_txn
                                    .condition
                                    .push(map_condition(&cluster_node_key, MatchSeq::Exact(0)));
                                resume_txn.if_then.push(TxnOp::put_with_ttl(
                                    cluster_node_key,
                                    serde_json::to_vec(&node)?,
                                    Some(self.lift_time * 4),
                                ));
                            }
                        },
                    }
                }
            }

            let warehouse_info_key = self.warehouse_info_key(&warehouse)?;

            resume_txn.condition.push(map_condition(
                &warehouse_info_key,
                MatchSeq::Exact(warehouse_info.info_seq),
            ));
            resume_txn.if_then.push(TxnOp::put(
                warehouse_info_key.clone(),
                serde_json::to_vec(&warehouse_info.warehouse_info)?,
            ));

            if self
                .metastore
                .transaction(resume_txn)
                .await
                .map_err(meta_service_error)?
                .success
            {
                return Ok(());
            }
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times while in resume warehouse).",
        ))
    }

    async fn suspend_warehouse(&self, warehouse: String) -> Result<()> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        for _idx in 0..10 {
            let mut warehouse_snapshot = self.warehouse_snapshot(&warehouse).await?;

            warehouse_snapshot.warehouse_info = match warehouse_snapshot.warehouse_info {
                WarehouseInfo::SelfManaged(_) => Err(ErrorCode::InvalidWarehouse(
                    "Cannot suspend self-managed warehouse",
                )),
                WarehouseInfo::SystemManaged(warehouse) => {
                    if warehouse.status.to_uppercase() != "RUNNING" {
                        return Err(ErrorCode::InvalidWarehouse(format!(
                            "Cannot suspend warehouse {:?}, because warehouse state is not running.",
                            warehouse.id
                        )));
                    }

                    Ok(WarehouseInfo::SystemManaged(SystemManagedWarehouse {
                        role_id: warehouse.role_id.clone(),
                        status: "Suspended".to_string(),
                        id: warehouse.id,
                        clusters: warehouse.clusters,
                    }))
                }
            }?;

            let mut suspend_txn = TxnRequest::default();

            let warehouse_info_key = self.warehouse_info_key(&warehouse)?;

            suspend_txn.condition.push(map_condition(
                &warehouse_info_key,
                MatchSeq::Exact(warehouse_snapshot.info_seq),
            ));
            suspend_txn.if_then.push(TxnOp::put(
                warehouse_info_key,
                serde_json::to_vec(&warehouse_snapshot.warehouse_info)?,
            ));

            for node_snapshot in warehouse_snapshot.snapshot_nodes {
                let node_key = self.node_key(&node_snapshot.node_info)?;
                let cluster_node_key = self.cluster_node_key(&node_snapshot.node_info)?;

                suspend_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(node_snapshot.node_seq),
                ));
                suspend_txn.condition.push(map_condition(
                    &cluster_node_key,
                    MatchSeq::Exact(node_snapshot.cluster_seq),
                ));

                suspend_txn.if_then.push(TxnOp::delete(cluster_node_key));
                let node = node_snapshot.node_info.unload_warehouse_info();
                suspend_txn.if_then.push(TxnOp::put_with_ttl(
                    node_key,
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time * 4),
                ));
            }

            if self
                .metastore
                .transaction(suspend_txn)
                .await
                .map_err(meta_service_error)?
                .success
            {
                return Ok(());
            }
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times while in suspend warehouse).",
        ))
    }

    async fn list_warehouses(&self) -> Result<Vec<WarehouseInfo>> {
        let values = self
            .metastore
            .list_kv_collect(ListOptions::unlimited(&self.warehouse_info_key_prefix))
            .await
            .map_err(meta_service_error)?;

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
            let mut warehouse_snapshot = self.warehouse_snapshot(&current).await?;

            warehouse_snapshot.warehouse_info = match warehouse_snapshot.warehouse_info {
                WarehouseInfo::SelfManaged(_) => Err(ErrorCode::InvalidWarehouse(
                    "Cannot rename self-managed warehouse",
                )),
                WarehouseInfo::SystemManaged(mut info) => {
                    info.id = to.clone();
                    Ok(WarehouseInfo::SystemManaged(info))
                }
            }?;

            let mut rename_txn = TxnRequest::default();

            let old_warehouse_info_key = self.warehouse_info_key(&current)?;

            let new_warehouse_info_key = self.warehouse_info_key(&to)?;

            rename_txn.condition.push(map_condition(
                &old_warehouse_info_key,
                MatchSeq::Exact(warehouse_snapshot.info_seq),
            ));

            rename_txn
                .condition
                .push(map_condition(&new_warehouse_info_key, MatchSeq::Exact(0)));
            rename_txn
                .else_then
                .push(TxnOp::get(new_warehouse_info_key.clone()));

            rename_txn
                .if_then
                .push(TxnOp::delete(old_warehouse_info_key));
            rename_txn.if_then.push(TxnOp::put(
                new_warehouse_info_key,
                serde_json::to_vec(&warehouse_snapshot.warehouse_info)?,
            ));

            for mut node_snapshot in warehouse_snapshot.snapshot_nodes {
                let node_key = self.node_key(&node_snapshot.node_info)?;
                let old_cluster_node_key = self.cluster_node_key(&node_snapshot.node_info)?;

                node_snapshot.node_info.warehouse_id = to.clone();

                let new_cluster_node_key = self.cluster_node_key(&node_snapshot.node_info)?;

                rename_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(node_snapshot.node_seq),
                ));
                rename_txn.condition.push(map_condition(
                    &old_cluster_node_key,
                    MatchSeq::Exact(node_snapshot.cluster_seq),
                ));

                rename_txn
                    .condition
                    .push(map_condition(&new_cluster_node_key, MatchSeq::Exact(0)));

                rename_txn.if_then.push(TxnOp::put_with_ttl(
                    node_key,
                    serde_json::to_vec(&node_snapshot.node_info)?,
                    Some(self.lift_time * 4),
                ));

                let node = node_snapshot.node_info.unload_warehouse_info();
                rename_txn.if_then.push(TxnOp::delete(old_cluster_node_key));
                rename_txn.if_then.push(TxnOp::put_with_ttl(
                    new_cluster_node_key.clone(),
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time * 4),
                ));
            }

            return match self
                .metastore
                .transaction(rename_txn)
                .await
                .map_err(meta_service_error)?
            {
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

    async fn list_warehouse_nodes(&self, warehouse: String) -> Result<Vec<NodeInfo>> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        let nodes_prefix = format!(
            "{}/{}/",
            self.cluster_node_key_prefix,
            escape_for_key(&warehouse)?
        );

        let values = self
            .metastore
            .list_kv_collect(ListOptions::unlimited(&nodes_prefix))
            .await
            .map_err(meta_service_error)?;

        let mut nodes_info = Vec::with_capacity(values.len());
        for (node_key, value) in values {
            let mut node_info = serde_json::from_slice::<NodeInfo>(&value.data)?;

            let suffix = &node_key[nodes_prefix.len()..];

            let Some((cluster, node)) = suffix.split_once('/') else {
                return Err(ErrorCode::InvalidWarehouse(format!(
                    "Node key is invalid {:?}",
                    node_key
                )));
            };

            node_info.id = unescape_for_key(node)?;
            node_info.cluster_id = unescape_for_key(cluster)?;
            node_info.warehouse_id = warehouse.to_string();
            nodes_info.push(node_info);
        }

        Ok(nodes_info)
    }

    async fn add_warehouse_cluster(
        &self,
        warehouse: String,
        cluster: String,
        nodes: Vec<SelectedNode>,
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

        let nodes_map = HashMap::from([(cluster.clone(), nodes.clone())]);

        for _idx in 0..10 {
            let mut selected_nodes = self
                .pick_assign_warehouse_node(&warehouse, &nodes_map)
                .await?;
            let selected_nodes = selected_nodes.remove(&cluster).unwrap();

            let mut create_cluster_txn = TxnRequest::default();

            let mut warehouse_snapshot = self.warehouse_snapshot(&warehouse).await?;

            warehouse_snapshot.warehouse_info = match warehouse_snapshot.warehouse_info {
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
                            Ok(WarehouseInfo::SystemManaged(SystemManagedWarehouse {
                                role_id: info.role_id,
                                status: info.status,
                                id: info.id,
                                clusters: info.clusters,
                            }))
                        }
                    }
                }
            }?;

            let warehouse_info_key = self.warehouse_info_key(&warehouse)?;

            create_cluster_txn.condition.push(map_condition(
                &warehouse_info_key,
                MatchSeq::Exact(warehouse_snapshot.info_seq),
            ));

            create_cluster_txn.if_then.push(TxnOp::put(
                warehouse_info_key.clone(),
                serde_json::to_vec(&warehouse_snapshot.warehouse_info)?,
            ));

            // lock all cluster state
            for node_snapshot in warehouse_snapshot.snapshot_nodes {
                let node_key = self.node_key(&node_snapshot.node_info)?;
                let cluster_node_key = self.cluster_node_key(&node_snapshot.node_info)?;

                create_cluster_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(node_snapshot.node_seq),
                ));
                create_cluster_txn.condition.push(map_condition(
                    &cluster_node_key,
                    MatchSeq::Exact(node_snapshot.cluster_seq),
                ));
            }

            for (seq, node) in selected_nodes {
                let node_key = self.node_key(&node)?;
                let cluster_node_key = self.cluster_node_key(&node)?;

                create_cluster_txn
                    .condition
                    .push(map_condition(&node_key, MatchSeq::Exact(seq)));
                create_cluster_txn.if_then.push(TxnOp::put_with_ttl(
                    node_key,
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time * 4),
                ));

                let node = node.unload_warehouse_info();
                create_cluster_txn
                    .condition
                    .push(map_condition(&cluster_node_key, MatchSeq::Exact(0)));
                create_cluster_txn.if_then.push(TxnOp::put_with_ttl(
                    cluster_node_key,
                    serde_json::to_vec(&node)?,
                    Some(self.lift_time * 4),
                ));
            }

            return match self
                .metastore
                .transaction(create_cluster_txn)
                .await
                .map_err(meta_service_error)?
            {
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

            let mut warehouse_snapshot = self.warehouse_snapshot(&warehouse).await?;

            warehouse_snapshot.warehouse_info = match warehouse_snapshot.warehouse_info {
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
                                Ok(WarehouseInfo::SystemManaged(SystemManagedWarehouse {
                                    role_id: info.role_id,
                                    status: info.status,
                                    id: info.id,
                                    clusters: info.clusters,
                                }))
                            }
                        },
                    }
                }
            }?;

            let warehouse_info_key = self.warehouse_info_key(&warehouse)?;

            drop_cluster_txn.condition.push(map_condition(
                &warehouse_info_key,
                MatchSeq::Exact(warehouse_snapshot.info_seq),
            ));

            drop_cluster_txn.if_then.push(TxnOp::put(
                warehouse_info_key.clone(),
                serde_json::to_vec(&warehouse_snapshot.warehouse_info)?,
            ));

            // lock all cluster state
            for node_snapshot in warehouse_snapshot.snapshot_nodes {
                let node_key = self.node_key(&node_snapshot.node_info)?;
                let cluster_node_key = self.cluster_node_key(&node_snapshot.node_info)?;

                drop_cluster_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(node_snapshot.node_seq),
                ));
                drop_cluster_txn.condition.push(map_condition(
                    &cluster_node_key,
                    MatchSeq::Exact(node_snapshot.cluster_seq),
                ));

                if node_snapshot.node_info.cluster_id == cluster {
                    let leave_node = node_snapshot.node_info.leave_warehouse();

                    drop_cluster_txn
                        .if_then
                        .push(TxnOp::delete(cluster_node_key));
                    drop_cluster_txn.if_then.push(TxnOp::put_with_ttl(
                        node_key,
                        serde_json::to_vec(&leave_node)?,
                        Some(self.lift_time * 4),
                    ))
                }
            }

            return match self
                .metastore
                .transaction(drop_cluster_txn)
                .await
                .map_err(meta_service_error)?
            {
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

            let mut warehouse_snapshot = self.warehouse_snapshot(&warehouse).await?;

            warehouse_snapshot.warehouse_info = match warehouse_snapshot.warehouse_info {
                WarehouseInfo::SelfManaged(_) => Err(ErrorCode::InvalidWarehouse(format!(
                    "Cannot rename cluster for warehouse {:?}, because it's self-managed warehouse.",
                    warehouse
                ))),
                WarehouseInfo::SystemManaged(mut info) => match info.clusters.contains_key(&cur) {
                    false => Err(ErrorCode::WarehouseClusterNotExists(format!(
                        "Warehouse cluster {:?}.{:?} not exists",
                        warehouse, cur
                    ))),
                    true => match info.clusters.contains_key(&to) {
                        true => Err(ErrorCode::WarehouseClusterAlreadyExists(format!(
                            "Warehouse cluster {:?}.{:?} already exists",
                            warehouse, to
                        ))),
                        false => {
                            let cluster_info = info.clusters.remove(&cur);
                            info.clusters.insert(to.clone(), cluster_info.unwrap());
                            Ok(WarehouseInfo::SystemManaged(SystemManagedWarehouse {
                                role_id: info.role_id,
                                status: info.status,
                                id: info.id,
                                clusters: info.clusters,
                            }))
                        }
                    },
                },
            }?;

            let warehouse_info_key = self.warehouse_info_key(&warehouse)?;

            rename_cluster_txn.condition.push(map_condition(
                &warehouse_info_key,
                MatchSeq::Exact(warehouse_snapshot.info_seq),
            ));

            rename_cluster_txn.if_then.push(TxnOp::put(
                warehouse_info_key.clone(),
                serde_json::to_vec(&warehouse_snapshot.warehouse_info)?,
            ));

            // lock all cluster state
            for mut node_snapshot in warehouse_snapshot.snapshot_nodes {
                let node_key = self.node_key(&node_snapshot.node_info)?;
                let old_cluster_node_key = self.cluster_node_key(&node_snapshot.node_info)?;

                rename_cluster_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(node_snapshot.node_seq),
                ));
                rename_cluster_txn.condition.push(map_condition(
                    &old_cluster_node_key,
                    MatchSeq::Exact(node_snapshot.cluster_seq),
                ));

                if node_snapshot.node_info.cluster_id == cur {
                    // rename node
                    node_snapshot.node_info.cluster_id = to.clone();
                    let new_cluster_node_key = self.cluster_node_key(&node_snapshot.node_info)?;

                    rename_cluster_txn.if_then.push(TxnOp::put_with_ttl(
                        node_key,
                        serde_json::to_vec(&node_snapshot.node_info)?,
                        Some(self.lift_time * 4),
                    ));

                    node_snapshot.node_info.cluster_id = String::new();
                    rename_cluster_txn
                        .condition
                        .push(map_condition(&new_cluster_node_key, MatchSeq::Exact(0)));
                    rename_cluster_txn
                        .if_then
                        .push(TxnOp::delete(old_cluster_node_key));
                    rename_cluster_txn.if_then.push(TxnOp::put_with_ttl(
                        new_cluster_node_key,
                        serde_json::to_vec(&node_snapshot.node_info)?,
                        Some(self.lift_time * 4),
                    ));
                }
            }

            return match self
                .metastore
                .transaction(rename_cluster_txn)
                .await
                .map_err(meta_service_error)?
            {
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

    async fn assign_warehouse_nodes(
        &self,
        warehouse: String,
        nodes: HashMap<String, Vec<SelectedNode>>,
    ) -> Result<()> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        if nodes.is_empty() {
            return Err(ErrorCode::EmptyNodesForWarehouse(
                "Cannot assign warehouse nodes with empty nodes list.",
            ));
        }

        if nodes.iter().any(|(name, _)| name.is_empty()) {
            return Err(ErrorCode::BadArguments("Assign cluster name is empty."));
        }

        if nodes.iter().any(|(_, list)| list.is_empty()) {
            return Err(ErrorCode::BadArguments(
                "Assign cluster nodes list is empty.",
            ));
        }

        for _idx in 0..10 {
            let selected_nodes = self.pick_assign_warehouse_node(&warehouse, &nodes).await?;

            let mut add_cluster_node_txn = TxnRequest::default();

            let mut warehouse_snapshot = self.warehouse_snapshot(&warehouse).await?;

            warehouse_snapshot.warehouse_info = match warehouse_snapshot.warehouse_info {
                WarehouseInfo::SelfManaged(_) => Err(ErrorCode::InvalidWarehouse(format!(
                    "Cannot assign nodes for warehouse {:?}, because it's self-managed warehouse.",
                    warehouse
                ))),
                WarehouseInfo::SystemManaged(mut info) => {
                    for (cluster, nodes) in &nodes {
                        let Some(cluster_info) = info.clusters.get_mut(cluster) else {
                            return Err(ErrorCode::WarehouseClusterNotExists(format!(
                                "Warehouse cluster {:?}.{:?} not exists",
                                warehouse, cluster
                            )));
                        };

                        cluster_info.nodes.extend(nodes.clone());
                    }

                    Ok(WarehouseInfo::SystemManaged(SystemManagedWarehouse {
                        role_id: info.role_id,
                        status: info.status,
                        id: info.id,
                        clusters: info.clusters,
                    }))
                }
            }?;

            let warehouse_info_key = self.warehouse_info_key(&warehouse)?;

            add_cluster_node_txn.condition.push(map_condition(
                &warehouse_info_key,
                MatchSeq::Exact(warehouse_snapshot.info_seq),
            ));

            add_cluster_node_txn.if_then.push(TxnOp::put(
                warehouse_info_key.clone(),
                serde_json::to_vec(&warehouse_snapshot.warehouse_info)?,
            ));

            // lock all cluster state
            for node_snapshot in warehouse_snapshot.snapshot_nodes {
                let node_key = self.node_key(&node_snapshot.node_info)?;
                let cluster_node_key = self.cluster_node_key(&node_snapshot.node_info)?;

                add_cluster_node_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(node_snapshot.node_seq),
                ));
                add_cluster_node_txn.condition.push(map_condition(
                    &cluster_node_key,
                    MatchSeq::Exact(node_snapshot.cluster_seq),
                ));
            }

            for selected_nodes in selected_nodes.into_values() {
                for (seq, node) in selected_nodes {
                    let node_key = self.node_key(&node)?;
                    let cluster_node_key = self.cluster_node_key(&node)?;

                    add_cluster_node_txn
                        .condition
                        .push(map_condition(&node_key, MatchSeq::Exact(seq)));
                    add_cluster_node_txn.if_then.push(TxnOp::put_with_ttl(
                        node_key,
                        serde_json::to_vec(&node)?,
                        Some(self.lift_time * 4),
                    ));

                    let node = node.unload_warehouse_info();
                    add_cluster_node_txn
                        .condition
                        .push(map_condition(&cluster_node_key, MatchSeq::Exact(0)));
                    add_cluster_node_txn.if_then.push(TxnOp::put_with_ttl(
                        cluster_node_key,
                        serde_json::to_vec(&node)?,
                        Some(self.lift_time * 4),
                    ));
                }
            }

            return match self
                .metastore
                .transaction(add_cluster_node_txn)
                .await
                .map_err(meta_service_error)?
            {
                res if res.success => Ok(()),
                _res => {
                    continue;
                }
            };
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times while in assign warehouse nodes).",
        ))
    }

    async fn unassign_warehouse_nodes(
        &self,
        warehouse: &str,
        nodes: HashMap<String, Vec<SelectedNode>>,
    ) -> Result<()> {
        if warehouse.is_empty() {
            return Err(ErrorCode::InvalidWarehouse("Warehouse name is empty."));
        }

        if nodes.is_empty() {
            return Err(ErrorCode::BadArguments("Unassign list is empty."));
        }

        if nodes.iter().any(|(name, _)| name.is_empty()) {
            return Err(ErrorCode::BadArguments("Unassign cluster name is empty."));
        }

        if nodes.iter().any(|(_, list)| list.is_empty()) {
            return Err(ErrorCode::BadArguments(
                "Unassign cluster nodes list is empty.",
            ));
        }

        for _idx in 0..10 {
            let mut removed_nodes = HashSet::new();
            let mut drop_cluster_node_txn = TxnRequest::default();

            let mut warehouse_snapshot = self.warehouse_snapshot(warehouse).await?;

            warehouse_snapshot.warehouse_info = match warehouse_snapshot.warehouse_info {
                WarehouseInfo::SelfManaged(_) => Err(ErrorCode::InvalidWarehouse(format!(
                    "Cannot unassign nodes for warehouse {:?}, because it's self-managed warehouse.",
                    warehouse
                ))),
                WarehouseInfo::SystemManaged(mut info) => {
                    for (cluster_id, nodes) in &nodes {
                        let Some(cluster) = info.clusters.get_mut(cluster_id) else {
                            return Err(ErrorCode::WarehouseClusterNotExists(format!(
                                "Warehouse cluster {:?}.{:?} not exists",
                                warehouse, cluster_id
                            )));
                        };

                        if cluster.nodes.len() == nodes.len() {
                            return Err(ErrorCode::EmptyNodesForWarehouse(format!(
                                "Cannot unassign all nodes for warehouse cluster {:?}.{:?}",
                                warehouse, cluster
                            )));
                        }

                        removed_nodes = info.remove_nodes(
                            cluster_id,
                            nodes,
                            &warehouse_snapshot.snapshot_nodes,
                        )?;
                    }

                    Ok(WarehouseInfo::SystemManaged(SystemManagedWarehouse {
                        role_id: info.role_id,
                        status: info.status,
                        id: info.id,
                        clusters: info.clusters,
                    }))
                }
            }?;

            let warehouse_info_key = self.warehouse_info_key(warehouse)?;

            drop_cluster_node_txn.condition.push(map_condition(
                &warehouse_info_key,
                MatchSeq::Exact(warehouse_snapshot.info_seq),
            ));

            drop_cluster_node_txn.if_then.push(TxnOp::put(
                warehouse_info_key.clone(),
                serde_json::to_vec(&warehouse_snapshot.warehouse_info)?,
            ));

            // lock all cluster state
            for node_snapshot in warehouse_snapshot.snapshot_nodes {
                let node_key = self.node_key(&node_snapshot.node_info)?;
                let cluster_node_key = self.cluster_node_key(&node_snapshot.node_info)?;

                drop_cluster_node_txn.condition.push(map_condition(
                    &node_key,
                    MatchSeq::Exact(node_snapshot.node_seq),
                ));
                drop_cluster_node_txn.condition.push(map_condition(
                    &cluster_node_key,
                    MatchSeq::Exact(node_snapshot.cluster_seq),
                ));

                if removed_nodes.contains(&node_snapshot.node_info.id) {
                    let node = node_snapshot.node_info.leave_warehouse();

                    drop_cluster_node_txn
                        .if_then
                        .push(TxnOp::delete(cluster_node_key));
                    drop_cluster_node_txn.if_then.push(TxnOp::put_with_ttl(
                        node_key,
                        serde_json::to_vec(&node)?,
                        Some(self.lift_time * 4),
                    ));
                }
            }

            let txn_reply = self
                .metastore
                .transaction(drop_cluster_node_txn)
                .await
                .map_err(meta_service_error)?;

            if txn_reply.success {
                return Ok(());
            }
        }

        Err(ErrorCode::WarehouseOperateConflict(
            "Warehouse operate conflict(tried 10 times while in unassign warehouse nodes).",
        ))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn list_warehouse_cluster_nodes(
        &self,
        warehouse: &str,
        cluster: &str,
    ) -> Result<Vec<NodeInfo>> {
        let cluster_nodes_prefix = format!(
            "{}/{}/{}/",
            self.cluster_node_key_prefix,
            escape_for_key(warehouse)?,
            escape_for_key(cluster)?
        );

        let values = self
            .metastore
            .list_kv_collect(ListOptions::unlimited(&cluster_nodes_prefix))
            .await
            .map_err(meta_service_error)?;

        let mut nodes_info = Vec::with_capacity(values.len());
        for (node_key, value) in values {
            let mut node_info = serde_json::from_slice::<NodeInfo>(&value.data)?;

            node_info.id = unescape_for_key(&node_key[cluster_nodes_prefix.len()..])?;
            node_info.cluster_id = cluster.to_string();
            node_info.warehouse_id = warehouse.to_string();
            nodes_info.push(node_info);
        }

        Ok(nodes_info)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_local_addr(&self) -> Result<String> {
        Ok(self
            .metastore
            .get_local_addr()
            .await
            .map_err(meta_service_error)?)
    }

    async fn list_online_nodes(&self) -> Result<Vec<NodeInfo>> {
        let reply = self
            .metastore
            .list_kv_collect(ListOptions::unlimited(&self.node_key_prefix))
            .await
            .map_err(meta_service_error)?;
        let mut online_nodes = Vec::with_capacity(reply.len());

        for (_, seq_data) in reply {
            online_nodes.push(serde_json::from_slice::<NodeInfo>(&seq_data.data)?);
        }

        Ok(online_nodes)
    }

    // Discovers and returns all the nodes that are in the same cluster with the given node, including the provided one.
    //
    // If the given node is not in a cluster yet, just return itself.
    async fn discover(&self, node_id: &str) -> Result<Vec<NodeInfo>> {
        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(node_id)?);

        let Some(seq) = self
            .metastore
            .get_kv(&node_key)
            .await
            .map_err(meta_service_error)?
        else {
            return Err(ErrorCode::NotFoundClusterNode(format!(
                "Node {} is offline, Please restart this node.",
                node_id
            )));
        };

        let node = serde_json::from_slice::<NodeInfo>(&seq.data)?;

        if !node.assigned_warehouse() {
            return Ok(vec![node]);
        }

        Ok(self
            .list_warehouse_cluster_nodes(&node.warehouse_id, &node.cluster_id)
            .await?
            .into_iter()
            .filter(|x| x.binary_version == self.version.commit_detail)
            .collect::<Vec<_>>())
    }

    async fn discover_warehouse_nodes(&self, node_id: &str) -> Result<Vec<NodeInfo>> {
        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(node_id)?);

        let Some(seq) = self
            .metastore
            .get_kv(&node_key)
            .await
            .map_err(meta_service_error)?
        else {
            return Err(ErrorCode::NotFoundClusterNode(format!(
                "Node {} is offline, Please restart this node.",
                node_id
            )));
        };

        let node = serde_json::from_slice::<NodeInfo>(&seq.data)?;

        if !node.assigned_warehouse() {
            return Ok(vec![node]);
        }

        Ok(self
            .list_warehouse_nodes(node.warehouse_id.clone())
            .await?
            .into_iter()
            .filter(|x| x.binary_version == self.version.commit_detail)
            .collect::<Vec<_>>())
    }

    async fn get_node_info(&self, node_id: &str) -> Result<Option<NodeInfo>> {
        let node_key = format!("{}/{}", self.node_key_prefix, escape_for_key(node_id)?);
        match self
            .metastore
            .get_kv(&node_key)
            .await
            .map_err(meta_service_error)?
        {
            None => Ok(None),
            Some(seq) => Ok(Some(serde_json::from_slice(&seq.data)?)),
        }
    }
}

impl SystemManagedWarehouse {
    pub fn remove_nodes(
        &mut self,
        cluster_id: &String,
        unassign: &[SelectedNode],
        nodes: &[NodeInfoSnapshot],
    ) -> Result<HashSet<String>> {
        let mut final_removed_nodes = HashSet::new();
        let mut match_any = Vec::with_capacity(unassign.len());
        let mut match_node_group = Vec::with_capacity(unassign.len());

        let Some(cluster) = self.clusters.get_mut(cluster_id) else {
            unreachable!()
        };

        // 1. assign node group == unassign node group
        for node in unassign {
            if cluster.nodes.remove_first(node).is_none() {
                match node {
                    SelectedNode::Random(None) => {
                        match_any.push(node);
                    }
                    SelectedNode::Random(Some(node)) => {
                        match_node_group.push(Some(node.clone()));
                    }
                }

                continue;
            }

            for node_snapshot in nodes {
                if &node_snapshot.node_info.cluster_id != cluster_id {
                    continue;
                }

                match (&node, &node_snapshot.node_info.runtime_node_group) {
                    (SelectedNode::Random(None), None) => {
                        if !final_removed_nodes.contains(&node_snapshot.node_info.id) {
                            final_removed_nodes.insert(node_snapshot.node_info.id.clone());
                            break;
                        }
                    }
                    (SelectedNode::Random(Some(left)), Some(right)) if left == right => {
                        if !final_removed_nodes.contains(&node_snapshot.node_info.id) {
                            final_removed_nodes.insert(node_snapshot.node_info.id.clone());
                            break;
                        }
                    }
                    _ => {
                        continue;
                    }
                }
            }
        }

        // 2. unassign Some(node group), assign None
        'match_node_group: for node_group in match_node_group {
            for node_snapshot in nodes {
                if &node_snapshot.node_info.cluster_id != cluster_id {
                    continue;
                }

                if node_snapshot.node_info.node_group == node_group
                    && !final_removed_nodes.contains(&node_snapshot.node_info.id)
                {
                    final_removed_nodes.insert(node_snapshot.node_info.id.clone());
                    cluster.nodes.remove_first(&SelectedNode::Random(
                        node_snapshot.node_info.runtime_node_group.clone(),
                    ));
                    continue 'match_node_group;
                }
            }

            return Err(ErrorCode::ClusterUnknownNode(format!(
                "Cannot found {:?} node group node in {:?}",
                node_group, cluster
            )));
        }

        // 3. assign Some(node group) and unassign None
        'match_any: for _index in 0..match_any.len() {
            for node_snapshot in nodes {
                if &node_snapshot.node_info.cluster_id != cluster_id {
                    continue;
                }

                if !final_removed_nodes.contains(&node_snapshot.node_info.id) {
                    final_removed_nodes.insert(node_snapshot.node_info.id.clone());
                    cluster.nodes.remove_first(&SelectedNode::Random(
                        node_snapshot.node_info.runtime_node_group.clone(),
                    ));
                    continue 'match_any;
                }
            }

            return Err(ErrorCode::ClusterUnknownNode(
                "Cannot unassign empty warehouse cluster",
            ));
        }

        Ok(final_removed_nodes)
    }
}

/// Build an error indicating that databend-meta responded with an unexpected response,
/// while expecting a TxnGetResponse.
fn invalid_get_resp(resp: Option<&TxnOpResponse>) -> MetaError {
    let invalid = InvalidReply::new(
        "Invalid response while upsert self managed node",
        &AnyError::error("Expect TxnGetResponse"),
    );
    error!("{} got: {:?}", invalid, resp);
    MetaError::from(invalid)
}
