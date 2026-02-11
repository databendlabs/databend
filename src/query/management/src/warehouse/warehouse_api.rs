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

use databend_common_exception::Result;
use databend_meta_types::NodeInfo;
use databend_meta_types::SeqV;

/// Databend-query cluster ID.
///
/// A cluster is a collection of databend nodes for computation.
pub type ClusterId = String;

/// Name of a node group.
///
/// It is used to filter nodes in a warehouse when creating a cluster.
pub type NodeGroupName = String;

/// Specifies how to select nodes in a warehouse without exposing node details.
#[derive(serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq, Debug)]
pub enum SelectedNode {
    /// Select a random online node from the tenant's node list.
    /// If `NodeGroupName` is specified, only nodes with this group name will be selected.
    Random(Option<NodeGroupName>),
}

/// Contains warehouse metadata that applies to both self-managed and system-managed warehouses.
///
/// This enum allows uniform handling of different warehouse types while maintaining their
/// distinct properties.
#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug)]
pub enum WarehouseInfo {
    /// A warehouse managed by the tenant, i.e., the cluster id is statically configured in config file.
    SelfManaged(ClusterId),
    /// A warehouse managed by the system, i.e., clusters in it can be dynamically assigned and updated.
    SystemManaged(SystemManagedWarehouse),
}

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct SystemManagedCluster {
    pub nodes: Vec<SelectedNode>,
}

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug)]
pub struct SystemManagedWarehouse {
    pub id: String,
    pub role_id: String,
    pub status: String,
    pub clusters: HashMap<String, SystemManagedCluster>,
}

/// Databend-query cluster management API
#[async_trait::async_trait]
pub trait WarehouseApi: Sync + Send {
    /// Start a new node.
    async fn start_node(&self, node: NodeInfo) -> Result<SeqV<NodeInfo>>;

    /// Shutdown the tenant's cluster one node by node.id.
    async fn shutdown_node(&self, node_id: String) -> Result<()>;

    /// Keep the tenant's cluster node alive.
    async fn heartbeat_node(&self, node: &mut NodeInfo, seq: u64) -> Result<u64>;

    async fn drop_warehouse(&self, warehouse: String) -> Result<WarehouseInfo>;

    async fn create_warehouse(
        &self,
        warehouse: String,
        nodes: Vec<SelectedNode>,
    ) -> Result<WarehouseInfo>;

    async fn resume_warehouse(&self, warehouse: String) -> Result<()>;

    async fn suspend_warehouse(&self, warehouse: String) -> Result<()>;

    async fn list_warehouses(&self) -> Result<Vec<WarehouseInfo>>;

    async fn rename_warehouse(&self, cur: String, to: String) -> Result<()>;

    async fn list_warehouse_nodes(&self, warehouse: String) -> Result<Vec<NodeInfo>>;

    async fn add_warehouse_cluster(
        &self,
        warehouse: String,
        cluster: String,
        nodes: Vec<SelectedNode>,
    ) -> Result<()>;

    async fn drop_warehouse_cluster(&self, warehouse: String, cluster: String) -> Result<()>;

    async fn rename_warehouse_cluster(
        &self,
        warehouse: String,
        cur: String,
        to: String,
    ) -> Result<()>;

    async fn assign_warehouse_nodes(
        &self,
        name: String,
        nodes: HashMap<String, Vec<SelectedNode>>,
    ) -> Result<()>;

    async fn unassign_warehouse_nodes(
        &self,
        warehouse: &str,
        nodes: HashMap<String, Vec<SelectedNode>>,
    ) -> Result<()>;

    /// Get the tenant's cluster all nodes.
    async fn list_warehouse_cluster_nodes(
        &self,
        warehouse: &str,
        cluster: &str,
    ) -> Result<Vec<NodeInfo>>;

    async fn get_local_addr(&self) -> Result<String>;

    async fn list_online_nodes(&self) -> Result<Vec<NodeInfo>>;

    async fn discover(&self, node_id: &str) -> Result<Vec<NodeInfo>>;

    async fn discover_warehouse_nodes(&self, node_id: &str) -> Result<Vec<NodeInfo>>;

    async fn get_node_info(&self, node_id: &str) -> Result<Option<NodeInfo>>;
}
