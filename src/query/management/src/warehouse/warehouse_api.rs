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
use databend_common_meta_types::NodeInfo;

#[derive(serde::Serialize, serde::Deserialize, Clone, Eq, PartialEq, Debug)]
pub enum SelectedNode {
    Random(Option<String>),
}

pub type SelectedNodes = Vec<SelectedNode>;

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug)]
pub enum WarehouseInfo {
    SelfManaged(String),
    SystemManaged(SystemManagedWarehouse),
}

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug, Clone)]
pub struct SystemManagedCluster {
    pub nodes: Vec<SelectedNode>,
}

#[derive(serde::Serialize, serde::Deserialize, Eq, PartialEq, Debug)]
pub struct SystemManagedWarehouse {
    pub id: String,
    pub status: String,
    pub display_name: String,
    pub clusters: HashMap<String, SystemManagedCluster>,
}

/// Databend-query cluster management API
#[async_trait::async_trait]
pub trait WarehouseApi: Sync + Send {
    /// Start a new node.
    async fn start_node(&self, node: NodeInfo) -> Result<u64>;

    /// Shutdown the tenant's cluster one node by node.id.
    async fn shutdown_node(&self, node_id: String) -> Result<()>;

    /// Keep the tenant's cluster node alive.
    async fn heartbeat_node(&self, node: &mut NodeInfo, seq: u64) -> Result<u64>;

    async fn drop_warehouse(&self, warehouse: String) -> Result<()>;

    async fn create_warehouse(&self, warehouse: String, nodes: SelectedNodes) -> Result<()>;

    async fn resume_warehouse(&self, warehouse: String) -> Result<()>;

    async fn suspend_warehouse(&self, warehouse: String) -> Result<()>;

    async fn list_warehouses(&self) -> Result<Vec<WarehouseInfo>>;

    async fn rename_warehouse(&self, cur: String, to: String) -> Result<()>;

    async fn list_warehouse_nodes(&self, warehouse: String) -> Result<Vec<NodeInfo>>;

    async fn add_warehouse_cluster(
        &self,
        warehouse: String,
        cluster: String,
        nodes: SelectedNodes,
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
        nodes: HashMap<String, SelectedNodes>,
    ) -> Result<()>;

    async fn unassign_warehouse_nodes(
        &self,
        warehouse: &str,
        nodes: HashMap<String, SelectedNodes>,
    ) -> Result<()>;

    /// Get the tenant's cluster all nodes.
    async fn list_warehouse_cluster_nodes(
        &self,
        warehouse: &str,
        cluster: &str,
    ) -> Result<Vec<NodeInfo>>;

    async fn get_local_addr(&self) -> Result<Option<String>>;

    async fn get_node_info(&self, node_id: &str) -> Result<NodeInfo>;
}
