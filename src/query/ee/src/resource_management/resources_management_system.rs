// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_management::SelectedNode;
use databend_common_management::WarehouseApi;
use databend_common_management::WarehouseInfo;
use databend_enterprise_resources_management::ResourcesManagement;
use databend_meta_types::NodeInfo;
use databend_meta_types::NodeType;

pub struct SystemResourcesManagement {
    pub warehouse_manager: Arc<dyn WarehouseApi>,
}

impl SystemResourcesManagement {
    pub fn create(warehouse_mgr: Arc<dyn WarehouseApi>) -> Result<Arc<dyn ResourcesManagement>> {
        Ok(Arc::new(SystemResourcesManagement {
            warehouse_manager: warehouse_mgr,
        }))
    }
}

#[async_trait::async_trait]
impl ResourcesManagement for SystemResourcesManagement {
    fn support_forward_warehouse_request(&self) -> bool {
        true
    }

    async fn init_node(&self, node: &mut NodeInfo) -> Result<()> {
        let config = GlobalConfig::instance();
        assert!(config.query.common.cluster_id.is_empty());
        assert!(config.query.common.warehouse_id.is_empty());
        assert!(config.query.common.resources_management.is_some());

        if let Some(resources_management) = &config.query.common.resources_management {
            assert_eq!(
                &resources_management.typ.to_ascii_lowercase(),
                "system_managed"
            );

            node.cluster_id = String::new();
            node.warehouse_id = String::new();
            node.node_type = NodeType::SystemManaged;
            node.node_group = resources_management.node_group.clone();
        }

        Ok(())
    }

    async fn create_warehouse(
        &self,
        name: String,
        nodes: Vec<SelectedNode>,
    ) -> Result<WarehouseInfo> {
        self.warehouse_manager.create_warehouse(name, nodes).await
    }

    async fn drop_warehouse(&self, name: String) -> Result<WarehouseInfo> {
        self.warehouse_manager.drop_warehouse(name).await
    }

    async fn resume_warehouse(&self, name: String) -> Result<()> {
        self.warehouse_manager.resume_warehouse(name).await
    }

    async fn suspend_warehouse(&self, name: String) -> Result<()> {
        self.warehouse_manager.suspend_warehouse(name).await
    }

    async fn rename_warehouse(&self, name: String, to: String) -> Result<()> {
        self.warehouse_manager.rename_warehouse(name, to).await
    }

    async fn inspect_warehouse(&self, name: String) -> Result<Vec<NodeInfo>> {
        self.warehouse_manager.list_warehouse_nodes(name).await
    }

    async fn list_warehouses(&self) -> Result<Vec<WarehouseInfo>> {
        self.warehouse_manager.list_warehouses().await
    }

    async fn add_warehouse_cluster(
        &self,
        name: String,
        cluster: String,
        nodes: Vec<SelectedNode>,
    ) -> Result<()> {
        self.warehouse_manager
            .add_warehouse_cluster(name, cluster, nodes)
            .await
    }

    async fn rename_warehouse_cluster(
        &self,
        name: String,
        cluster: String,
        new_cluster: String,
    ) -> Result<()> {
        self.warehouse_manager
            .rename_warehouse_cluster(name, cluster, new_cluster)
            .await
    }

    async fn drop_warehouse_cluster(&self, name: String, cluster: String) -> Result<()> {
        self.warehouse_manager
            .drop_warehouse_cluster(name, cluster)
            .await
    }

    async fn assign_warehouse_nodes(
        &self,
        name: String,
        nodes: HashMap<String, Vec<SelectedNode>>,
    ) -> Result<()> {
        self.warehouse_manager
            .assign_warehouse_nodes(name, nodes)
            .await
    }

    async fn unassign_warehouse_nodes(
        &self,
        name: String,
        nodes: HashMap<String, Vec<SelectedNode>>,
    ) -> Result<()> {
        self.warehouse_manager
            .unassign_warehouse_nodes(&name, nodes)
            .await
    }

    async fn list_online_nodes(&self) -> Result<Vec<NodeInfo>> {
        self.warehouse_manager.list_online_nodes().await
    }
}
