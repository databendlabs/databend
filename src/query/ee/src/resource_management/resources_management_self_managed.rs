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
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::SelectedNode;
use databend_common_management::WarehouseInfo;
use databend_enterprise_resources_management::ResourcesManagement;
use databend_meta_types::NodeInfo;
use databend_meta_types::NodeType;

pub struct SelfManagedResourcesManagement {}

impl SelfManagedResourcesManagement {
    pub fn create(cfg: &InnerConfig) -> Result<Arc<dyn ResourcesManagement>> {
        if cfg.query.common.cluster_id.is_empty() || cfg.query.common.warehouse_id.is_empty() {
            return Err(ErrorCode::InvalidConfig(
                "cluster_id or warehouse_id is empty with self-managed resources management",
            ));
        }

        Ok(Arc::new(SelfManagedResourcesManagement {}))
    }
}

#[async_trait::async_trait]
impl ResourcesManagement for SelfManagedResourcesManagement {
    fn support_forward_warehouse_request(&self) -> bool {
        false
    }

    async fn init_node(&self, node: &mut NodeInfo) -> Result<()> {
        let config = GlobalConfig::instance();
        node.cluster_id = config.query.common.cluster_id.clone();
        node.warehouse_id = config.query.common.warehouse_id.clone();
        node.node_type = NodeType::SelfManaged;
        Ok(())
    }

    async fn create_warehouse(&self, _: String, _: Vec<SelectedNode>) -> Result<WarehouseInfo> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented create warehouse with self-managed resources management",
        ))
    }

    async fn drop_warehouse(&self, _: String) -> Result<WarehouseInfo> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented drop warehouse with self-managed resources management",
        ))
    }

    async fn resume_warehouse(&self, _: String) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented resume warehouse with self-managed resources management",
        ))
    }

    async fn suspend_warehouse(&self, _: String) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented suspend warehouse with self-managed resources management",
        ))
    }

    async fn rename_warehouse(&self, _: String, _: String) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented rename warehouse with self-managed resources management",
        ))
    }

    async fn inspect_warehouse(&self, _: String) -> Result<Vec<NodeInfo>> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented inspect warehouse with self-managed resources management",
        ))
    }

    async fn list_warehouses(&self) -> Result<Vec<WarehouseInfo>> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented list warehouses with self-managed resources management",
        ))
    }

    async fn add_warehouse_cluster(
        &self,
        _: String,
        _: String,
        _: Vec<SelectedNode>,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented add warehouse cluster with self-managed resources management",
        ))
    }

    async fn rename_warehouse_cluster(&self, _: String, _: String, _: String) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented rename warehouse cluster with self-managed resources management",
        ))
    }

    async fn drop_warehouse_cluster(&self, _: String, _: String) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented drop warehouse cluster with self-managed resources management",
        ))
    }

    async fn assign_warehouse_nodes(
        &self,
        _: String,
        _: HashMap<String, Vec<SelectedNode>>,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented assign warehouse nodes with self-managed resources management",
        ))
    }

    async fn unassign_warehouse_nodes(
        &self,
        _: String,
        _: HashMap<String, Vec<SelectedNode>>,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented unassign warehouse nodes with self-managed resources management",
        ))
    }

    async fn list_online_nodes(&self) -> Result<Vec<NodeInfo>> {
        Err(ErrorCode::Unimplemented(
            "Unimplemented list online nodes with self-managed resources management",
        ))
    }
}
