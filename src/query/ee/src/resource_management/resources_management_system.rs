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
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::SelectedNode;
use databend_common_management::WarehouseApi;
use databend_common_management::WarehouseInfo;
use databend_common_management::WarehouseMgr;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_meta_types::NodeInfo;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::resource_management::resources_management_self_managed::SelfManagedResourcesManagement;

pub struct SystemResourcesManagement {
    warehouse_manager: Arc<dyn WarehouseApi>,
}

#[async_trait::async_trait]
impl ResourcesManagement for SystemResourcesManagement {
    async fn create_warehouse(&self, name: String, nodes: Vec<SelectedNode>) -> Result<()> {
        self.warehouse_manager.create_warehouse(name, nodes).await
    }

    async fn drop_warehouse(&self, name: String) -> Result<()> {
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
}

impl SystemResourcesManagement {
    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        if !cfg.query.cluster_id.is_empty() && cfg.query.resources_management.is_some() {
            return Err(ErrorCode::InvalidConfig("Configuration error, cannot set both cluster_id and resources_management at the same time"));
        }

        let service: Arc<dyn ResourcesManagement> = match &cfg.query.resources_management {
            None => Arc::new(SelfManagedResourcesManagement {}),
            Some(_resources_management) => {
                let meta_api_provider = MetaStoreProvider::new(cfg.meta.to_meta_grpc_client_conf());
                match meta_api_provider.create_meta_store().await {
                    Err(cause) => {
                        return Err(ErrorCode::from(cause)
                            .add_message_back("(while create resources management)."))
                    }
                    Ok(metastore) => {
                        let tenant_id = &cfg.query.tenant_id;
                        let lift_time = Duration::from_secs(60);
                        let warehouse_manager =
                            WarehouseMgr::create(metastore, tenant_id.tenant_name(), lift_time)?;

                        Arc::new(SystemResourcesManagement {
                            warehouse_manager: Arc::new(warehouse_manager),
                        })
                    }
                }
            }
        };

        GlobalInstance::set(service);
        Ok(())
    }
}
