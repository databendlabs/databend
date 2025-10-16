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
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::SelectedNode;
use databend_common_sql::plans::AssignWarehouseNodesPlan;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::interpreters::util::AuditElement;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct AssignWarehouseNodesInterpreter {
    ctx: Arc<QueryContext>,
    plan: AssignWarehouseNodesPlan,
}

impl AssignWarehouseNodesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AssignWarehouseNodesPlan) -> Result<Self> {
        Ok(AssignWarehouseNodesInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AssignWarehouseNodesInterpreter {
    fn name(&self) -> &str {
        "AddWarehouseClusterNodeInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::SystemManagement)?;

        let mut cluster_selected_nodes = HashMap::with_capacity(self.plan.assign_clusters.len());
        for (cluster, nodes_map) in &self.plan.assign_clusters {
            let mut selected_nodes = Vec::with_capacity(nodes_map.len());
            for (group, nodes) in nodes_map {
                for _ in 0..*nodes {
                    selected_nodes.push(SelectedNode::Random(group.clone()));
                }
            }

            cluster_selected_nodes.insert(cluster.clone(), selected_nodes);
        }

        GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .assign_warehouse_nodes(self.plan.warehouse.clone(), cluster_selected_nodes)
            .await?;

        let user_info = self.ctx.get_current_user()?;
        log::info!(
            target: "databend::log::audit",
            "{}",
            serde_json::to_string(&AuditElement::create(&user_info, "alter_warehouse_assign_nodes", &self.plan))?
        );

        Ok(PipelineBuildResult::create())
    }
}
