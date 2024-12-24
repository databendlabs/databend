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
use databend_common_exception::Result;
use databend_common_management::SelectedNode;
use databend_common_sql::plans::AssignWarehouseNodesPlan;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;

pub struct AssignWarehouseNodesInterpreter {
    plan: AssignWarehouseNodesPlan,
}

impl AssignWarehouseNodesInterpreter {
    pub fn try_create(plan: AssignWarehouseNodesPlan) -> Result<Self> {
        Ok(AssignWarehouseNodesInterpreter { plan })
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
    async fn execute2(&self) -> Result<PipelineBuildResult> {
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
        Ok(PipelineBuildResult::create())
    }
}
