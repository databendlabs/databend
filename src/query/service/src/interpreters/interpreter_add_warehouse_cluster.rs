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

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::SelectedNode;
use databend_common_sql::plans::AddWarehouseClusterPlan;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct AddWarehouseClusterInterpreter {
    #[allow(dead_code)]
    ctx: Arc<QueryContext>,
    plan: AddWarehouseClusterPlan,
}

impl AddWarehouseClusterInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AddWarehouseClusterPlan) -> Result<Self> {
        Ok(AddWarehouseClusterInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AddWarehouseClusterInterpreter {
    fn name(&self) -> &str {
        "AddWarehouseClusterInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if let Some(cluster_size) = self.plan.options.get("cluster_size") {
            if !self.plan.nodes.is_empty() {
                return Err(ErrorCode::InvalidArgument(
                    "CLUSTER_SIZE option and node list exists in one query.",
                ));
            }

            let Ok(cluster_size) = cluster_size.parse::<usize>() else {
                return Err(ErrorCode::InvalidArgument(
                    "CLUSTER_SIZE must be a <number>",
                ));
            };

            GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
                .add_warehouse_cluster(
                    self.plan.warehouse.clone(),
                    self.plan.cluster.clone(),
                    vec![SelectedNode::Random(None); cluster_size],
                )
                .await?;

            return Ok(PipelineBuildResult::create());
        }

        if self.plan.nodes.is_empty() {
            return Err(ErrorCode::InvalidArgument("Cluster nodes list is empty"));
        }

        let mut selected_nodes = Vec::with_capacity(self.plan.nodes.len());
        for (group, nodes) in &self.plan.nodes {
            for _ in 0..*nodes {
                selected_nodes.push(SelectedNode::Random(group.clone()));
            }
        }

        GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .add_warehouse_cluster(
                self.plan.warehouse.clone(),
                self.plan.cluster.clone(),
                selected_nodes,
            )
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
