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
use databend_common_sql::plans::CreateWarehousePlan;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateWarehouseInterpreter {
    #[allow(dead_code)]
    ctx: Arc<QueryContext>,
    plan: CreateWarehousePlan,
}

impl CreateWarehouseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateWarehousePlan) -> Result<Self> {
        Ok(CreateWarehouseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateWarehouseInterpreter {
    fn name(&self) -> &str {
        "CreateWarehouseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if let Some(warehouse_size) = self.plan.options.get("warehouse_size") {
            if !self.plan.nodes.is_empty() {
                return Err(ErrorCode::InvalidArgument(
                    "WAREHOUSE_SIZE option and node list exists in one query.",
                ));
            }

            let Ok(warehouse_size) = warehouse_size.parse::<usize>() else {
                return Err(ErrorCode::InvalidArgument(
                    "WAREHOUSE_SIZE must be a <number>",
                ));
            };

            GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
                .create_warehouse(self.plan.warehouse.clone(), vec![
                    SelectedNode::Random(None);
                    warehouse_size
                ])
                .await?;

            return Ok(PipelineBuildResult::create());
        }

        if self.plan.nodes.is_empty() {
            return Err(ErrorCode::InvalidArgument("Warehouse nodes list is empty"));
        }

        let mut selected_nodes = Vec::with_capacity(self.plan.nodes.len());
        for (group, nodes) in &self.plan.nodes {
            for _ in 0..*nodes {
                selected_nodes.push(SelectedNode::Random(group.clone()));
            }
        }

        GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .create_warehouse(self.plan.warehouse.clone(), selected_nodes)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
