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
use databend_common_exception::Result;
use databend_common_sql::plans::RenameWarehouseClusterPlan;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;

pub struct RenameWarehouseClusterInterpreter {
    plan: RenameWarehouseClusterPlan,
}

impl RenameWarehouseClusterInterpreter {
    pub fn try_create(plan: RenameWarehouseClusterPlan) -> Result<Self> {
        Ok(RenameWarehouseClusterInterpreter { plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RenameWarehouseClusterInterpreter {
    fn name(&self) -> &str {
        "RenameWarehouseClusterInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .rename_warehouse_cluster(
                self.plan.warehouse.clone(),
                self.plan.cluster.clone(),
                self.plan.new_cluster.clone(),
            )
            .await?;
        Ok(PipelineBuildResult::create())
    }
}
