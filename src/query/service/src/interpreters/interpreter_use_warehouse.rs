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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::UseWarehousePlan;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct UseWarehouseInterpreter {
    ctx: Arc<QueryContext>,
    plan: UseWarehousePlan,
}

impl UseWarehouseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: UseWarehousePlan) -> Result<Self> {
        Ok(UseWarehouseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for UseWarehouseInterpreter {
    fn name(&self) -> &str {
        "UseWarehouseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::SystemManagement)?;

        // check warehouse exists
        let _nodes = GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .inspect_warehouse(self.plan.warehouse.clone())
            .await?;

        unsafe {
            self.ctx
                .get_session_settings()
                .set_warehouse(self.plan.warehouse.clone())?;
        }

        Ok(PipelineBuildResult::create())
    }
}
