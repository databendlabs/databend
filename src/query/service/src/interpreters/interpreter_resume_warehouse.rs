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
use databend_common_sql::plans::ResumeWarehousePlan;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::interpreters::Interpreter;
use crate::interpreters::util::AuditElement;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct ResumeWarehouseInterpreter {
    ctx: Arc<QueryContext>,
    plan: ResumeWarehousePlan,
}

impl ResumeWarehouseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ResumeWarehousePlan) -> Result<Self> {
        Ok(ResumeWarehouseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ResumeWarehouseInterpreter {
    fn name(&self) -> &str {
        "ResumeWarehouseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::SystemManagement)?;

        GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .resume_warehouse(self.plan.warehouse.clone())
            .await?;

        let user_info = self.ctx.get_current_user()?;
        log::info!(
            target: "databend::log::audit",
            "{}",
            serde_json::to_string(&AuditElement::create(&user_info, "resume_warehouse", &self.plan))?
        );

        Ok(PipelineBuildResult::create())
    }
}
