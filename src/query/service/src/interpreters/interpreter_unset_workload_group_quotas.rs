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
use databend_common_management::WorkloadApi;
use databend_common_management::WorkloadMgr;
use databend_common_sql::plans::UnsetWorkloadGroupQuotasPlan;

use crate::interpreters::util::AuditElement;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct UnsetWorkloadGroupQuotasInterpreter {
    ctx: Arc<QueryContext>,
    plan: UnsetWorkloadGroupQuotasPlan,
}

impl UnsetWorkloadGroupQuotasInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: UnsetWorkloadGroupQuotasPlan) -> Result<Self> {
        Ok(UnsetWorkloadGroupQuotasInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for UnsetWorkloadGroupQuotasInterpreter {
    fn name(&self) -> &str {
        "UnsetWorkloadGroupQuotasInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::WorkloadGroup)?;

        let workload_manager = GlobalInstance::get::<Arc<WorkloadMgr>>();
        workload_manager
            .unset_quotas(self.plan.name.clone(), self.plan.quotas.clone())
            .await?;

        let user_info = self.ctx.get_current_user()?;
        log::info!(
            target: "databend::log::audit",
            "{}",
            serde_json::to_string(&AuditElement::create(&user_info, "unset_workload_quotas", &self.plan))?
        );
        Ok(PipelineBuildResult::create())
    }
}
