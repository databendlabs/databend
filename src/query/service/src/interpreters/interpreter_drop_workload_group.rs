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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::WorkloadApi;
use databend_common_management::WorkloadMgr;
use databend_common_sql::plans::DropWorkloadGroupPlan;

use crate::interpreters::util::AuditElement;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct DropWorkloadGroupInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropWorkloadGroupPlan,
}

impl DropWorkloadGroupInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropWorkloadGroupPlan) -> Result<Self> {
        Ok(DropWorkloadGroupInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropWorkloadGroupInterpreter {
    fn name(&self) -> &str {
        "DropWorkloadGroupInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::WorkloadGroup)?;

        let workload_mgr = GlobalInstance::get::<Arc<WorkloadMgr>>();

        match WorkloadMgr::drop(&workload_mgr, self.plan.name.clone()).await {
            Ok(_) => {
                let user_info = self.ctx.get_current_user()?;
                log::info!(
                    target: "databend::log::audit",
                    "{}",
                    serde_json::to_string(&AuditElement::create(&user_info, "drop_workload", &self.plan))?
                );

                Ok(PipelineBuildResult::create())
            }
            Err(cause) => {
                match self.plan.if_exists && cause.code() == ErrorCode::UNKNOWN_WORKLOAD {
                    true => Ok(PipelineBuildResult::create()),
                    false => Err(cause),
                }
            }
        }
    }
}
