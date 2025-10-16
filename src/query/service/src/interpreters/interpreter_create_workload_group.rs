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

use databend_common_ast::ast::QuotaValueStmt;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::workload_group::QuotaValue;
use databend_common_base::runtime::workload_group::WorkloadGroup;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::WorkloadApi;
use databend_common_management::WorkloadMgr;
use databend_common_sql::plans::CreateWorkloadGroupPlan;

use crate::interpreters::util::AuditElement;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateWorkloadGroupInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateWorkloadGroupPlan,
}

impl CreateWorkloadGroupInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateWorkloadGroupPlan) -> Result<Self> {
        Ok(CreateWorkloadGroupInterpreter { ctx, plan })
    }
}

pub fn to_quota_value(quota: &QuotaValueStmt) -> QuotaValue {
    match quota {
        QuotaValueStmt::Duration(v) => QuotaValue::Duration(*v),
        QuotaValueStmt::Percentage(v) => QuotaValue::Percentage(*v),
        QuotaValueStmt::Bytes(v) => QuotaValue::Bytes(*v),
        QuotaValueStmt::Number(v) => QuotaValue::Number(*v),
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateWorkloadGroupInterpreter {
    fn name(&self) -> &str {
        "CreateWorkloadGroupInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::WorkloadGroup)?;

        let mut workload_quotas = HashMap::with_capacity(self.plan.quotas.len());

        for (key, value) in &self.plan.quotas {
            workload_quotas.insert(key.clone(), to_quota_value(value));
        }

        let workload_group = WorkloadGroup {
            id: String::new(),
            name: self.plan.name.clone(),
            quotas: workload_quotas,
        };

        let workload_manager = GlobalInstance::get::<Arc<WorkloadMgr>>();
        match workload_manager.create(workload_group).await {
            Ok(_) => {
                let user_info = self.ctx.get_current_user()?;
                log::info!(
                    target: "databend::log::audit",
                    "{}",
                    serde_json::to_string(&AuditElement::create(&user_info, "create_workload", &self.plan))?
                );
                Ok(PipelineBuildResult::create())
            }
            Err(cause) => match self.plan.if_not_exists
                && cause.code() == ErrorCode::ALREADY_EXISTS_WORKLOAD
            {
                true => Ok(PipelineBuildResult::create()),
                false => Err(cause),
            },
        }
    }
}
