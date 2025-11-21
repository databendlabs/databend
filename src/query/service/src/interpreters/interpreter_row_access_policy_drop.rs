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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::DropRowAccessPolicyPlan;
use databend_common_users::UserApiProvider;
use databend_enterprise_row_access_policy_feature::get_row_access_policy_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropRowAccessPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropRowAccessPolicyPlan,
}

impl DropRowAccessPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropRowAccessPolicyPlan) -> Result<Self> {
        Ok(DropRowAccessPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropRowAccessPolicyInterpreter {
    fn name(&self) -> &str {
        "DropRowAccessPolicyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::RowAccessPolicy)?;
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_row_access_policy_handler();
        if let Err(e) = handler
            .drop_row_access_policy(meta_api, self.plan.clone().into())
            .await
        {
            if e.code() == ErrorCode::UNKNOWN_ROW_ACCESS_POLICY && self.plan.if_exists {
                return Ok(PipelineBuildResult::create());
            } else {
                return Err(e);
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
