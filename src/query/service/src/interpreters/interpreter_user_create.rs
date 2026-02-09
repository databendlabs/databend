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

use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::UserApi;
use databend_common_management::meta_service_error;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserQuota;
use databend_common_sql::plans::CreateUserPlan;
use databend_common_users::UserApiProvider;
use databend_meta_types::MatchSeq;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct CreateUserInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateUserPlan,
}

impl CreateUserInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateUserPlan) -> Result<Self> {
        Ok(CreateUserInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateUserInterpreter {
    fn name(&self) -> &str {
        "CreateUserInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "create_user_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();

        let user_mgr = UserApiProvider::instance();
        let user_counts = user_mgr
            .user_api(&tenant)
            .get_raw_users()
            .await
            .map_err(meta_service_error)?
            .len();

        let quota_api = UserApiProvider::instance().tenant_quota_api(&tenant);
        let quota = quota_api.get_quota(MatchSeq::GE(0)).await?.data;
        if quota.max_users != 0 && user_counts >= quota.max_users as usize {
            return Err(ErrorCode::TenantQuotaExceeded(format!(
                "Max users quota exceeded: {}",
                quota.max_users
            )));
        };

        let now = Utc::now();
        let user_info = UserInfo {
            auth_info: plan.auth_info.clone(),
            name: plan.user.username,
            hostname: plan.user.hostname,
            grants: UserGrantSet::empty(),
            quota: UserQuota::no_limit(),
            option: plan.user_option,
            history_auth_infos: vec![plan.auth_info.clone()],
            password_fails: Vec::new(),
            password_update_on: plan.password_update_on,
            lockout_time: None,

            created_on: now,
            update_on: now,
        };
        user_mgr
            .create_user(&tenant, user_info, &plan.create_option)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
