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
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::CreateOption;
use databend_common_sql::plans::CreateStagePlan;
use databend_common_storage::init_stage_operator;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_meta_client::types::MatchSeq;
use log::debug;
use log::info;

use crate::interpreters::DropUserStageInterpreter;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextAuthorization;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextTableAccess;

#[derive(Debug)]
pub struct CreateUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateStagePlan,
}

impl CreateUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateStagePlan) -> Result<Self> {
        Ok(CreateUserStageInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateUserStageInterpreter {
    fn name(&self) -> &str {
        "CreateUserStageInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            debug!("ctx.id" = self.ctx.get_id().as_str(); "create_user_stage_execute");

            let plan = self.plan.clone();
            let user_mgr = UserApiProvider::instance();
            let user_stage = plan.stage_info;

            // Check user stage.
            if user_stage.stage_type == StageType::User {
                return Err(ErrorCode::StagePermissionDenied(
                    "user stage is not allowed to be created",
                ));
            }

            let tenant = &plan.tenant;

            let quota_api = user_mgr.tenant_quota_api(tenant);
            let quota = quota_api.get_quota(MatchSeq::GE(0)).await?.data;
            let stages = user_mgr.get_stages(tenant).await?;
            if quota.max_stages != 0 && stages.len() >= quota.max_stages as usize {
                return Err(ErrorCode::TenantQuotaExceeded(format!(
                    "Max stages quota exceeded {}",
                    quota.max_stages
                )));
            };

            let tenant = &plan.tenant;

            let old_stage = match plan.create_option {
                CreateOption::CreateOrReplace => user_mgr
                    .get_stage(tenant, &user_stage.stage_name)
                    .await
                    .ok(),
                _ => None,
            };

            // Remove the old internal stage's files before replacing its metadata. Internal stage
            // prefixes are name-based, so removing them afterwards could leave the old files
            // visible to the replacement's owner if removal failed. If the metadata transaction
            // fails instead, the old stage remains without its files; retrying the statement
            // completes the replace.
            if let Some(stage) = old_stage {
                if stage.stage_type != StageType::External {
                    let op = init_stage_operator(&stage)?;
                    DropUserStageInterpreter::remove_all(self.ctx.clone(), op).await?;
                    info!(
                        "create or replace stage {:?} with all objects removed in stage",
                        user_stage.stage_name
                    );
                }
            }

            let mut user_stage = user_stage;
            user_stage.creator = Some(self.ctx.get_current_user()?.identity());
            user_stage.created_on = Utc::now();
            // The stage and its ownership are written in one Meta transaction.
            // IF NOT EXISTS on an existing stage writes neither.
            let owner = self.ctx.get_current_role().map(|role| role.name);
            user_mgr
                .add_stage(
                    tenant,
                    user_stage.clone(),
                    &plan.create_option,
                    owner.as_deref(),
                )
                .await?;
            RoleCacheManager::instance().invalidate_cache(tenant);

            // create dir if new stage if not external stage. Creating an existing
            // directory is a no-op, so this is safe for IF NOT EXISTS as well.
            if user_stage.stage_type != StageType::External {
                let op = self.ctx.get_application_level_data_operator()?.operator();
                op.create_dir(&user_stage.stage_prefix()).await?
            }

            Ok(PipelineBuildResult::create())
        })
    }
}
