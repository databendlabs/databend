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

use databend_common_exception::Result;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::StageType;
use databend_common_sql::plans::DropStagePlan;
use databend_common_storages_stage::StageTable;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use log::debug;
use log::info;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct DropUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropStagePlan,
}

impl DropUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropStagePlan) -> Result<Self> {
        Ok(DropUserStageInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropUserStageInterpreter {
    fn name(&self) -> &str {
        "DropUserStageInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "drop_user_stage_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        let stage = user_mgr.get_stage(&tenant, &plan.name).await;
        user_mgr
            .drop_stage(&tenant, &plan.name, plan.if_exists)
            .await?;

        if let Ok(stage) = stage {
            // we should do `drop ownership` after actually drop stage,
            // drop the ownership
            let role_api = UserApiProvider::instance().role_api(&tenant);
            let owner_object = OwnershipObject::Stage {
                name: self.plan.name.clone(),
            };

            role_api
                .revoke_ownership(
                    &owner_object,
                    self.ctx
                        .get_settings()
                        .get_enable_upgrade_meta_data_to_pb()?,
                )
                .await?;
            RoleCacheManager::instance().invalidate_cache(&tenant);

            if !matches!(&stage.stage_type, StageType::External) {
                let op = StageTable::get_op(&stage)?;
                op.remove_all("/").await?;
                info!(
                    "drop stage {:?} with all objects removed in stage",
                    stage.stage_name
                );
            }
        };

        Ok(PipelineBuildResult::create())
    }
}
