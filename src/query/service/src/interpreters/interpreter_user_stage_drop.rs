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
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::TaggableObject;
use databend_common_sql::plans::DropStagePlan;
use databend_common_storage::StageFilesInfo;
use databend_common_storage::init_stage_operator;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use log::debug;
use log::info;
use opendal::Operator;

use crate::interpreters::Interpreter;
use crate::interpreters::RemoveUserStageInterpreter;
use crate::interpreters::cleanup_object_tags;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextTableAccess;

#[derive(Debug)]
pub struct DropUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropStagePlan,
}

impl DropUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropStagePlan) -> Result<Self> {
        Ok(DropUserStageInterpreter { ctx, plan })
    }

    pub async fn remove_all(table_ctx: Arc<dyn TableContext>, op: Operator) -> Result<()> {
        RemoveUserStageInterpreter::remove_all(
            table_ctx,
            op,
            StageFilesInfo {
                path: "/".to_string(),
                ..Default::default()
            },
            true,
        )
        .await
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
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            debug!("ctx.id" = self.ctx.get_id().as_str(); "drop_user_stage_execute");

            let plan = self.plan.clone();
            let tenant = self.ctx.get_tenant();
            let user_mgr = UserApiProvider::instance();

            // Get stage info first for cleanup operations
            let stage = user_mgr.get_stage(&tenant, &plan.name).await;

            // 1. Remove stage files for internal stages
            if let Ok(stage) = &stage {
                if !matches!(&stage.stage_type, StageType::External) {
                    let op = init_stage_operator(stage)?;
                    Self::remove_all(self.ctx.clone(), op).await?;
                    info!(
                        "drop stage {:?} with all objects removed in stage",
                        stage.stage_name
                    );
                }
            }

            // 2. Delete stage metadata and ownership in the same Meta transaction.
            user_mgr
                .drop_stage(&tenant, &plan.name, plan.if_exists)
                .await?;

            RoleCacheManager::instance().invalidate_cache(&tenant);

            // 3. Tag cleanup is separate from the metadata/ownership transaction.
            let taggable_object = TaggableObject::Stage {
                name: plan.name.clone(),
            };
            cleanup_object_tags(&tenant, taggable_object).await?;

            Ok(PipelineBuildResult::create())
        })
    }
}
