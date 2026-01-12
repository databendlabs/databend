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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::CreateOption;
use databend_common_sql::plans::AlterStageActionPlan;
use databend_common_sql::plans::AlterStagePlan;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct AlterUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterStagePlan,
}

impl AlterUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterStagePlan) -> Result<Self> {
        Ok(AlterUserStageInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterUserStageInterpreter {
    fn name(&self) -> &str {
        "AlterUserStageInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();
        let mut stage = match user_mgr.get_stage(&tenant, &self.plan.stage_name).await {
            Ok(stage) => stage,
            Err(e) => {
                if self.plan.if_exists && e.code() == ErrorCode::UNKNOWN_STAGE {
                    return Ok(PipelineBuildResult::create());
                }
                return Err(e);
            }
        };

        if matches!(stage.stage_type, StageType::User) {
            return Err(ErrorCode::StagePermissionDenied(
                "user stage is not allowed to be altered",
            ));
        }

        match &self.plan.action {
            AlterStageActionPlan::Set(options) => {
                if let Some(storage) = options.storage_params.as_ref() {
                    if !matches!(stage.stage_type, StageType::External) {
                        return Err(ErrorCode::BadArguments(format!(
                            "Stage {} is not external, LOCATION/URL can only be set for external stages",
                            stage.stage_name
                        )));
                    }
                    stage.stage_params.storage = storage.clone();
                }
                if let Some(file_format) = options.file_format.as_ref() {
                    stage.file_format_params = file_format.clone();
                }
                if let Some(comment) = &options.comment {
                    stage.comment = comment.clone();
                }
            }
            AlterStageActionPlan::Unset(options) => {
                if options.file_format {
                    stage.file_format_params = FileFormatParams::default();
                }
                if options.comment {
                    stage.comment.clear();
                }
            }
        }

        user_mgr
            .add_stage(&tenant, stage, &CreateOption::CreateOrReplace)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
