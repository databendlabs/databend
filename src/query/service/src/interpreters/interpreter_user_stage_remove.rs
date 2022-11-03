// Copyright 2022 Datafuse Labs.
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

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planner::plans::RemoveStagePlan;
use common_storages_preludes::stage::StageTable;
use regex::Regex;

use crate::interpreters::common::list_files;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct RemoveUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: RemoveStagePlan,
}

impl RemoveUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RemoveStagePlan) -> Result<Self> {
        Ok(RemoveUserStageInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RemoveUserStageInterpreter {
    fn name(&self) -> &str {
        "RemoveUserStageInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = self.plan.clone();

        let table_ctx: Arc<dyn TableContext> = self.ctx.clone();
        let op = StageTable::get_op(&table_ctx, &self.plan.stage)?;

        let files = list_files(&self.ctx, &plan.stage, &plan.path).await?;

        let files = if plan.pattern.is_empty() {
            files
        } else {
            let regex = Regex::new(&plan.pattern).map_err(|e| {
                ErrorCode::SyntaxException(format!(
                    "Pattern format invalid, got:{}, error:{:?}",
                    &plan.pattern, e
                ))
            })?;

            files
                .into_iter()
                .filter(|file| regex.is_match(&file.path))
                .collect()
        };

        for name in files.iter().map(|f| f.path.as_str()) {
            op.object(name).delete().await?;
        }

        Ok(PipelineBuildResult::create())
    }
}
