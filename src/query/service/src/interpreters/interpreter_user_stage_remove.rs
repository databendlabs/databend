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
use common_sql::plans::RemoveStagePlan;
use common_storages_stage::list_file;
use common_storages_stage::StageTable;
use regex::Regex;

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

        let mut files = list_file(table_ctx, &plan.path, &plan.stage).await?;

        let files = if plan.pattern.is_empty() {
            files
        } else {
            let regex = Regex::new(&plan.pattern).map_err(|e| {
                ErrorCode::SyntaxException(format!(
                    "Pattern format invalid, got:{}, error:{:?}",
                    &plan.pattern, e
                ))
            })?;

            files.retain(|v| regex.is_match(&v.path));
            files
        };

        for name in files.iter().map(|f| f.path.as_str()) {
            op.object(name).delete().await?;
        }

        Ok(PipelineBuildResult::create())
    }
}
