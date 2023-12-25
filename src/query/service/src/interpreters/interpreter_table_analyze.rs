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

use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_sql::plans::AnalyzeTablePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct AnalyzeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: AnalyzeTablePlan,
}

impl AnalyzeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AnalyzeTablePlan) -> Result<Self> {
        Ok(AnalyzeTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AnalyzeTableInterpreter {
    fn name(&self) -> &str {
        "AnalyzeTableInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        // check mutability
        table.check_mutable()?;

        table.analyze(self.ctx.clone()).await?;
        return Ok(PipelineBuildResult::create());
    }
}
