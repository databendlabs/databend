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

use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_sql::plans::OptimizeCompactSegmentPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct OptimizeCompactSegmentInterpreter {
    ctx: Arc<QueryContext>,
    plan: OptimizeCompactSegmentPlan,
}

impl OptimizeCompactSegmentInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: OptimizeCompactSegmentPlan) -> Result<Self> {
        Ok(OptimizeCompactSegmentInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizeCompactSegmentInterpreter {
    fn name(&self) -> &str {
        "OptimizeCompactSegmentInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.table,
                &LockTableOption::LockWithRetry,
            )
            .await?;

        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let table = catalog
            .get_table(
                &self.ctx.get_tenant(),
                &self.plan.database,
                &self.plan.table,
            )
            .await?;
        // check mutability
        table.check_mutable()?;

        table
            .compact_segments(self.ctx.clone(), self.plan.num_segment_limit)
            .await?;

        drop(lock_guard);
        Ok(PipelineBuildResult::create())
    }
}
