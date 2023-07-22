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
use std::time::SystemTime;

use common_catalog::plan::PushDownInfo;
use common_exception::ErrorCode;
use common_exception::Result;
use log::info;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterClusteringHistory;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::Pipeline;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::ReclusterTablePlan;

const MAX_RECLUSTER_TIMES: usize = 1000;

pub struct ReclusterTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: ReclusterTablePlan,
}

impl ReclusterTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ReclusterTablePlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ReclusterTableInterpreter {
    fn name(&self) -> &str {
        "ReclusterTableInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let ctx = self.ctx.clone();
        let settings = ctx.get_settings();
        let tenant = ctx.get_tenant();
        let start = SystemTime::now();

        // Build extras via push down scalar
        let extras = if let Some(scalar) = &plan.push_downs {
            let filter = scalar
                .as_expr()?
                .project_column_ref(|col| col.column_name.clone())
                .as_remote_expr();

            Some(PushDownInfo {
                filter: Some(filter),
                ..PushDownInfo::default()
            })
        } else {
            None
        };

        // Status.
        {
            let status = "recluster: begin to run recluster";
            ctx.set_status_info(status);
            info!("{}", status);
        }
        let mut times = 0;
        let mut block_count = 0;
        let max_threads = settings.get_max_threads()?;
        loop {
            let table = self
                .ctx
                .get_catalog(&plan.catalog)?
                .get_table(tenant.as_str(), &plan.database, &plan.table)
                .await?;

            // check if the table is locked.
            let catalog = self.ctx.get_catalog(&self.plan.catalog)?;
            let reply = catalog
                .list_table_lock_revs(table.get_table_info().ident.table_id)
                .await?;
            if !reply.is_empty() {
                return Err(ErrorCode::TableAlreadyLocked(format!(
                    "table '{}' is locked, please retry recluster later",
                    self.plan.table
                )));
            }

            let mut pipeline = Pipeline::create();
            let reclustered_block_count = table
                .recluster(ctx.clone(), extras.clone(), plan.limit, &mut pipeline)
                .await?;
            if pipeline.is_empty() {
                break;
            };

            block_count += reclustered_block_count;
            let max_threads = std::cmp::min(max_threads, reclustered_block_count) as usize;
            pipeline.set_max_threads(max_threads);

            let query_id = ctx.get_id();
            let executor_settings = ExecutorSettings::try_create(&settings, query_id)?;
            let executor = PipelineCompleteExecutor::try_create(pipeline, executor_settings)?;

            ctx.set_executor(executor.get_inner())?;
            executor.execute()?;

            times += 1;
            // Status.
            {
                let status = format!(
                    "recluster: run recluster tasks:{} times, cost:{} sec",
                    times,
                    start.elapsed().map_or(0, |d| d.as_secs())
                );
                ctx.set_status_info(&status);
                info!("{}", &status);
            }

            if !plan.is_final || times >= MAX_RECLUSTER_TIMES {
                break;
            }
        }

        if block_count != 0 {
            InterpreterClusteringHistory::write_log(
                &ctx,
                start,
                &plan.database,
                &plan.table,
                block_count,
            )?;
        }

        Ok(PipelineBuildResult::create())
    }
}
