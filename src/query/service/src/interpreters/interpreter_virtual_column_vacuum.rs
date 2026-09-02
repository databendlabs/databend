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
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::UInt64Type;
use databend_common_sql::plans::VacuumVirtualColumnPlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::cleanup_vacuum_virtual_column_files;
use databend_common_storages_fuse::operations::do_vacuum_virtual_column;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

pub struct VacuumVirtualColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumVirtualColumnPlan,
}

impl VacuumVirtualColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumVirtualColumnPlan) -> Result<Self> {
        Ok(VacuumVirtualColumnInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumVirtualColumnInterpreter {
    fn name(&self) -> &str {
        "VacuumVirtualColumnInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let table = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;

        table.check_mutable()?;

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;

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

        let mut build_res = PipelineBuildResult::create();
        let vacuum_result =
            do_vacuum_virtual_column(self.ctx.clone(), fuse_table, &mut build_res.main_pipeline)
                .await?;

        execute_complete_pipeline(self.ctx.clone(), build_res).await?;

        let cleanup_removed_files = if vacuum_result.need_cleanup {
            if vacuum_result.need_commit {
                let latest_table = fuse_table.refresh(self.ctx.as_ref()).await?;
                let latest_fuse_table = FuseTable::try_from_table(latest_table.as_ref())?;
                cleanup_vacuum_virtual_column_files(self.ctx.clone(), latest_fuse_table).await?
            } else {
                cleanup_vacuum_virtual_column_files(self.ctx.clone(), fuse_table).await?
            }
        } else {
            0
        };

        let removed_files = vacuum_result.removed_files + cleanup_removed_files;
        drop(lock_guard);

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![removed_files]),
        ])])
    }
}

async fn execute_complete_pipeline(
    ctx: Arc<QueryContext>,
    mut build_res: PipelineBuildResult,
) -> Result<()> {
    if build_res.main_pipeline.is_empty() {
        return Ok(());
    }

    let settings = ctx.get_settings();
    build_res.set_max_threads(settings.get_max_threads()? as usize);

    if !build_res.main_pipeline.is_complete_pipeline()? {
        return Err(ErrorCode::Internal(
            "Vacuum virtual column commit pipeline must be complete",
        ));
    }

    let mut pipelines = build_res.sources_pipelines;
    pipelines.push(build_res.main_pipeline);
    let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
    let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
    ctx.set_executor(complete_executor.get_inner())?;
    complete_executor.execute().await
}
