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
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::UInt64Type;
use databend_common_license::license::Feature::VirtualColumn;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::RefreshVirtualColumnPlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_virtual_column::get_virtual_column_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct RefreshVirtualColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshVirtualColumnPlan,
}

impl RefreshVirtualColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshVirtualColumnPlan) -> Result<Self> {
        Ok(RefreshVirtualColumnInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshVirtualColumnInterpreter {
    fn name(&self) -> &str {
        "RefreshVirtualColumnInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), VirtualColumn)?;

        let table = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;
        // check mutability
        table.check_mutable()?;

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;

        let handler = get_virtual_column_handler();

        // Generating virtual column data is a time-consuming operation.
        // At this stage, neither the table is locked nor the BlockMeta is modified;
        // only write the virtual column data to parquet file,
        // thus not affecting concurrent insert operations
        let results = handler
            .prepare_refresh_virtual_column(
                self.ctx.clone(),
                fuse_table,
                self.plan.limit,
                self.plan.overwrite,
                self.plan.selection.clone(),
            )
            .await?;

        if results.is_empty() {
            let result_block = DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![0])]);
            return PipelineBuildResult::from_blocks(vec![result_block]);
        }

        // Lock the table and submit the BlockMeta with the virtual column,
        // this takes a very short time and has a very low probability of failure.
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

        let mut commit_res = PipelineBuildResult::create();
        let applied_blocks = handler
            .commit_refresh_virtual_column(
                self.ctx.clone(),
                fuse_table,
                &mut commit_res.main_pipeline,
                results,
            )
            .await?;

        // return the number of refreshed blocks.
        let result_block =
            DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![applied_blocks])]);

        if commit_res.main_pipeline.is_empty() {
            return PipelineBuildResult::from_blocks(vec![result_block]);
        }

        commit_res.main_pipeline.add_lock_guard(lock_guard);

        let mut result_res = PipelineBuildResult::from_blocks(vec![result_block])?;
        result_res
            .sources_pipelines
            .extend(commit_res.sources_pipelines);
        result_res.sources_pipelines.push(commit_res.main_pipeline);
        Ok(result_res)
    }
}
