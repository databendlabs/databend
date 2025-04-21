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
use databend_common_license::license::Feature::VirtualColumn;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::RefreshVirtualColumnPlan;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_virtual_column::get_virtual_column_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

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
        true
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
        let segment_locs = self.plan.segment_locs.clone();

        let mut build_res = PipelineBuildResult::create();
        let handler = get_virtual_column_handler();
        let _ = handler
            .do_refresh_virtual_column(
                self.ctx.clone(),
                fuse_table,
                segment_locs,
                &mut build_res.main_pipeline,
            )
            .await?;

        Ok(build_res)
    }
}
