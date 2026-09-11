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
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::VacuumTablesPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextTableAccess;
use crate::table_functions::fuse_vacuum2::vacuum_tables;

pub struct VacuumTablesInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumTablesPlan,
}

impl VacuumTablesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumTablesPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumTablesInterpreter {
    fn name(&self) -> &str {
        "VacuumTablesInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Vacuum)?;

        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let table_ctx: Arc<dyn TableContext> = self.ctx.clone();
        vacuum_tables(&table_ctx, catalog.as_ref(), self.plan.database.as_deref()).await?;

        Ok(PipelineBuildResult::create())
    }
}
