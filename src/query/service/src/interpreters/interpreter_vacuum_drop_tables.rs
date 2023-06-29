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

use std::cmp::min;
use std::sync::Arc;

use common_exception::Result;
use common_expression::types::StringType;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FromData;
use common_license::license::Feature::Vacuum;
use common_license::license_manager::get_license_manager;
use common_sql::plans::VacuumDropTablePlan;
use common_storages_fuse::FuseTable;
use vacuum_handler::get_vacuum_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

const DRY_RUN_LIMIT: usize = 1000;

#[allow(dead_code)]
pub struct VacuumDropTablesInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumDropTablePlan,
}

impl VacuumDropTablesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumDropTablePlan) -> Result<Self> {
        Ok(VacuumDropTablesInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumDropTablesInterpreter {
    fn name(&self) -> &str {
        "VacuumDropTablesInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager.manager.check_enterprise_enabled(
            &self.ctx.get_settings(),
            self.ctx.get_tenant(),
            Vacuum,
        )?;

        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();

        Ok(PipelineBuildResult::create())
    }
}
