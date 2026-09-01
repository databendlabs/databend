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
use databend_common_sql::plans::VacuumAllPlan;
use databend_common_sql::plans::VacuumDropTablePlan;
use databend_common_sql::plans::VacuumTablesPlan;
use databend_common_sql::plans::VacuumTemporaryFilesPlan;

use crate::interpreters::Interpreter;
use crate::interpreters::VacuumDropTablesInterpreter;
use crate::interpreters::VacuumTablesInterpreter;
use crate::interpreters::VacuumTemporaryFilesInterpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextLicense;

pub struct VacuumAllInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumAllPlan,
}

impl VacuumAllInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumAllPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumAllInterpreter {
    fn name(&self) -> &str {
        "VacuumAllInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Vacuum)?;

        VacuumTablesInterpreter::try_create(self.ctx.clone(), VacuumTablesPlan {
            catalog: self.plan.catalog.clone(),
            database: None,
        })?
        .execute2()
        .await?;

        VacuumDropTablesInterpreter::try_create(self.ctx.clone(), VacuumDropTablePlan {
            catalog: self.plan.catalog.clone(),
            database: String::new(),
        })?
        .execute2()
        .await?;

        VacuumTemporaryFilesInterpreter::try_create(self.ctx.clone(), VacuumTemporaryFilesPlan {
            limit: None,
            retain: None,
        })?
        .execute2()
        .await?;

        Ok(PipelineBuildResult::create())
    }
}
