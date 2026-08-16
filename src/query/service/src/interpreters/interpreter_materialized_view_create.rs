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
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::CreateMaterializedViewMeta;
use databend_common_sql::plans::CreateMaterializedViewPlan;

use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextTableAccess;

pub struct CreateMaterializedViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateMaterializedViewPlan,
}

impl CreateMaterializedViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateMaterializedViewPlan) -> Result<Self> {
        Ok(CreateMaterializedViewInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateMaterializedViewInterpreter {
    fn name(&self) -> &str {
        "CreateMaterializedViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::MaterializedView)?;

        let table_interpreter =
            CreateTableInterpreter::try_create(self.ctx.clone(), self.plan.table_plan.clone())?;

        let materialized_view = CreateMaterializedViewMeta {
            definition: self.plan.mv_definition.clone(),
            expected_source_generation: self.plan.expected_source_generation,
        };
        let catalog = self.ctx.get_catalog(&self.plan.table_plan.catalog).await?;
        let mut req = table_interpreter.build_request(None)?;
        req.source_table_option = self.plan.source_table_option.clone();
        req.materialized_view = Some(materialized_view);
        // MV tables deliberately have no independent ownership. Reuse table
        // validation/request construction, then publish directly through the catalog.
        catalog.create_table(req).await?;

        Ok(PipelineBuildResult::create())
    }
}
