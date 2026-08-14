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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::MATERIALIZED_VIEW_ENGINE;
use databend_common_sql::plans::RefreshMaterializedViewPlan;
use databend_enterprise_materialized_view::get_materialized_view_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextTableAccess;

pub struct RefreshMaterializedViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshMaterializedViewPlan,
}

impl RefreshMaterializedViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshMaterializedViewPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshMaterializedViewInterpreter {
    fn name(&self) -> &str {
        "RefreshMaterializedViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::MaterializedView)?;

        let table = self
            .ctx
            .get_table(
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.view_name,
            )
            .await?;
        if table.engine() != MATERIALIZED_VIEW_ENGINE {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} is not a materialized view",
                self.plan.database, self.plan.view_name
            )));
        }

        get_materialized_view_handler()
            .do_refresh_materialized_view(
                self.ctx.clone(),
                table,
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.view_name,
            )
            .await?;
        Ok(PipelineBuildResult::create())
    }
}
