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
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_sql::plans::ShowCreateMaterializedViewPlan;

use crate::interpreters::Interpreter;
use crate::interpreters::ShowCreateTableInterpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

pub struct ShowCreateMaterializedViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowCreateMaterializedViewPlan,
}

impl ShowCreateMaterializedViewInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: ShowCreateMaterializedViewPlan,
    ) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowCreateMaterializedViewInterpreter {
    fn name(&self) -> &str {
        "ShowCreateMaterializedViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let table = catalog
            .get_table(&tenant, &self.plan.database, &self.plan.view_name)
            .await?;
        if !is_materialized_view_engine(table.engine()) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "`{}`.`{}` is not a MATERIALIZED VIEW",
                self.plan.database, self.plan.view_name
            )));
        }

        ShowCreateTableInterpreter::build_result(
            self.ctx.as_ref(),
            catalog.as_ref(),
            &tenant,
            &self.plan.database,
            table.as_ref(),
            false,
        )
        .await
    }
}
