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
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RefreshMaterializedViewPlan;

use crate::interpreters::InsertInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::common::plan_materialized_view_query;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

pub struct RefreshMaterializedViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshMaterializedViewPlan,
}

impl RefreshMaterializedViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshMaterializedViewPlan) -> Result<Self> {
        Ok(RefreshMaterializedViewInterpreter { ctx, plan })
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
        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let view_name = self.plan.view_name.clone();

        let plan =
            plan_materialized_view_query(&self.ctx, &catalog_name, &db_name, &view_name).await?;

        let select_plan = match plan {
            Plan::Query { .. } => plan,
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "materialized view `{}`.`{}` has invalid refresh query: not a SELECT",
                    db_name, view_name
                )));
            }
        };

        let table = self
            .ctx
            .get_table(&catalog_name, &db_name, &view_name)
            .await?;

        let insert_plan = databend_common_sql::plans::Insert {
            catalog: catalog_name,
            database: db_name,
            table: view_name,
            branch: None,
            schema: table.schema(),
            overwrite: true,
            source: InsertInputSource::SelectPlan(Box::new(select_plan)),
            table_info: Some(table.get_table_info().clone()),
        };

        let interpreter = InsertInterpreter::try_create(self.ctx.clone(), insert_plan)?;
        interpreter.execute2().await
    }
}
