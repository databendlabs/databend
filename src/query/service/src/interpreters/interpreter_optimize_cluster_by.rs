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
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans::OptimizeClusterBy;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct OptimizeClusterByInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
}

impl OptimizeClusterByInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, s_expr: SExpr) -> Result<Self> {
        Ok(OptimizeClusterByInterpreter { ctx, s_expr })
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizeClusterByInterpreter {
    fn name(&self) -> &str {
        "OptimizeClusterByInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let OptimizeClusterBy {
            catalog_name,
            database_name,
            table_name,
            metadata,
            bind_context,
        } = self.s_expr.plan().clone().try_into()?;

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;

        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
        let physical_plan = builder
            .build(&self.s_expr, bind_context.column_set())
            .await?;
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;

        table.append_data(self.ctx.clone(), &mut build_res.main_pipeline)?;
        table.commit_insertion(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            None,
            vec![],
            true,
            None,
            unsafe { self.ctx.get_settings().get_deduplicate_label()? },
        )?;
        return Ok(build_res);
    }
}
