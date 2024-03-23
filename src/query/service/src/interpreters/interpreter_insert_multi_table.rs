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
use databend_common_expression::RemoteExpr;
use databend_common_sql::executor::physical_plans::PhysicalInsertMultiTable;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::InsertMultiTable;
use databend_common_sql::plans::Plan;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sql::executor::cast_expr_to_non_null_boolean;
use crate::sql::executor::physical_plans::Duplicate;
use crate::sql::executor::physical_plans::Shuffle;
pub struct InsertMultiTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertMultiTable,
}

impl InsertMultiTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InsertMultiTable) -> Result<InterpreterPtr> {
        Ok(Arc::new(Self { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertMultiTableInterpreter {
    fn name(&self) -> &str {
        "InsertMultiTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let physical_plan = self.build_physical_plan().await?;
        let build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        Ok(build_res)
    }
}

impl InsertMultiTableInterpreter {
    async fn build_physical_plan(&self) -> Result<PhysicalPlan> {
        let InsertMultiTable {
            input_source,
            whens,
            opt_else,
            overwrite,
            is_first,
            intos,
        } = &self.plan;

        let (select_plan, select_column_bindings, _metadata) = match input_source {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => {
                let mut builder1 =
                    PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                (
                    builder1.build(s_expr, bind_context.column_set()).await?,
                    bind_context.columns.clone(),
                    metadata,
                )
            }
            _ => unreachable!(),
        };

        let n_target = 0;
        let rule = vec![];

        let duplicate = PhysicalPlan::Duplicate(Box::new(Duplicate {
            plan_id: 0,
            input: Box::new(select_plan),
            n: n_target,
        }));

        let shuffle = PhysicalPlan::Shuffle(Box::new(Shuffle {
            plan_id: 0,
            input: Box::new(duplicate),
            rule,
        }));
        let filters: Result<Vec<RemoteExpr>> = whens
            .iter()
            .map(|v| {
                let expr = cast_expr_to_non_null_boolean(
                    v.condition.as_expr()?.project_column_ref(|col| col.index),
                )?;
                Ok(expr.as_remote_expr())
            })
            .collect();
        Ok(PhysicalPlan::InsertMultiTable(Box::new(
            PhysicalInsertMultiTable {
                plan_id: 0,
                input: Box::new(select_plan),
                select_column_bindings,
                filters: filters?,
                keep_remain: opt_else.is_some(),
            },
        )))
    }
}
