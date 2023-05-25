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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_sql::executor::PhysicalPlanBuilder;
use common_sql::plans::Plan;
use common_sql::plans::RefreshIndexPlan;
use common_storages_fuse::operations::AggIndexSink;
use common_storages_fuse::FuseTable;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;

pub struct RefreshIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshIndexPlan,
}

impl RefreshIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshIndexPlan) -> Result<Self> {
        Ok(RefreshIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshIndexInterpreter {
    fn name(&self) -> &str {
        "RefreshIndexInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let (query_plan, schema, select_column_bindings) = match self.plan.query_plan.as_ref() {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => {
                let mut builder2 = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone());
                (
                    builder2.build(s_expr.as_ref()).await?,
                    bind_context.output_schema(),
                    bind_context.columns.clone(),
                )
            }
            _ => {
                return Err(ErrorCode::SemanticError(
                    "Refresh aggregating index encounter Non-Query Plan",
                ));
            }
        };

        dbg!(&select_column_bindings);

        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &query_plan, false).await?;

        let input_schema = query_plan.output_schema()?;
        dbg!(&input_schema);

        let fuse_table = FuseTable::do_create(self.plan.table_info.clone())?;
        let fuse_table: Arc<FuseTable> = fuse_table.into();
        let table_schema = infer_table_schema(&schema)?;
        dbg!(&table_schema);

        build_res.main_pipeline.resize(1)?;
        build_res.main_pipeline.add_sink(|input| {
            AggIndexSink::try_create(
                input,
                fuse_table.get_operator(),
                self.plan.index_id,
                fuse_table.get_write_settings(),
                table_schema.clone(),
                input_schema.clone(),
                &select_column_bindings,
            )
        })?;

        return Ok(build_res);
    }
}
