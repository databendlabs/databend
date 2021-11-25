// Copyright 2020 Datafuse Labs.
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

use std::io::Cursor;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::InsertIntoPlan;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_planners::SinkPlan;
use common_planners::StagePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_streams::SourceStream;
use common_streams::ValueSource;
use futures::TryStreamExt;

use crate::catalogs::Table;
use crate::interpreters::utils::apply_plan_rewrite;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::sessions::QueryContext;

pub struct InsertIntoInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertIntoPlan,
}

impl InsertIntoInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InsertIntoPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertIntoInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertIntoInterpreter {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute(
        &self,
        mut input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let database = &self.plan.db_name;
        let table = &self.plan.tbl_name;
        let table = self.ctx.get_table(database, table).await?;

        let append_op_logs = if let Some(plan_node) = &self.plan.select_plan {
            // if there is a PlanNode that provides values
            // e.g. `insert into ... as select ...`
            self.insert_with_select_plan(plan_node.as_ref(), table.as_ref())
                .await?
        } else {
            let input_stream = if self.plan.values_opt.is_some() {
                // if values are provided in SQL
                // e.g. `insert into ... value(...), ...`
                let values = self.plan.values_opt.clone().take().ok_or_else(|| {
                    ErrorCode::EmptyData("values of insert plan not exist or consumed")
                })?;
                let block_size = self.ctx.get_settings().get_max_block_size()? as usize;
                let values_source =
                    ValueSource::new(Cursor::new(values), self.plan.schema(), block_size);
                let stream_source = SourceStream::new(Box::new(values_source));
                stream_source.execute().await
            } else {
                // if values are provided as a block stream
                // e.g. using clickhouse client
                input_stream
                    .take()
                    .ok_or_else(|| ErrorCode::EmptyData("input stream not exist or consumed"))
            }?;
            table.append_data(self.ctx.clone(), input_stream).await?
        };

        // feed back the append operation logs to table
        table
            .commit(self.ctx.clone(), append_op_logs.try_collect().await?)
            .await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
impl InsertIntoInterpreter {
    async fn insert_with_select_plan(
        &self,
        plan_node: &PlanNode,
        table: &dyn Table,
    ) -> Result<SendableDataBlockStream> {
        if let PlanNode::Select(sel) = plan_node {
            let optimized_plan = self.rewrite_plan(sel, table.get_table_info())?;
            plan_scheduler_ext::schedule_query(&self.ctx, &optimized_plan).await
        } else {
            Err(ErrorCode::UnknownTypeOfQuery(format!(
                "Unsupported select query plan for insert_into interpreter, {}",
                plan_node.name()
            )))
        }
    }

    fn rewrite_plan(&self, select_plan: &SelectPlan, table_info: &TableInfo) -> Result<PlanNode> {
        let output_schema = self.plan.schema();
        let select_schema = select_plan.schema();

        // validate schema
        if select_schema.fields().len() < output_schema.fields().len() {
            return Err(ErrorCode::BadArguments(
                "Fields in select statement is less than expected",
            ));
        }

        // check if cast needed
        let cast_needed = select_schema != output_schema;

        // optimize and rewrite the SelectPlan.input
        let optimized_plan = apply_plan_rewrite(
            self.ctx.clone(),
            Optimizers::create(self.ctx.clone()),
            &select_plan.input,
        )?;

        // rewrite the optimized the plan
        let rewritten_plan = match optimized_plan {
            // if it is a StagePlan Node, we insert the a SinkPlan in between the Stage and Stage.input
            // i.e.
            //    StagePlan <~ PlanNodeA  => StagePlan <~ Sink <~ PlanNodeA
            PlanNode::Stage(r) => {
                let prev_input = r.input.clone();
                let sink = PlanNode::Sink(SinkPlan {
                    table_info: table_info.clone(),
                    input: prev_input,
                    cast_needed,
                });
                PlanNode::Stage(StagePlan {
                    kind: r.kind,
                    input: Arc::new(sink),
                    scatters_expr: r.scatters_expr,
                })
            }
            // otherwise, we just prepend a SinkPlan
            // i.e.
            //    node <~ PlanNodeA  => Sink<~ node <~ PlanNodeA
            node => PlanNode::Sink(SinkPlan {
                table_info: table_info.clone(),
                input: Arc::new(node),
                cast_needed,
            }),
        };
        Ok(rewritten_plan)
    }
}
