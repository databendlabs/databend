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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::series::Series;
use common_datavalues::series::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Expression;
use common_planners::InsertIntoPlan;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_planners::SinkPlan;
use common_planners::StagePlan;
use common_streams::DataBlockStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use futures::TryStreamExt;

use crate::interpreters::plan_scheduler_ext;
use crate::interpreters::utils::apply_plan_rewrite;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::optimizers::Optimizers;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::storages::Table;

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
            let input_stream = if self.plan.value_exprs_opt.is_some() {
                // if values are provided in SQL
                // e.g. `insert into ... value(...), ...`
                let values_exprs = self.plan.value_exprs_opt.clone().take().unwrap();
                let blocks = self.block_from_values_exprs(values_exprs)?;
                let stream: SendableDataBlockStream =
                    Box::pin(futures::stream::iter(vec![DataBlock::concat_blocks(
                        &blocks,
                    )]));

                Ok(stream)
            } else {
                // if values are provided as a block stream
                // e.g. using clickhouse client
                input_stream
                    .take()
                    .ok_or_else(|| ErrorCode::EmptyData("input stream not exist or consumed"))
            }?;

            let progress_stream = Box::pin(ProgressStream::try_create(
                input_stream,
                self.ctx.progress_callback()?,
            )?);

            table.append_data(self.ctx.clone(), progress_stream).await?
        };

        // feed back the append operation logs to table
        table
            .commit(
                self.ctx.clone(),
                append_op_logs.try_collect().await?,
                self.plan.overwrite,
            )
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
        let optimized_plan =
            apply_plan_rewrite(Optimizers::create(self.ctx.clone()), &select_plan.input)?;

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

    fn block_from_values_exprs(
        &self,
        values_exprs: Vec<Vec<Expression>>,
    ) -> Result<Vec<DataBlock>> {
        let dummy = DataSchemaRefExt::create(vec![DataField::new("dummy", DataType::UInt8, false)]);
        let one_row_block = DataBlock::create_by_array(dummy.clone(), vec![Series::new(vec![1u8])]);

        values_exprs
            .iter()
            .map(|exprs| {
                let executor = ExpressionExecutor::try_create(
                    "Insert into from values",
                    dummy.clone(),
                    self.plan.schema(),
                    exprs.clone(),
                    true,
                )?;
                executor.execute(&one_row_block)
            })
            .collect::<Result<Vec<_>>>()
    }
}
