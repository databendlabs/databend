// Copyright 2021 Datafuse Labs.
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

use common_datavalues::prelude::ToDataType;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::Expression;
use common_planners::ExpressionPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::PlanRewriter;

use crate::optimizers::Optimizer;
use crate::sessions::QueryContext;
use crate::storages::ToReadDataSourcePlan;

struct StatisticsExactImpl<'a> {
    ctx: &'a Arc<QueryContext>,
    rewritten: bool,
}

pub struct StatisticsExactOptimizer {
    ctx: Arc<QueryContext>,
}

impl PlanRewriter for StatisticsExactImpl<'_> {
    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
        let new_plan = match (
            &plan.group_expr[..],
            &plan.aggr_expr[..],
            plan.input.as_ref(),
        ) {
            (
                [],
                [Expression::AggregateFunction {
                    ref op,
                    distinct: false,
                    ref args,
                    ..
                }],
                PlanNode::Expression(ExpressionPlan { input, .. }),
            ) if op == "count" && args.len() == 1 => match (&args[0], input.as_ref()) {
                (Expression::Literal { .. }, PlanNode::ReadSource(read_source_plan))
                    if read_source_plan.statistics.is_exact =>
                {
                    let db_name = "system";
                    let table_name = "one";

                    futures::executor::block_on(async move {
                        let table = self.ctx.get_table(db_name, table_name).await?;
                        let source_plan = table.read_plan(self.ctx.clone(), None).await?;
                        let dummy_read_plan = PlanNode::ReadSource(source_plan);

                        let expr = Expression::create_literal_with_type(
                            DataValue::UInt64(read_source_plan.statistics.read_rows as u64),
                            u64::to_data_type(),
                        );

                        self.rewritten = true;
                        let alias_name = plan.aggr_expr[0].column_name();
                        PlanBuilder::from(&dummy_read_plan)
                            .expression(&[expr.clone()], "Exact Statistics")?
                            .project(&[expr.alias(&alias_name)])?
                            .build()
                    })?
                }
                _ => PlanNode::AggregatorPartial(plan.clone()),
            },
            (_, _, _) => PlanNode::AggregatorPartial(plan.clone()),
        };
        Ok(new_plan)
    }

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        let input = self.rewrite_plan_node(plan.input.as_ref())?;

        if self.rewritten {
            self.rewritten = false;
            return Ok(input);
        }

        Ok(PlanNode::AggregatorFinal(AggregatorFinalPlan {
            schema: plan.schema.clone(),
            schema_before_group_by: plan.schema_before_group_by.clone(),
            aggr_expr: plan.aggr_expr.clone(),
            group_expr: plan.group_expr.clone(),
            input: Arc::new(input),
        }))
    }
}

impl Optimizer for StatisticsExactOptimizer {
    fn name(&self) -> &str {
        "StatisticsExact"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        /*
            TODO:
                SELECT COUNT(1), COUNT(1) FROM (
                    SELECT COUNT(1) FROM (
                        SELECT * FROM system.settings LIMIT 1
                    )
                )
        */
        let mut visitor = StatisticsExactImpl {
            ctx: &self.ctx,
            rewritten: false,
        };
        visitor.rewrite_plan_node(plan)
    }
}

impl StatisticsExactOptimizer {
    pub fn create(ctx: Arc<QueryContext>) -> Self {
        StatisticsExactOptimizer { ctx }
    }
}
