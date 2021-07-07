// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::AggregatorPartialPlan;
use common_planners::Expression;
use common_planners::ExpressionPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::PlanRewriter;

use crate::optimizers::Optimizer;
use crate::sessions::FuseQueryContextRef;

struct StatisticsExactImpl<'a> {
    ctx: &'a FuseQueryContextRef,
}

pub struct StatisticsExactOptimizer {
    ctx: FuseQueryContextRef,
}

impl<'plan> PlanRewriter<'plan> for StatisticsExactImpl<'_> {
    fn rewrite_aggregate_partial(
        &mut self,
        plan: &'plan AggregatorPartialPlan,
    ) -> Result<PlanNode> {
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
                }],
                PlanNode::Expression(ExpressionPlan { input, .. }),
            ) if op == "count" && args.len() == 1 => match (&args[0], input.as_ref()) {
                (Expression::Literal(_), PlanNode::ReadSource(read_source_plan))
                    if read_source_plan.statistics.is_exact =>
                {
                    let db_name = "system";
                    let table_name = "one";

                    let dummy_read_plan =
                        self.ctx.get_table(db_name, table_name).and_then(|table| {
                            table
                                .schema()
                                .and_then(|ref schema| {
                                    PlanBuilder::scan(db_name, table_name, schema, None, None, None)
                                })
                                .and_then(|builder| builder.build())
                                .and_then(|dummy_scan_plan| match dummy_scan_plan {
                                    PlanNode::Scan(ref dummy_scan_plan) => table
                                        .read_plan(
                                            self.ctx.clone(),
                                            dummy_scan_plan,
                                            self.ctx.get_max_threads()? as usize,
                                        )
                                        .map(PlanNode::ReadSource),
                                    _unreachable_plan => {
                                        panic!("Logical error: cannot downcast to scan plan")
                                    }
                                })
                        })?;
                    let rows = read_source_plan.statistics.read_rows as u64;
                    let states = DataValue::Struct(vec![DataValue::UInt64(Some(rows))]);
                    let ser = serde_json::to_string(&states)?;
                    PlanBuilder::from(&dummy_read_plan)
                        .expression(
                            &[Expression::Literal(DataValue::Utf8(Some(ser.clone())))],
                            "Exact Statistics",
                        )?
                        .project(&[Expression::Column(ser).alias("count(0)")])?
                        .build()?
                }
                _ => PlanNode::AggregatorPartial(plan.clone()),
            },
            (_, _, _) => PlanNode::AggregatorPartial(plan.clone()),
        };
        Ok(new_plan)
    }
}

#[async_trait::async_trait]
impl Optimizer for StatisticsExactOptimizer {
    fn name(&self) -> &str {
        "StatisticsExact"
    }

    async fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = StatisticsExactImpl { ctx: &self.ctx };
        visitor.rewrite_plan_node(plan)
    }
}

impl StatisticsExactOptimizer {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        StatisticsExactOptimizer { ctx }
    }
}
