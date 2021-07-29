// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::Expression;
use common_planners::ExpressionPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::PlanRewriter;
use common_planners::TableScanInfo;

use crate::optimizers::Optimizer;
use crate::sessions::FuseQueryContextRef;

struct StatisticsExactImpl<'a> {
    ctx: &'a FuseQueryContextRef,
}

pub struct StatisticsExactOptimizer {
    ctx: FuseQueryContextRef,
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
                }],
                PlanNode::Expression(ExpressionPlan { input, .. }),
            ) if op == "count" && args.len() == 1 => match (&args[0], input.as_ref()) {
                (Expression::Literal { .. }, PlanNode::ReadSource(read_source_plan))
                    if read_source_plan.statistics.is_exact =>
                {
                    let db_name = "system";
                    let table_name = "one";
                    let table_id = 1;
                    let table_version = None;

                    let dummy_read_plan =
                        self.ctx
                            .get_table(db_name, table_name)
                            .and_then(|table_meta| {
                                let table = table_meta.datasource();
                                table
                                    .schema()
                                    .and_then(|ref schema| {
                                        let tbl_scan_info = TableScanInfo {
                                            table_name,
                                            table_id,
                                            table_version,
                                            table_schema: schema.as_ref(),
                                            table_args: None,
                                        };
                                        PlanBuilder::scan(db_name, tbl_scan_info, None, None)
                                    })
                                    .and_then(|builder| builder.build())
                                    .and_then(|dummy_scan_plan| match dummy_scan_plan {
                                        PlanNode::Scan(ref dummy_scan_plan) => table
                                            .read_plan(
                                                self.ctx.clone(),
                                                dummy_scan_plan,
                                                self.ctx.get_settings().get_max_threads()? as usize,
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
                            &[Expression::create_literal(DataValue::Utf8(Some(
                                ser.clone(),
                            )))],
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

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        Ok(PlanNode::AggregatorFinal(AggregatorFinalPlan {
            schema: plan.schema.clone(),
            schema_before_group_by: plan.schema_before_group_by.clone(),
            aggr_expr: plan.aggr_expr.clone(),
            group_expr: plan.group_expr.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?),
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
        let mut visitor = StatisticsExactImpl { ctx: &self.ctx };
        visitor.rewrite_plan_node(plan)
    }
}

impl StatisticsExactOptimizer {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        StatisticsExactOptimizer { ctx }
    }
}
