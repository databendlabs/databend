// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::error::FuseQueryResult;
use crate::optimizers::{IOptimizer, Optimizer};
use crate::planners::{ExpressionPlan, FilterPlan, PlanNode};
use crate::sessions::FuseQueryContextRef;
use std::collections::HashMap;

pub struct FilterPushDownOptimizer {
    ctx: FuseQueryContextRef,
}

impl FilterPushDownOptimizer {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        FilterPushDownOptimizer { ctx }
    }
}

/// replaces columns by its name on the projection.
fn rewrite_alias_expr(
    expr: &ExpressionPlan,
    projection: &HashMap<String, ExpressionPlan>,
) -> FuseQueryResult<ExpressionPlan> {
    let expressions = Optimizer::expression_plan_children(expr)?;

    let expressions = expressions
        .iter()
        .map(|e| rewrite_alias_expr(e, &projection))
        .collect::<FuseQueryResult<Vec<_>>>()?;

    if let ExpressionPlan::Field(name) = expr {
        if let Some(expr) = projection.get(name) {
            return Ok(expr.clone());
        }
    }
    Ok(Optimizer::rebuild_from_exprs(&expr, &expressions))
}

impl IOptimizer for FilterPushDownOptimizer {
    fn name(&self) -> &str {
        "FilterPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> FuseQueryResult<PlanNode> {
        let mut plans = plan.get_all_nodes()?;
        let projection_map = Optimizer::projection_to_map(plan)?;

        for plan in plans.iter_mut() {
            if let PlanNode::Filter(filter) = plan {
                let rewritten = rewrite_alias_expr(&filter.predicate, &projection_map)?;
                let new_filter = FilterPlan {
                    predicate: rewritten,
                    input: filter.input.clone(),
                };
                *filter = new_filter;
            }
        }
        PlanNode::plan_list_to_node(self.ctx.clone(), &plans)
    }
}
