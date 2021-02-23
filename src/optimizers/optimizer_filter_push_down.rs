// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use crate::datavalues::DataSchema;
use crate::error::FuseQueryResult;
use crate::optimizers::{IOptimizer, Optimizer};
use crate::planners::{walk_postorder, EmptyPlan, ExpressionPlan, FilterPlan, PlanNode};
use crate::sessions::FuseQueryContextRef;

pub struct FilterPushDownOptimizer {}

impl FilterPushDownOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        FilterPushDownOptimizer {}
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
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty()),
        });

        let projection_map = Optimizer::projection_to_map(plan)?;
        walk_postorder(plan, |node| {
            if let PlanNode::Filter(filter) = node {
                let rewritten_expr = rewrite_alias_expr(&filter.predicate, &projection_map)?;
                let mut new_filter_node = PlanNode::Filter(FilterPlan {
                    predicate: rewritten_expr,
                    input: rewritten_node.input(),
                });
                new_filter_node.set_input(&rewritten_node)?;
                rewritten_node = new_filter_node;
            } else {
                let mut clone_node = node.clone();
                clone_node.set_input(&rewritten_node)?;
                rewritten_node = clone_node;
            }
            Ok(true)
        })?;

        Ok(rewritten_node)
    }
}
