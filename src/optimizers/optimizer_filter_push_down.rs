// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::error::FuseQueryResult;
use crate::optimizers::{IOptimizer, Optimizer};
use crate::planners::{ExpressionPlan, FilterPlan, PlanNode};
use std::collections::HashMap;

pub struct FilterPushDownOptimizer {}

impl FilterPushDownOptimizer {
    pub fn create() -> Self {
        FilterPushDownOptimizer {}
    }
}

/// replaces columns by its name on the projection.
fn rewrite(
    expr: &ExpressionPlan,
    projection: &HashMap<String, ExpressionPlan>,
) -> FuseQueryResult<ExpressionPlan> {
    let expressions = Optimizer::expression_plan_children(expr)?;

    let expressions = expressions
        .iter()
        .map(|e| rewrite(e, &projection))
        .collect::<FuseQueryResult<Vec<_>>>()?;

    if let ExpressionPlan::Field(name) = expr {
        if let Some(expr) = projection.get(name) {
            return Ok(expr.clone());
        }
    }
    Ok(rewrite_expression(&expr, &expressions))
}

fn rewrite_expression(expr: &ExpressionPlan, expressions: &[ExpressionPlan]) -> ExpressionPlan {
    match expr {
        ExpressionPlan::Alias(alias, _) => {
            ExpressionPlan::Alias(alias.clone(), Box::from(expressions[0].clone()))
        }
        ExpressionPlan::Field(_) => expr.clone(),
        ExpressionPlan::Constant(_) => expr.clone(),
        ExpressionPlan::BinaryExpression { op, .. } => ExpressionPlan::BinaryExpression {
            left: Box::new(expressions[0].clone()),
            op: op.clone(),
            right: Box::new(expressions[1].clone()),
        },
        ExpressionPlan::Function { op, .. } => ExpressionPlan::Function {
            op: op.clone(),
            args: expressions.to_vec(),
        },
    }
}

impl IOptimizer for FilterPushDownOptimizer {
    fn name(&self) -> &str {
        "FilterPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> FuseQueryResult<PlanNode> {
        let mut plans = plan.to_plans()?;
        let projection_map = Optimizer::projection_to_map(plan)?;

        for plan in plans.iter_mut() {
            if let PlanNode::Filter(filter) = plan {
                let rewritten = rewrite(&filter.predicate, &projection_map)?;
                let new_filter = FilterPlan {
                    predicate: rewritten,
                    input: filter.input.clone(),
                };
                *filter = new_filter;
            }
        }
        PlanNode::plans_to_node(&plans)
    }
}
