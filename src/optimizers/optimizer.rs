// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::contexts::FuseQueryContextRef;
use crate::error::FuseQueryResult;
use crate::optimizers::FilterPushDownOptimizer;
use crate::planners::{ExpressionPlan, PlanNode};
use std::collections::HashMap;

pub trait IOptimizer {
    fn name(&self) -> &str;
    fn optimize(&mut self, plan: &PlanNode) -> FuseQueryResult<PlanNode>;
}

pub struct Optimizer {
    optimizers: Vec<Box<dyn IOptimizer>>,
}

impl Optimizer {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        let optimizers: Vec<Box<dyn IOptimizer>> =
            vec![Box::new(FilterPushDownOptimizer::create(ctx))];
        Optimizer { optimizers }
    }

    pub fn optimize(&mut self, plan: &PlanNode) -> FuseQueryResult<PlanNode> {
        let mut plan = plan.clone();
        for optimizer in self.optimizers.iter_mut() {
            plan = optimizer.optimize(&plan)?;
        }
        Ok(plan)
    }

    fn projections_to_map(
        plan: &PlanNode,
        map: &mut HashMap<String, ExpressionPlan>,
    ) -> FuseQueryResult<()> {
        match plan {
            PlanNode::Projection(v) => {
                v.schema.fields().iter().enumerate().for_each(|(i, field)| {
                    let expr = match &v.expr[i] {
                        ExpressionPlan::Alias(_alias, plan) => plan.as_ref().clone(),
                        other => other.clone(),
                    };
                    map.insert(field.name().clone(), expr);
                })
            }
            PlanNode::Aggregate(v) => Self::projections_to_map(v.input.as_ref(), map)?,
            PlanNode::Filter(v) => Self::projections_to_map(v.input.as_ref(), map)?,
            PlanNode::Limit(v) => Self::projections_to_map(v.input.as_ref(), map)?,
            PlanNode::Explain(v) => Self::projections_to_map(v.plan.as_ref(), map)?,
            PlanNode::Select(v) => Self::projections_to_map(v.plan.as_ref(), map)?,
            _ => {}
        }
        Ok(())
    }

    pub fn projection_to_map(plan: &PlanNode) -> FuseQueryResult<HashMap<String, ExpressionPlan>> {
        let mut map = HashMap::new();
        Self::projections_to_map(plan, &mut map)?;
        Ok(map)
    }

    pub fn expression_plan_children(expr: &ExpressionPlan) -> FuseQueryResult<Vec<ExpressionPlan>> {
        Ok(match expr {
            ExpressionPlan::Alias(_, expr) => vec![expr.as_ref().clone()],
            ExpressionPlan::Field(_) => vec![],
            ExpressionPlan::Constant(_) => vec![],
            ExpressionPlan::BinaryExpression { left, right, .. } => {
                vec![left.as_ref().clone(), right.as_ref().clone()]
            }
            ExpressionPlan::Function { args, .. } => args.clone(),
            ExpressionPlan::Wildcard => vec![],
        })
    }

    pub fn rebuild_from_exprs(
        expr: &ExpressionPlan,
        expressions: &[ExpressionPlan],
    ) -> ExpressionPlan {
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
            other => other.clone(),
        }
    }
}
