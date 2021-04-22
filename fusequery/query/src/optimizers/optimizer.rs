// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use anyhow::Result;
use common_planners::ExpressionPlan;
use common_planners::PlanNode;

use crate::optimizers::FilterPushDownOptimizer;
use crate::sessions::FuseQueryContextRef;

pub trait IOptimizer {
    fn name(&self) -> &str;
    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode>;
}

pub struct Optimizer {
    optimizers: Vec<Box<dyn IOptimizer>>
}

impl Optimizer {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        let optimizers: Vec<Box<dyn IOptimizer>> =
            vec![Box::new(FilterPushDownOptimizer::create(ctx))];
        Optimizer { optimizers }
    }

    pub fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut plan = plan.clone();
        for optimizer in self.optimizers.iter_mut() {
            plan = optimizer.optimize(&plan)?;
        }
        Ok(plan)
    }

    fn projections_to_map(
        plan: &PlanNode,
        map: &mut HashMap<String, ExpressionPlan>
    ) -> Result<()> {
        match plan {
            PlanNode::Projection(v) => {
                v.schema.fields().iter().enumerate().for_each(|(i, field)| {
                    let expr = match &v.expr[i] {
                        ExpressionPlan::Alias(_alias, plan) => plan.as_ref().clone(),
                        other => other.clone()
                    };
                    map.insert(field.name().clone(), expr);
                })
            }
            PlanNode::AggregatorPartial(v) => Self::projections_to_map(v.input.as_ref(), map)?,
            PlanNode::AggregatorFinal(v) => Self::projections_to_map(v.input.as_ref(), map)?,
            PlanNode::Filter(v) => Self::projections_to_map(v.input.as_ref(), map)?,
            PlanNode::Limit(v) => Self::projections_to_map(v.input.as_ref(), map)?,
            PlanNode::Explain(v) => Self::projections_to_map(v.input.as_ref(), map)?,
            PlanNode::Select(v) => Self::projections_to_map(v.input.as_ref(), map)?,
            _ => {}
        }
        Ok(())
    }

    pub fn projection_to_map(plan: &PlanNode) -> Result<HashMap<String, ExpressionPlan>> {
        let mut map = HashMap::new();
        Self::projections_to_map(plan, &mut map)?;
        Ok(map)
    }

    pub fn expression_plan_children(expr: &ExpressionPlan) -> Result<Vec<ExpressionPlan>> {
        Ok(match expr {
            ExpressionPlan::Alias(_, expr) => vec![expr.as_ref().clone()],
            ExpressionPlan::Column(_) => vec![],
            ExpressionPlan::Literal(_) => vec![],
            ExpressionPlan::BinaryExpression { left, right, .. } => {
                vec![left.as_ref().clone(), right.as_ref().clone()]
            }
            ExpressionPlan::Function { args, .. } => args.clone(),
            ExpressionPlan::Wildcard => vec![],
            ExpressionPlan::Sort { expr, .. } => vec![expr.as_ref().clone()],
            ExpressionPlan::Cast { expr, .. } => vec![expr.as_ref().clone()]
        })
    }

    pub fn rebuild_from_exprs(
        expr: &ExpressionPlan,
        expressions: &[ExpressionPlan]
    ) -> ExpressionPlan {
        match expr {
            ExpressionPlan::Alias(alias, _) => {
                ExpressionPlan::Alias(alias.clone(), Box::from(expressions[0].clone()))
            }
            ExpressionPlan::Column(_) => expr.clone(),
            ExpressionPlan::Literal(_) => expr.clone(),
            ExpressionPlan::BinaryExpression { op, .. } => ExpressionPlan::BinaryExpression {
                left: Box::new(expressions[0].clone()),
                op: op.clone(),
                right: Box::new(expressions[1].clone())
            },
            ExpressionPlan::Function { op, .. } => ExpressionPlan::Function {
                op: op.clone(),
                args: expressions.to_vec()
            },
            other => other.clone()
        }
    }
}
