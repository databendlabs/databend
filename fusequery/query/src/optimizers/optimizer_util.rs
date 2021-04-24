// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use anyhow::Result;
use common_planners::ExpressionPlan;
use common_planners::PlanNode;

pub struct OptimizerUtil;

impl OptimizerUtil {
    pub fn projections_to_map(
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

    /// replaces expression columns by its name on the projection.
    pub fn rewrite_alias_expr(
        expr: &ExpressionPlan,
        projection: &HashMap<String, ExpressionPlan>
    ) -> Result<ExpressionPlan> {
        let expressions = Self::expression_plan_children(expr)?;

        let expressions = expressions
            .iter()
            .map(|e| Self::rewrite_alias_expr(e, &projection))
            .collect::<Result<Vec<_>>>()?;

        if let ExpressionPlan::Column(name) = expr {
            if let Some(expr) = projection.get(name) {
                return Ok(expr.clone());
            }
        }
        Ok(Self::rebuild_from_exprs(&expr, &expressions))
    }

    /// replaces expressions columns by its name on the projection.
    pub fn rewrite_alias_exprs(
        projection_map: &HashMap<String, ExpressionPlan>,
        exprs: &[ExpressionPlan]
    ) -> Result<Vec<ExpressionPlan>> {
        exprs
            .iter()
            .map(|x| Self::rewrite_alias_expr(x, projection_map))
            .collect::<Result<Vec<_>>>()
    }
}
