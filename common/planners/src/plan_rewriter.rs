// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::collections::HashSet;

use common_exception::ErrorCodes;
use common_exception::Result;

use crate::ExpressionPlan;
use crate::PlanNode;

pub struct PlanRewriter {}

struct QueryAliasData {
    aliases: HashMap<String, ExpressionPlan>,
    inside_aliases: HashSet<String>,
    // deepest alias current step in
    current_alias: String
}

impl PlanRewriter {
    /// Recursively extract the aliases in projection exprs
    ///
    /// SELECT (x+1) as y, y*y FROM ..
    /// ->
    /// SELECT (x+1) as y, (x+1)*(x+1) FROM ..
    pub fn rewrite_projection_aliases(exprs: Vec<ExpressionPlan>) -> Result<Vec<ExpressionPlan>> {
        let mut mp = HashMap::new();
        PlanRewriter::alias_exprs_to_map(&exprs, &mut mp)?;

        let mut data = QueryAliasData {
            aliases: mp,
            inside_aliases: HashSet::new(),
            current_alias: "".into()
        };

        exprs
            .iter()
            .map(|expr| PlanRewriter::expr_rewrite_alias(expr, &mut data))
            .collect()
    }

    fn alias_exprs_to_map(
        exprs: &[ExpressionPlan],
        mp: &mut HashMap<String, ExpressionPlan>
    ) -> Result<()> {
        for expr in exprs.iter() {
            if let ExpressionPlan::Alias(alias, alias_expr) = expr {
                if let Some(expr_result) = mp.get(alias) {
                    let hash_result = format!("{:?}", expr_result);
                    let hash_expr = format!("{:?}", expr);

                    if hash_result != hash_expr {
                        return Result::Err(ErrorCodes::SyntexException(format!(
                            "Planner Error: Different expressions with the same alias {}",
                            alias
                        )));
                    }
                }
                mp.insert(alias.clone(), *alias_expr.clone());
            }
        }
        Ok(())
    }

    fn expr_rewrite_alias(
        expr: &ExpressionPlan,
        data: &mut QueryAliasData
    ) -> Result<ExpressionPlan> {
        match expr {
            ExpressionPlan::Column(field) => {
                // x + 1 --> x
                if *field == data.current_alias {
                    return Ok(expr.clone());
                }

                // x + 1 --> y, y + 1 --> x
                if data.inside_aliases.contains(field) {
                    return Result::Err(ErrorCodes::SyntexException(format!(
                        "Planner Error: Cyclic aliases: {}",
                        field
                    )));
                }

                let tmp = data.aliases.get(field).cloned();
                if let Some(e) = tmp {
                    let previous_alias = data.current_alias.clone();

                    data.current_alias = field.clone();
                    data.inside_aliases.insert(field.clone());
                    let c = PlanRewriter::expr_rewrite_alias(&e, data)?;
                    data.inside_aliases.remove(field);
                    data.current_alias = previous_alias;

                    return Ok(c);
                }
                Ok(expr.clone())
            }

            ExpressionPlan::BinaryExpression { left, op, right } => {
                let left_new = PlanRewriter::expr_rewrite_alias(left, data)?;
                let right_new = PlanRewriter::expr_rewrite_alias(right, data)?;

                Ok(ExpressionPlan::BinaryExpression {
                    left: Box::new(left_new),
                    op: op.clone(),
                    right: Box::new(right_new)
                })
            }

            ExpressionPlan::Function { name, args } => {
                let new_args: Result<Vec<ExpressionPlan>> = args
                    .iter()
                    .map(|v| PlanRewriter::expr_rewrite_alias(v, data))
                    .collect();

                match new_args {
                    Ok(v) => Ok(ExpressionPlan::Function {
                        name: name.clone(),
                        args: v
                    }),
                    Err(v) => Err(v)
                }
            }

            ExpressionPlan::Alias(alias, plan) => {
                if data.inside_aliases.contains(alias) {
                    return Result::Err(ErrorCodes::SyntexException(format!(
                        "Planner Error: Cyclic aliases: {}",
                        alias
                    )));
                }

                let previous_alias = data.current_alias.clone();
                data.current_alias = alias.clone();
                data.inside_aliases.insert(alias.clone());
                let new_expr = PlanRewriter::expr_rewrite_alias(plan, data)?;
                data.inside_aliases.remove(alias);
                data.current_alias = previous_alias;

                Ok(ExpressionPlan::Alias(alias.clone(), Box::new(new_expr)))
            }
            ExpressionPlan::Cast { expr, data_type } => {
                let new_expr = PlanRewriter::expr_rewrite_alias(expr, data)?;
                Ok(ExpressionPlan::Cast {
                    expr: Box::new(new_expr),
                    data_type: data_type.clone()
                })
            }
            ExpressionPlan::Wildcard | ExpressionPlan::Literal(_) | ExpressionPlan::Sort { .. } => {
                Ok(expr.clone())
            }
        }
    }

    /// replaces expression columns by its name on the projection.
    /// SELECT a as b ... where b>1
    /// ->
    /// SELECT a as b ... where a>1
    pub fn rewrite_alias_expr(
        projection_map: &HashMap<String, ExpressionPlan>,
        expr: &ExpressionPlan
    ) -> Result<ExpressionPlan> {
        let expressions = Self::expression_plan_children(expr)?;

        let expressions = expressions
            .iter()
            .map(|e| Self::rewrite_alias_expr(projection_map, e))
            .collect::<Result<Vec<_>>>()?;

        if let ExpressionPlan::Column(name) = expr {
            if let Some(expr) = projection_map.get(name) {
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
            .map(|e| Self::rewrite_alias_expr(projection_map, e))
            .collect::<Result<Vec<_>>>()
    }

    /// Collect all unique projection fields to a map.
    pub fn projection_to_map(plan: &PlanNode) -> Result<HashMap<String, ExpressionPlan>> {
        let mut map = HashMap::new();
        Self::projections_to_map(plan, &mut map)?;
        Ok(map)
    }

    /// Get the expression children.
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

    /// Collect all unique projection fields to a map.
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
            // Aggregator aggr_expr is the projection
            PlanNode::AggregatorPartial(v) => {
                for expr in &v.aggr_expr {
                    let field = expr.to_data_field(&v.input.schema())?;
                    map.insert(field.name().clone(), expr.clone());
                }
            }
            // Aggregator aggr_expr is the projection
            PlanNode::AggregatorFinal(v) => {
                for expr in &v.aggr_expr {
                    let field = expr.to_data_field(&v.input.schema())?;
                    map.insert(field.name().clone(), expr.clone());
                }
            }
            other => Self::projections_to_map(other.input().as_ref(), map)?
        }
        Ok(())
    }

    fn rebuild_from_exprs(expr: &ExpressionPlan, expressions: &[ExpressionPlan]) -> ExpressionPlan {
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
            ExpressionPlan::Function { name, .. } => ExpressionPlan::Function {
                name: name.clone(),
                args: expressions.to_vec()
            },
            other => other.clone()
        }
    }
}
