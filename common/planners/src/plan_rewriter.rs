// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::collections::HashSet;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::ExpressionAction;
use crate::PlanNode;

pub struct PlanRewriter {}

struct QueryAliasData {
    aliases: HashMap<String, ExpressionAction>,
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
    pub fn rewrite_projection_aliases(exprs: &[ExpressionAction]) -> Result<Vec<ExpressionAction>> {
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
        exprs: &[ExpressionAction],
        mp: &mut HashMap<String, ExpressionAction>
    ) -> Result<()> {
        for expr in exprs.iter() {
            if let ExpressionAction::Alias(alias, alias_expr) = expr {
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
        expr: &ExpressionAction,
        data: &mut QueryAliasData
    ) -> Result<ExpressionAction> {
        match expr {
            ExpressionAction::Column(field) => {
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

            ExpressionAction::BinaryExpression { left, op, right } => {
                let left_new = PlanRewriter::expr_rewrite_alias(left, data)?;
                let right_new = PlanRewriter::expr_rewrite_alias(right, data)?;

                Ok(ExpressionAction::BinaryExpression {
                    left: Box::new(left_new),
                    op: op.clone(),
                    right: Box::new(right_new)
                })
            }

            ExpressionAction::Function { op, args } => {
                let new_args: Result<Vec<ExpressionAction>> = args
                    .iter()
                    .map(|v| PlanRewriter::expr_rewrite_alias(v, data))
                    .collect();

                match new_args {
                    Ok(v) => Ok(ExpressionAction::Function {
                        op: op.clone(),
                        args: v
                    }),
                    Err(v) => Err(v)
                }
            }

            ExpressionAction::Alias(alias, plan) => {
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

                Ok(ExpressionAction::Alias(alias.clone(), Box::new(new_expr)))
            }
            ExpressionAction::Cast { expr, data_type } => {
                let new_expr = PlanRewriter::expr_rewrite_alias(expr, data)?;
                Ok(ExpressionAction::Cast {
                    expr: Box::new(new_expr),
                    data_type: data_type.clone()
                })
            }
            ExpressionAction::Wildcard
            | ExpressionAction::Literal(_)
            | ExpressionAction::Sort { .. } => Ok(expr.clone())
        }
    }

    /// replaces expression columns by its name on the projection.
    /// SELECT a as b ... where b>1
    /// ->
    /// SELECT a as b ... where a>1
    pub fn rewrite_alias_expr(
        projection_map: &HashMap<String, ExpressionAction>,
        expr: &ExpressionAction
    ) -> Result<ExpressionAction> {
        let expressions = Self::expression_plan_children(expr)?;

        let expressions = expressions
            .iter()
            .map(|e| Self::rewrite_alias_expr(projection_map, e))
            .collect::<Result<Vec<_>>>()?;

        if let ExpressionAction::Column(name) = expr {
            if let Some(expr) = projection_map.get(name) {
                return Ok(expr.clone());
            }
        }
        Ok(Self::rebuild_from_exprs(&expr, &expressions))
    }

    /// replaces expressions columns by its name on the projection.
    pub fn rewrite_alias_exprs(
        projection_map: &HashMap<String, ExpressionAction>,
        exprs: &[ExpressionAction]
    ) -> Result<Vec<ExpressionAction>> {
        exprs
            .iter()
            .map(|e| Self::rewrite_alias_expr(projection_map, e))
            .collect::<Result<Vec<_>>>()
    }

    /// Collect all unique projection fields to a map.
    pub fn projection_to_map(plan: &PlanNode) -> Result<HashMap<String, ExpressionAction>> {
        let mut map = HashMap::new();
        Self::projections_to_map(plan, &mut map)?;
        Ok(map)
    }

    /// Get the expression children.
    pub fn expression_plan_children(expr: &ExpressionAction) -> Result<Vec<ExpressionAction>> {
        Ok(match expr {
            ExpressionAction::Alias(_, expr) => vec![expr.as_ref().clone()],
            ExpressionAction::Column(_) => vec![],
            ExpressionAction::Literal(_) => vec![],
            ExpressionAction::BinaryExpression { left, right, .. } => {
                vec![left.as_ref().clone(), right.as_ref().clone()]
            }
            ExpressionAction::Function { args, .. } => args.clone(),
            ExpressionAction::Wildcard => vec![],
            ExpressionAction::Sort { expr, .. } => vec![expr.as_ref().clone()],
            ExpressionAction::Cast { expr, .. } => vec![expr.as_ref().clone()]
        })
    }

    /// Get the leaves of an expression.
    pub fn expression_plan_columns(expr: &ExpressionAction) -> Result<Vec<ExpressionAction>> {
        Ok(match expr {
            ExpressionAction::Alias(_, expr) => Self::expression_plan_columns(expr)?,
            ExpressionAction::Column(_) => vec![expr.clone()],
            ExpressionAction::Literal(_) => vec![],
            ExpressionAction::BinaryExpression { left, right, .. } => {
                let mut l = Self::expression_plan_columns(left)?;
                let mut r = Self::expression_plan_columns(right)?;
                l.append(&mut r);
                l
            }
            ExpressionAction::Function { args, .. } => {
                let mut v = vec![];
                for arg in args {
                    let mut col = Self::expression_plan_columns(arg)?;
                    v.append(&mut col);
                }
                v
            }
            ExpressionAction::Wildcard => vec![],
            ExpressionAction::Sort { expr, .. } => Self::expression_plan_columns(expr)?,
            ExpressionAction::Cast { expr, .. } => Self::expression_plan_columns(expr)?
        })
    }

    /// Collect all unique projection fields to a map.
    fn projections_to_map(
        plan: &PlanNode,
        map: &mut HashMap<String, ExpressionAction>
    ) -> Result<()> {
        match plan {
            PlanNode::Projection(v) => {
                v.schema.fields().iter().enumerate().for_each(|(i, field)| {
                    let expr = match &v.expr[i] {
                        ExpressionAction::Alias(_alias, plan) => plan.as_ref().clone(),
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

    fn rebuild_from_exprs(
        expr: &ExpressionAction,
        expressions: &[ExpressionAction]
    ) -> ExpressionAction {
        match expr {
            ExpressionAction::Alias(alias, _) => {
                ExpressionAction::Alias(alias.clone(), Box::from(expressions[0].clone()))
            }
            ExpressionAction::Column(_) => expr.clone(),
            ExpressionAction::Literal(_) => expr.clone(),
            ExpressionAction::BinaryExpression { op, .. } => ExpressionAction::BinaryExpression {
                left: Box::new(expressions[0].clone()),
                op: op.clone(),
                right: Box::new(expressions[1].clone())
            },
            ExpressionAction::Function { op, .. } => ExpressionAction::Function {
                op: op.clone(),
                args: expressions.to_vec()
            },
            other => other.clone()
        }
    }

    /// Check if aggr is in group-by's list
    /// Case1: group is a column, the name needs to match with aggr
    /// Case2: aggr is an alias, unfold aggr
    /// Case3: group and aggr are exactly the same expression
    pub fn check_aggr_in_group_expr(
        aggr: &ExpressionAction,
        group_by_names: &HashSet<String>,
        input_schema: &DataSchemaRef
    ) -> Result<bool> {
        match aggr {
            ExpressionAction::Alias(alias, plan) => {
                if group_by_names.contains(alias) {
                    return Ok(true);
                } else {
                    return Self::check_aggr_in_group_expr(plan, group_by_names, input_schema);
                }
            }
            _ => {
                let aggr_str = format!("{:?}", aggr);
                if group_by_names.contains(&aggr_str) {
                    return Ok(true);
                } else {
                    let columns = Self::expression_plan_columns(aggr)?;
                    for col in columns {
                        let cn = col.to_data_field(input_schema)?.name().clone();
                        if !group_by_names.contains(&cn) {
                            return Ok(false);
                        }
                    }
                }
            }
        };
        Ok(true)
    }

    pub fn exprs_to_fields(
        exprs: &[ExpressionAction],
        input_schema: &DataSchemaRef
    ) -> Result<Vec<DataField>> {
        exprs
            .iter()
            .map(|expr| expr.to_data_field(input_schema))
            .collect::<Result<_>>()
    }

    pub fn exprs_to_names(exprs: &[ExpressionAction], names: &mut HashSet<String>) -> Result<()> {
        for expr in exprs {
            let name = String::from(format!("{:?}", expr).as_str());
            names.insert(name.clone());
        }
        Ok(())
    }
}
