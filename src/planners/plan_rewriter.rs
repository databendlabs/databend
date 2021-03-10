// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::ExpressionPlan;
use std::collections::{HashMap, HashSet};

pub struct PlanRewriter {}

impl PlanRewriter {
    /// Recursively extract the aliases in exprs
    pub fn exprs_extract_aliases(
        exprs: Vec<ExpressionPlan>,
    ) -> FuseQueryResult<Vec<ExpressionPlan>> {
        let mut mp = HashMap::new();
        PlanRewriter::exprs_to_map(&exprs, &mut mp)?;

        let mut forbidden_aliases = HashSet::new();
        let mut inside_aliases = HashSet::new();

        exprs
            .iter()
            .map(|expr| {
                PlanRewriter::expr_rewrite_alias(
                    expr,
                    &mp,
                    &mut inside_aliases,
                    &mut forbidden_aliases,
                )
            })
            .collect()
    }

    fn exprs_to_map(
        exprs: &[ExpressionPlan],
        mp: &mut HashMap<String, ExpressionPlan>,
    ) -> FuseQueryResult<()> {
        for expr in exprs.iter() {
            if let ExpressionPlan::Alias(alias, alias_expr) = expr {
                if let Some(expr_result) = mp.get(alias) {
                    let hash_result = format!("{:?}", expr_result);
                    let hash_expr = format!("{:?}", expr);

                    if hash_result != hash_expr {
                        return Err(FuseQueryError::build_plan_error(format!(
                            "Different expressions with the same alias {}",
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
        mp: &HashMap<String, ExpressionPlan>,
        inside_aliases: &mut HashSet<String>,
        forbidden_aliases: &mut HashSet<String>,
    ) -> FuseQueryResult<ExpressionPlan> {
        match expr {
            ExpressionPlan::Field(field) => {
                if inside_aliases.contains(field) {
                    return Err(FuseQueryError::build_plan_error(format!(
                        "Cyclic aliases: {}",
                        field
                    )));
                }

                if let Some(e) = mp.get(field) {
                    forbidden_aliases.insert(field.clone());
                    let c =
                        PlanRewriter::expr_rewrite_alias(e, mp, inside_aliases, forbidden_aliases)?;
                    forbidden_aliases.remove(field);
                    return Ok(c);
                }
                Ok(expr.clone())
            }

            ExpressionPlan::BinaryExpression { left, op, right } => {
                let left_new =
                    PlanRewriter::expr_rewrite_alias(left, mp, inside_aliases, forbidden_aliases)?;
                let right_new =
                    PlanRewriter::expr_rewrite_alias(right, mp, inside_aliases, forbidden_aliases)?;

                Ok(ExpressionPlan::BinaryExpression {
                    left: Box::new(left_new),
                    op: op.clone(),
                    right: Box::new(right_new),
                })
            }

            ExpressionPlan::Function { op, args } => {
                let new_args: FuseQueryResult<Vec<ExpressionPlan>> = args
                    .iter()
                    .map(|v| {
                        PlanRewriter::expr_rewrite_alias(v, mp, inside_aliases, forbidden_aliases)
                    })
                    .collect();

                match new_args {
                    Ok(v) => Ok(ExpressionPlan::Function {
                        op: op.clone(),
                        args: v,
                    }),
                    Err(v) => Err(v),
                }
            }

            ExpressionPlan::Alias(alias, plan) => {
                if inside_aliases.contains(alias) {
                    return Err(FuseQueryError::build_plan_error(format!(
                        "Cyclic aliases: {}",
                        alias
                    )));
                }
                inside_aliases.insert(alias.clone());
                let new_expr =
                    PlanRewriter::expr_rewrite_alias(plan, mp, inside_aliases, forbidden_aliases)?;
                inside_aliases.remove(alias);

                Ok(ExpressionPlan::Alias(alias.clone(), Box::new(new_expr)))
            }

            ExpressionPlan::Wildcard | ExpressionPlan::Constant(_) => Ok(expr.clone()),
        }
    }
}
