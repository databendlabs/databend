// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};

use crate::ExpressionPlan;

pub struct PlanRewriter {}

struct QueryAliasData {
    aliases: HashMap<String, ExpressionPlan>,
    inside_aliases: HashSet<String>,
    // deepest alias current step in
    current_alias: String,
}

impl PlanRewriter {
    /// Recursively extract the aliases in exprs
    pub fn exprs_extract_aliases(exprs: Vec<ExpressionPlan>) -> Result<Vec<ExpressionPlan>> {
        let mut mp = HashMap::new();
        PlanRewriter::exprs_to_map(&exprs, &mut mp)?;

        let mut data = QueryAliasData {
            aliases: mp,
            inside_aliases: HashSet::new(),
            current_alias: "".into(),
        };

        exprs
            .iter()
            .map(|expr| PlanRewriter::expr_rewrite_alias(expr, &mut data))
            .collect()
    }

    fn exprs_to_map(
        exprs: &[ExpressionPlan],
        mp: &mut HashMap<String, ExpressionPlan>,
    ) -> Result<()> {
        for expr in exprs.iter() {
            if let ExpressionPlan::Alias(alias, alias_expr) = expr {
                if let Some(expr_result) = mp.get(alias) {
                    let hash_result = format!("{:?}", expr_result);
                    let hash_expr = format!("{:?}", expr);

                    if hash_result != hash_expr {
                        bail!(
                            "Planner Error: Different expressions with the same alias {}",
                            alias
                        );
                    }
                }
                mp.insert(alias.clone(), *alias_expr.clone());
            }
        }
        Ok(())
    }

    fn expr_rewrite_alias(
        expr: &ExpressionPlan,
        data: &mut QueryAliasData,
    ) -> Result<ExpressionPlan> {
        match expr {
            ExpressionPlan::Column(field) => {
                // x + 1 --> x
                if *field == data.current_alias {
                    return Ok(expr.clone());
                }

                // x + 1 --> y, y + 1 --> x
                if data.inside_aliases.contains(field) {
                    bail!("Planner Error: Cyclic aliases: {}", field);
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
                    right: Box::new(right_new),
                })
            }

            ExpressionPlan::Function { op, args } => {
                let new_args: Result<Vec<ExpressionPlan>> = args
                    .iter()
                    .map(|v| PlanRewriter::expr_rewrite_alias(v, data))
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
                if data.inside_aliases.contains(alias) {
                    bail!("Planner Error: Cyclic aliases: {}", alias);
                }

                let previous_alias = data.current_alias.clone();
                data.current_alias = alias.clone();
                data.inside_aliases.insert(alias.clone());
                let new_expr = PlanRewriter::expr_rewrite_alias(plan, data)?;
                data.inside_aliases.remove(alias);
                data.current_alias = previous_alias;

                Ok(ExpressionPlan::Alias(alias.clone(), Box::new(new_expr)))
            }

            ExpressionPlan::Wildcard | ExpressionPlan::Literal(_) => Ok(expr.clone()),
        }
    }
}
