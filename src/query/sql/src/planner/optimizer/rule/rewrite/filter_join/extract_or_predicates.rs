// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_exception::Result;
use itertools::Itertools;

use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::AndExpr;
use crate::plans::Filter;
use crate::plans::OrExpr;
use crate::ColumnSet;
use crate::ScalarExpr;

pub fn rewrite_predicates(s_expr: &SExpr) -> Result<Vec<ScalarExpr>> {
    let filter: Filter = s_expr.plan().clone().try_into()?;
    let join = s_expr.child(0)?;
    let mut new_predicates = Vec::new();
    let mut origin_predicates = filter.predicates.clone();
    for predicate in filter.predicates.iter() {
        if let ScalarExpr::OrExpr(or_expr) = predicate {
            for join_child in join.children().iter() {
                let rel_expr = RelExpr::with_s_expr(join_child);
                let used_columns = rel_expr.derive_relational_prop()?.used_columns;
                if let Some(predicate) = extract_or_predicate(or_expr, &used_columns)? {
                    new_predicates.push(predicate)
                }
            }
        }
    }
    origin_predicates.extend(new_predicates);
    // Deduplicate predicates here to prevent handled by `EliminateFilter` rule later,
    // which may cause infinite loop.
    origin_predicates = origin_predicates.into_iter().unique().collect();
    Ok(origin_predicates)
}

// Only need to be executed once
fn extract_or_predicate(
    or_expr: &OrExpr,
    required_columns: &ColumnSet,
) -> Result<Option<ScalarExpr>> {
    let or_args = flatten_ors(or_expr.clone());
    let mut extracted_scalars = Vec::new();
    for or_arg in or_args.iter() {
        let mut sub_scalars = Vec::new();
        if let ScalarExpr::AndExpr(and_expr) = or_arg {
            let and_args = flatten_ands(and_expr.clone());
            for and_arg in and_args.iter() {
                if let ScalarExpr::OrExpr(or_expr) = and_arg {
                    if let Some(scalar) = extract_or_predicate(or_expr, required_columns)? {
                        sub_scalars.push(scalar);
                    }
                } else {
                    let used_columns = and_arg.used_columns();
                    if used_columns.is_subset(required_columns) {
                        sub_scalars.push(and_arg.clone());
                    }
                }
            }
        } else {
            let used_columns = or_arg.used_columns();
            if used_columns.is_subset(required_columns) {
                sub_scalars.push(or_arg.clone());
            }
        }
        if sub_scalars.is_empty() {
            return Ok(None);
        }

        extracted_scalars.push(make_and_expr(&sub_scalars));
    }

    if !extracted_scalars.is_empty() {
        return Ok(Some(make_or_expr(&extracted_scalars)));
    }

    Ok(None)
}

// Flatten nested ORs, such as `a=1 or b=1 or c=1`
// It'll be flatten to [a=1, b=1, c=1]
fn flatten_ors(or_expr: OrExpr) -> Vec<ScalarExpr> {
    let mut flattened_ors = Vec::new();
    let or_args = vec![*or_expr.left, *or_expr.right];
    for or_arg in or_args.iter() {
        match or_arg {
            ScalarExpr::OrExpr(or_expr) => flattened_ors.extend(flatten_ors(or_expr.clone())),
            _ => flattened_ors.push(or_arg.clone()),
        }
    }
    flattened_ors
}

// Flatten nested ORs, such as `a=1 and b=1 and c=1`
// It'll be flatten to [a=1, b=1, c=1]
fn flatten_ands(and_expr: AndExpr) -> Vec<ScalarExpr> {
    let mut flattened_ands = Vec::new();
    let and_args = vec![*and_expr.left, *and_expr.right];
    for and_arg in and_args.iter() {
        match and_arg {
            ScalarExpr::AndExpr(and_expr) => flattened_ands.extend(flatten_ands(and_expr.clone())),
            _ => flattened_ands.push(and_arg.clone()),
        }
    }
    flattened_ands
}

// Merge predicates to AND scalar
fn make_and_expr(scalars: &[ScalarExpr]) -> ScalarExpr {
    if scalars.len() == 1 {
        return scalars[0].clone();
    }
    ScalarExpr::AndExpr(AndExpr {
        left: Box::new(scalars[0].clone()),
        right: Box::new(make_and_expr(&scalars[1..])),
        return_type: Box::new(scalars[0].data_type()),
    })
}

// Merge predicates to OR scalar
fn make_or_expr(scalars: &[ScalarExpr]) -> ScalarExpr {
    if scalars.len() == 1 {
        return scalars[0].clone();
    }
    ScalarExpr::OrExpr(OrExpr {
        left: Box::new(scalars[0].clone()),
        right: Box::new(make_or_expr(&scalars[1..])),
        return_type: Box::new(scalars[0].data_type()),
    })
}
