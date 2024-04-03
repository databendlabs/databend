// Copyright 2021 Datafuse Labs
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

use databend_common_exception::Result;
use itertools::Itertools;

use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::ColumnSet;
use crate::ScalarExpr;

pub fn rewrite_predicates(s_expr: &SExpr) -> Result<Vec<ScalarExpr>> {
    let filter: Filter = s_expr.plan().clone().try_into()?;
    let join = s_expr.child(0)?;
    let mut new_predicates = Vec::new();
    let mut origin_predicates = filter.predicates.clone();
    for predicate in filter.predicates.iter() {
        match predicate {
            ScalarExpr::FunctionCall(func) if func.func_name == "or" => {
                for join_child in join.children() {
                    let rel_expr = RelExpr::with_s_expr(join_child);
                    let prop = rel_expr.derive_relational_prop()?;
                    if let Some(predicate) =
                        extract_or_predicate(&func.arguments, &prop.used_columns)?
                    {
                        new_predicates.push(predicate)
                    }
                }
            }
            _ => (),
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
    or_args: &[ScalarExpr],
    required_columns: &ColumnSet,
) -> Result<Option<ScalarExpr>> {
    let flatten_or_args = flatten_ors(or_args);
    let mut extracted_scalars = Vec::new();
    for or_arg in flatten_or_args.iter() {
        let mut sub_scalars = Vec::new();
        match or_arg {
            ScalarExpr::FunctionCall(func) if func.func_name == "and" => {
                let and_args = flatten_and(&func.arguments);
                for and_arg in and_args.iter() {
                    match and_arg {
                        ScalarExpr::FunctionCall(func) if func.func_name == "or" => {
                            if let Some(scalar) =
                                extract_or_predicate(&func.arguments, required_columns)?
                            {
                                sub_scalars.push(scalar);
                            }
                        }
                        _ => {
                            let used_columns = and_arg.used_columns();
                            if used_columns.is_subset(required_columns) {
                                sub_scalars.push(and_arg.clone());
                            }
                        }
                    }
                }
            }
            _ => {
                let used_columns = or_arg.used_columns();
                if used_columns.is_subset(required_columns) {
                    sub_scalars.push(or_arg.clone());
                }
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
fn flatten_ors(or_args: &[ScalarExpr]) -> Vec<ScalarExpr> {
    let mut flattened_ors = Vec::new();
    for or_arg in or_args.iter() {
        match or_arg {
            ScalarExpr::FunctionCall(func) if func.func_name == "or" => {
                flattened_ors.extend(flatten_ors(&func.arguments))
            }
            _ => flattened_ors.push(or_arg.clone()),
        }
    }
    flattened_ors
}

// Flatten nested ORs, such as `a=1 and b=1 and c=1`
// It'll be flatten to [a=1, b=1, c=1]
fn flatten_and(and_args: &[ScalarExpr]) -> Vec<ScalarExpr> {
    let mut flattened_and = Vec::new();
    for and_arg in and_args.iter() {
        match and_arg {
            ScalarExpr::FunctionCall(func) if func.func_name == "and" => {
                flattened_and.extend(flatten_and(&func.arguments));
            }
            _ => flattened_and.push(and_arg.clone()),
        }
    }
    flattened_and
}

// Merge predicates to AND scalar
fn make_and_expr(scalars: &[ScalarExpr]) -> ScalarExpr {
    scalars
        .iter()
        .cloned()
        .reduce(|lhs, rhs| {
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and".to_string(),
                params: vec![],
                arguments: vec![lhs, rhs],
            })
        })
        .unwrap()
}

// Merge predicates to OR scalar
fn make_or_expr(scalars: &[ScalarExpr]) -> ScalarExpr {
    scalars
        .iter()
        .cloned()
        .reduce(|lhs, rhs| {
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "or".to_string(),
                params: vec![],
                arguments: vec![lhs, rhs],
            })
        })
        .unwrap()
}
