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

use common_exception::Result;
use itertools::Itertools;

use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::ColumnSet;
use crate::ScalarExpr;

pub struct ExtractedPredicates {
    pub(crate) remaining: Option<ScalarExpr>,
    pub(crate) extracted: Option<Vec<Option<ScalarExpr>>>,
}

pub fn rewrite_predicates(s_expr: &SExpr) -> Result<Vec<ScalarExpr>> {
    let mut filter: Filter = s_expr.plan().clone().try_into()?;
    let join = s_expr.child(0)?;
    let mut new_predicates = Vec::new();
    let mut required_columns = Vec::new();
    for join_child in join.children().iter() {
        let rel_expr = RelExpr::with_s_expr(join_child);
        let prop = rel_expr.derive_relational_prop()?;
        required_columns.push(prop.used_columns.clone());
    }
    for predicate in filter.predicates.iter_mut() {
        match predicate {
            ScalarExpr::FunctionCall(func) if func.func_name == "or" => {
                let extracted_predicate = extract_or_predicate(&func.arguments, &required_columns)?;
                match (extracted_predicate.remaining, extracted_predicate.extracted) {
                    (Some(remaining), Some(extracted)) => {
                        new_predicates.push(remaining);
                        for sub_scalar in extracted.into_iter().flatten() {
                            new_predicates.push(sub_scalar);
                        }
                    }
                    (Some(remaining), None) => {
                        new_predicates.push(remaining);
                    }
                    (None, Some(extracted)) => {
                        for sub_scalar in extracted.into_iter().flatten() {
                            new_predicates.push(sub_scalar);
                        }
                    }
                    (None, None) => {
                        unreachable!()
                    }
                }
            }
            _ => new_predicates.push(predicate.clone()),
        }
    }
    // Deduplicate predicates here to prevent handled by `EliminateFilter` rule later,
    // which may cause infinite loop.
    new_predicates = new_predicates.into_iter().unique().collect();
    Ok(new_predicates)
}

// Only need to be executed once
fn extract_or_predicate(
    or_args: &[ScalarExpr],
    required_columns: &[ColumnSet],
) -> Result<ExtractedPredicates> {
    let mut flatten_or_args = flatten_ors(or_args);
    let mut remaining_scalars = Vec::new();
    let mut extracted_scalars = vec![Vec::new(); required_columns.len()];
    let mut has_extracted = false;
    for or_arg in flatten_or_args.iter_mut() {
        let mut remaining_scalar = Vec::new();
        let mut extracted_scalar = vec![Vec::new(); required_columns.len()];
        match or_arg {
            ScalarExpr::FunctionCall(func) if func.func_name == "and" => {
                let and_args = flatten_ands(&func.arguments);
                for and_arg in and_args.iter() {
                    match and_arg {
                        ScalarExpr::FunctionCall(func) if func.func_name == "or" => {
                            let extracted_predicate =
                                extract_or_predicate(&func.arguments, required_columns)?;
                            match (extracted_predicate.remaining, extracted_predicate.extracted) {
                                (Some(remaining), Some(extracted)) => {
                                    remaining_scalar.push(remaining);
                                    for (index, sub_scalar) in extracted.into_iter().enumerate() {
                                        if let Some(scalar) = sub_scalar {
                                            extracted_scalar[index].push(scalar);
                                        }
                                    }
                                }
                                (Some(remaining), None) => {
                                    remaining_scalar.push(remaining);
                                }
                                (None, Some(extracted)) => {
                                    for (index, sub_scalar) in extracted.into_iter().enumerate() {
                                        if let Some(scalar) = sub_scalar {
                                            extracted_scalar[index].push(scalar);
                                        }
                                    }
                                }
                                (None, None) => {
                                    unreachable!()
                                }
                            }
                        }
                        _ => {
                            let mut find = false;
                            let used_columns = and_arg.used_columns();
                            for (index, required) in required_columns.iter().enumerate() {
                                if used_columns.is_subset(required) {
                                    extracted_scalar[index].push(and_arg.clone());
                                    find = true;
                                    break;
                                }
                            }
                            if !find {
                                remaining_scalar.push(and_arg.clone());
                            }
                        }
                    }
                }
            }
            _ => {
                let mut find = false;
                let used_columns = or_arg.used_columns();
                for (index, required) in required_columns.iter().enumerate() {
                    if used_columns.is_subset(required) {
                        extracted_scalar[index].push(or_arg.clone());
                        find = true;
                        break;
                    }
                }
                if !find {
                    remaining_scalar.push(or_arg.clone());
                }
            }
        }
        let mut has_extracted_scalars = false;
        for (index, scalars) in extracted_scalar.into_iter().enumerate() {
            if !scalars.is_empty() {
                has_extracted_scalars = true;
                has_extracted = true;
                extracted_scalars[index].push(make_and_expr(&scalars));
            }
        }
        if !has_extracted_scalars {
            return Ok(ExtractedPredicates {
                remaining: Some(make_or_expr(&flatten_or_args)),
                extracted: None,
            });
        }
        if !remaining_scalar.is_empty() {
            remaining_scalars.push(make_and_expr(&remaining_scalar));
        }
    }

    let mut extracted = vec![None; required_columns.len()];
    if has_extracted {
        for (index, scalars) in extracted_scalars.into_iter().enumerate() {
            if !scalars.is_empty() {
                extracted[index] = Some(make_or_expr(&scalars));
            }
        }
        if !remaining_scalars.is_empty() {
            return Ok(ExtractedPredicates {
                remaining: Some(make_or_expr(&remaining_scalars)),
                extracted: Some(extracted),
            });
        } else {
            return Ok(ExtractedPredicates {
                remaining: None,
                extracted: Some(extracted),
            });
        }
    }

    Ok(ExtractedPredicates {
        remaining: Some(make_or_expr(&flatten_or_args)),
        extracted: None,
    })
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
fn flatten_ands(and_args: &[ScalarExpr]) -> Vec<ScalarExpr> {
    let mut flattened_ands = Vec::new();
    for and_arg in and_args.iter() {
        match and_arg {
            ScalarExpr::FunctionCall(func) if func.func_name == "and" => {
                flattened_ands.extend(flatten_ands(&func.arguments));
            }
            _ => flattened_ands.push(and_arg.clone()),
        }
    }
    flattened_ands
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
