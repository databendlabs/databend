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

use std::collections::HashMap;

use databend_common_expression::Expr;
use log::debug;

use super::BlockOperator;
use crate::optimizer::ColumnSet;

/// Eliminate common expression in `Map` operator
pub fn apply_cse(
    operators: Vec<BlockOperator>,
    mut input_num_columns: usize,
) -> Vec<BlockOperator> {
    let mut results = Vec::with_capacity(operators.len());

    for op in operators {
        match op {
            BlockOperator::Map { exprs, projections } => {
                // find common expression
                let mut cse_counter = HashMap::new();
                for expr in exprs.iter() {
                    count_expressions(expr, &mut cse_counter);
                }

                let mut cse_candidates: Vec<Expr> = cse_counter
                    .iter()
                    .filter(|(_, count)| **count > 1)
                    .map(|(expr, _)| expr.clone())
                    .collect();

                // Make sure the smaller expr goes firstly
                cse_candidates.sort_by_key(|a| a.sql_display().len());

                let mut temp_var_counter = input_num_columns;
                if !cse_candidates.is_empty() {
                    let mut new_exprs = Vec::new();
                    let mut cse_replacements = HashMap::new();

                    let candidates_nums = cse_candidates.len();
                    for cse_candidate in &cse_candidates {
                        let temp_var = format!("__temp_cse_{}", temp_var_counter);
                        let temp_expr = Expr::ColumnRef {
                            span: None,
                            id: temp_var_counter,
                            data_type: cse_candidate.data_type().clone(),
                            display_name: temp_var.clone(),
                        };

                        let mut expr_cloned = cse_candidate.clone();
                        perform_cse_replacement(&mut expr_cloned, &cse_replacements);

                        debug!(
                            "cse_candidate: {}, temp_expr: {}",
                            expr_cloned.sql_display(),
                            temp_expr.sql_display()
                        );

                        new_exprs.push(expr_cloned);
                        cse_replacements.insert(cse_candidate.sql_display(), temp_expr);
                        temp_var_counter += 1;
                    }

                    let projections = projections
                        .unwrap_or((0..input_num_columns + exprs.len()).collect::<ColumnSet>());

                    // Regenerate the projections based on the replacements
                    // 1. Initialize the new_projections with the original projections with unchanged indexes
                    let mut new_projections = projections
                        .iter()
                        .filter(|idx| **idx < input_num_columns)
                        .copied()
                        .collect::<ColumnSet>();

                    for mut expr in exprs {
                        perform_cse_replacement(&mut expr, &cse_replacements);
                        new_exprs.push(expr);

                        // 2. Increment projection index because the position is occupied by the cse
                        if projections.contains(&(temp_var_counter - candidates_nums)) {
                            new_projections.insert(temp_var_counter);
                        }
                        temp_var_counter += 1;
                    }

                    results.push(BlockOperator::Map {
                        exprs: new_exprs,
                        projections: Some(new_projections),
                    });
                } else {
                    results.push(BlockOperator::Map { exprs, projections });
                }
            }
            BlockOperator::Project { projection } => {
                input_num_columns = projection.len();
                results.push(BlockOperator::Project { projection });
            }
        }
    }

    results
}

/// `count_expressions` recursively counts the occurrences of expressions in an expression tree
/// and stores the count in a HashMap.
fn count_expressions(expr: &Expr, counter: &mut HashMap<Expr, usize>) {
    match expr {
        Expr::FunctionCall { function, .. } if function.signature.name == "if" => {}
        Expr::FunctionCall { function, .. } if function.signature.name == "is_not_error" => {}
        Expr::FunctionCall { args, .. } | Expr::LambdaFunctionCall { args, .. } => {
            let entry = counter.entry(expr.clone()).or_insert(0);
            *entry += 1;

            for arg in args {
                count_expressions(arg, counter);
            }
        }
        Expr::Cast {
            expr: inner_expr, ..
        } => {
            let entry = counter.entry(expr.clone()).or_insert(0);
            *entry += 1;

            count_expressions(inner_expr, counter);
        }
        // ignore constant and column ref
        Expr::Constant { .. } | Expr::ColumnRef { .. } => {}
    }
}

// `perform_cse_replacement` performs common subexpression elimination (CSE) on an expression tree
// by replacing subexpressions that appear multiple times with a single shared expression.
fn perform_cse_replacement(expr: &mut Expr, cse_replacements: &HashMap<String, Expr>) {
    // If expr itself is a key in cse_replacements, return the replaced expression.
    if let Some(replacement) = cse_replacements.get(&expr.sql_display()) {
        *expr = replacement.clone();
        return;
    }

    match expr {
        Expr::Cast {
            expr: inner_expr, ..
        } => {
            perform_cse_replacement(inner_expr.as_mut(), cse_replacements);
        }
        Expr::FunctionCall { args, .. } | Expr::LambdaFunctionCall { args, .. } => {
            for arg in args.iter_mut() {
                perform_cse_replacement(arg, cse_replacements);
            }
        }
        // ignore constant and column ref
        Expr::Constant { .. } | Expr::ColumnRef { .. } => {}
    }
}
