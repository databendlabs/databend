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

use std::collections::BTreeSet;
use std::collections::HashMap;

use databend_common_expression::Cast;
use databend_common_expression::Expr;
use databend_common_expression::expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use log::debug;

use super::BlockOperator;

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

                let mut cse_candidates: Vec<&Expr> = cse_counter
                    .into_iter()
                    .filter(|(_, count)| *count > 1)
                    .map(|(expr, _)| expr)
                    .collect();

                // Make sure smaller expressions come first.
                cse_candidates.sort_by_key(|expr| expression_size(expr));

                let mut temp_var_counter = input_num_columns;
                if !cse_candidates.is_empty() {
                    let mut new_exprs = Vec::new();
                    let mut cse_replacements = HashMap::new();

                    let candidates_nums = cse_candidates.len();
                    for cse_candidate in cse_candidates.into_iter().cloned() {
                        let temp_var = format!("__temp_cse_{}", temp_var_counter);
                        let temp_expr: Expr<_> = expr::ColumnRef {
                            span: None,
                            id: temp_var_counter,
                            data_type: cse_candidate.data_type().clone(),
                            display_name: temp_var.clone(),
                        }
                        .into();

                        let mut expr_cloned = cse_candidate.clone();
                        perform_cse_replacement(&mut expr_cloned, &cse_replacements);

                        debug!("cse_candidate: {expr_cloned}, temp_expr: {temp_expr}");

                        new_exprs.push(expr_cloned);
                        cse_replacements.insert(cse_candidate, temp_expr);
                        temp_var_counter += 1;
                    }

                    let projections =
                        projections.unwrap_or((0..input_num_columns + exprs.len()).collect());

                    // Regenerate the projections based on the replacements
                    // 1. Initialize the new_projections with the original projections with unchanged indexes
                    let mut new_projections = projections
                        .iter()
                        .filter(|idx| **idx < input_num_columns)
                        .copied()
                        .collect::<BTreeSet<_>>();

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
fn count_expressions<'a>(expr: &'a Expr, counter: &mut HashMap<&'a Expr, usize>) {
    if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
        return;
    }
    match expr {
        Expr::FunctionCall(expr::FunctionCall { function, .. })
            if function.signature.name == "if" => {}
        Expr::FunctionCall(expr::FunctionCall { function, .. })
            if function.signature.name == "is_not_error" => {}
        Expr::FunctionCall(expr::FunctionCall { args, .. })
        | Expr::LambdaFunctionCall(expr::LambdaFunctionCall { args, .. }) => {
            let entry = counter.entry(expr).or_insert(0);
            *entry += 1;

            for arg in args {
                count_expressions(arg, counter);
            }
        }
        Expr::Cast(Cast {
            expr: inner_expr, ..
        }) => {
            let entry = counter.entry(expr).or_insert(0);
            *entry += 1;

            count_expressions(inner_expr, counter);
        }
        // ignore constant and column ref
        Expr::Constant(_) | Expr::ColumnRef(_) => {}
    }
}

/// Return the number of nodes in an expression tree. A child expression is always smaller than
/// its parent, so sorting by this value ensures that nested CSE candidates are materialized first.
fn expression_size(expr: &Expr) -> usize {
    match expr {
        Expr::Cast(expr::Cast {
            expr: inner_expr, ..
        }) => 1 + expression_size(inner_expr),
        Expr::FunctionCall(expr::FunctionCall { args, .. })
        | Expr::LambdaFunctionCall(expr::LambdaFunctionCall { args, .. }) => {
            1 + args.iter().map(expression_size).sum::<usize>()
        }
        Expr::Constant(_) | Expr::ColumnRef(_) => 1,
    }
}

// `perform_cse_replacement` performs common subexpression elimination (CSE) on an expression tree
// by replacing subexpressions that appear multiple times with a single shared expression.
fn perform_cse_replacement(expr: &mut Expr, cse_replacements: &HashMap<Expr, Expr>) {
    // If expr itself is a key in cse_replacements, return the replaced expression.
    if let Some(replacement) = cse_replacements.get(expr) {
        *expr = replacement.clone();
        return;
    }

    match expr {
        Expr::Cast(expr::Cast {
            expr: inner_expr, ..
        }) => {
            perform_cse_replacement(inner_expr.as_mut(), cse_replacements);
        }
        Expr::FunctionCall(expr::FunctionCall { args, .. })
        | Expr::LambdaFunctionCall(expr::LambdaFunctionCall { args, .. }) => {
            for arg in args.iter_mut() {
                perform_cse_replacement(arg, cse_replacements);
            }
        }
        // ignore constant and column ref
        Expr::Constant(_) | Expr::ColumnRef(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::RawExpr;
    use databend_common_expression::Scalar;
    use databend_common_expression::type_check::check;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    #[test]
    fn test_cse_distinguishes_expressions_with_same_display() {
        let data_type = DataType::Number(NumberDataType::Int32);
        let plus = |id| RawExpr::FunctionCall {
            span: None,
            name: "plus".to_string(),
            params: vec![],
            args: vec![
                RawExpr::ColumnRef {
                    span: None,
                    id,
                    data_type: data_type.clone(),
                    display_name: "a".to_string(),
                },
                RawExpr::Constant {
                    span: None,
                    scalar: Scalar::Number(NumberScalar::UInt64(1)),
                    data_type: None,
                },
            ],
        };

        // The expressions render identically, but refer to different input columns.
        let exprs = [plus(0), plus(0), plus(1), plus(1)]
            .iter()
            .map(|expr| check(expr, &BUILTIN_FUNCTIONS).unwrap())
            .collect();
        let operators = apply_cse(
            vec![BlockOperator::Map {
                exprs,
                projections: None,
            }],
            2,
        );

        let BlockOperator::Map { exprs, .. } = &operators[0] else {
            unreachable!()
        };
        assert_eq!(exprs.len(), 6);

        let mut source_ids = exprs[..2]
            .iter()
            .map(|expr| match expr {
                Expr::FunctionCall(call) => match &call.args[0] {
                    Expr::ColumnRef(column) => column.id,
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        source_ids.sort_unstable();
        assert_eq!(source_ids, vec![0, 1]);

        let replacement_ids = exprs[2..]
            .iter()
            .map(|expr| match expr {
                Expr::ColumnRef(column) => column.id,
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(replacement_ids[0], replacement_ids[1]);
        assert_eq!(replacement_ids[2], replacement_ids[3]);
        assert_ne!(replacement_ids[0], replacement_ids[2]);
    }
}
