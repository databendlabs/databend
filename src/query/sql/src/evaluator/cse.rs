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
                let output_num_columns = projections
                    .as_ref()
                    .map_or(input_num_columns + exprs.len(), BTreeSet::len);
                // find common expression
                let mut cse_counter = HashMap::new();
                for expr in exprs.iter() {
                    count_expressions(expr, &mut cse_counter);
                }

                let mut cse_candidates: Vec<&Expr> = cse_counter
                    .into_iter()
                    // Candidates are prepended to the Map. Only expressions whose
                    // dependencies already exist in its input can be evaluated there.
                    .filter(|(expr, count)| {
                        *count > 1 && expr.column_refs().keys().all(|id| *id < input_num_columns)
                    })
                    .map(|(expr, _)| expr)
                    .collect();

                // Make sure smaller expressions come first.
                cse_candidates.sort_by_key(|expr| expression_size(expr));
                prune_candidates(&exprs, &mut cse_candidates);

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
                        // Shift original intra-map references before inserting CSE
                        // references, which already use the new column numbering.
                        // Candidate keys contain input columns only, so still match.
                        remap_map_columns(&mut expr, input_num_columns, candidates_nums);
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
                input_num_columns = output_num_columns;
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

/// Count references in the candidate DAG rather than occurrences in the original trees.
/// A retained candidate evaluates its children once; an inlined candidate evaluates them
/// once per reference. Parents are larger than children, so a reverse size traversal
/// resolves both nested and cascading single-use candidates in one pass.
fn prune_candidates(exprs: &[Expr], candidates: &mut Vec<&Expr>) {
    if candidates.is_empty() {
        return;
    }
    let mut references: HashMap<&Expr, usize> = candidates.iter().map(|expr| (*expr, 0)).collect();
    for expr in exprs {
        count_candidate_references(expr, &mut references);
    }
    for candidate in candidates.iter().rev() {
        if references[candidate] != 0 {
            count_candidate_children(candidate, &mut references);
        }
    }
    candidates.retain(|candidate| references[candidate] > 1);
}

fn count_candidate_references(expr: &Expr, references: &mut HashMap<&Expr, usize>) {
    if let Some(references) = references.get_mut(expr) {
        *references += 1;
    } else {
        count_candidate_children(expr, references);
    }
}

fn count_candidate_children(expr: &Expr, references: &mut HashMap<&Expr, usize>) {
    match expr {
        Expr::FunctionCall(expr::FunctionCall { function, .. })
            if matches!(function.signature.name.as_str(), "if" | "is_not_error") => {}
        Expr::FunctionCall(expr::FunctionCall { args, .. })
        | Expr::LambdaFunctionCall(expr::LambdaFunctionCall { args, .. }) => {
            for arg in args {
                count_candidate_references(arg, references);
            }
        }
        Expr::Cast(Cast { expr, .. }) => count_candidate_references(expr, references),
        Expr::Constant(_) | Expr::ColumnRef(_) => {}
    }
}

/// Inserting temporaries before the original Map expressions shifts all columns
/// produced by those expressions. Unlike extraction, renumbering must also visit
/// protected branches. Lambda bodies have their own column scope; only args refer
/// to the enclosing block.
fn remap_map_columns(expr: &mut Expr, input_num_columns: usize, temporaries: usize) {
    match expr {
        Expr::ColumnRef(column) if column.id >= input_num_columns => column.id += temporaries,
        Expr::Cast(Cast { expr, .. }) => remap_map_columns(expr, input_num_columns, temporaries),
        Expr::FunctionCall(expr::FunctionCall { args, .. })
        | Expr::LambdaFunctionCall(expr::LambdaFunctionCall { args, .. }) => {
            for arg in args {
                remap_map_columns(arg, input_num_columns, temporaries);
            }
        }
        Expr::Constant(_) | Expr::ColumnRef(_) => {}
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
        Expr::FunctionCall(expr::FunctionCall { function, .. })
            if matches!(function.signature.name.as_str(), "if" | "is_not_error") => {}
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
    use std::borrow::Cow;

    use databend_common_expression::ConstantFolder;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::RawExpr;
    use databend_common_expression::Scalar;
    use databend_common_expression::type_check::check;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression_test_support::parse_raw_expr;

    use super::*;

    fn parse(text: &str) -> Expr {
        let raw = parse_raw_expr(
            text,
            &[("a", DataType::Number(NumberDataType::Int32))],
            &BUILTIN_FUNCTIONS,
        );
        ConstantFolder::fold(
            Cow::Owned(check(&raw, &BUILTIN_FUNCTIONS).unwrap()),
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
        )
        .0
        .into_owned()
    }

    fn check_cse(
        sql: &[&str],
        projections: Option<BTreeSet<usize>>,
        candidates: usize,
    ) -> Vec<Expr> {
        let original = BlockOperator::Map {
            exprs: sql.iter().map(|sql| parse(sql)).collect(),
            projections,
        };
        let optimized = apply_cse(vec![original.clone()], 1).pop().unwrap();
        let input = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![0, 1, 2, 3])]);
        let ctx = FunctionContext::default();
        let expected = original.execute(&ctx, input.clone()).unwrap();
        let actual = optimized.execute(&ctx, input).unwrap();
        assert_eq!(actual.num_rows(), expected.num_rows());
        assert_eq!(actual.num_columns(), expected.num_columns());
        for column in 0..expected.num_columns() {
            assert_eq!(
                actual.get_by_offset(column).value(),
                expected.get_by_offset(column).value()
            );
        }
        let BlockOperator::Map { exprs, .. } = optimized else {
            unreachable!()
        };
        assert_eq!(exprs.len(), sql.len() + candidates, "{exprs:?}");
        exprs
    }

    #[test]
    fn test_cse_prunes_nested_single_use_candidates() {
        let sql = "((a + 1) * 2) + 3";
        let exprs = check_cse(&[sql, sql], None, 1);
        assert_eq!(exprs[0], parse(sql));
        assert_eq!(exprs[1], exprs[2]);

        // A cast is also a candidate and must propagate its one effective use.
        let sql = "CAST((a + 1) * 2 AS STRING)";
        let exprs = check_cse(&[sql, sql], None, 1);
        assert_eq!(exprs[0], parse(sql));
    }

    #[test]
    fn test_cse_retains_multiple_reference_positions() {
        // One consumer, but two reference positions: the child still saves work.
        let sql = "(a + 1) * (a + 1)";
        let exprs = check_cse(&[sql, sql], None, 2);
        let Expr::FunctionCall(parent) = &exprs[1] else {
            unreachable!()
        };
        assert!(matches!(&parent.args[0], Expr::ColumnRef(_)));
        assert_eq!(parent.args[0], parent.args[1]);
    }

    #[test]
    fn test_cse_retains_shared_children() {
        let exprs = check_cse(
            &["(a + 1) * 2", "(a + 1) * 2", "(a + 1) * 3", "(a + 1) * 3"],
            None,
            3,
        );
        assert_eq!(exprs[0], parse("a + 1"));
        // A child used directly as an output also remains shared.
        check_cse(&["(a + 1) * 2", "(a + 1) * 2", "a + 1"], None, 2);
        // Prune the middle candidate but retain its child, which also has a
        // direct output reference.
        check_cse(
            &["((a + 1) * 2) + 3", "((a + 1) * 2) + 3", "a + 1"],
            None,
            2,
        );
    }

    #[test]
    fn test_cse_pruning_preserves_projections() {
        for projections in [
            BTreeSet::new(),
            BTreeSet::from([0]),
            BTreeSet::from([2]),
            BTreeSet::from([0, 1, 3]),
        ] {
            check_cse(
                &["(a + 1) * 2", "(a + 1) * 2", "a + 7"],
                Some(projections),
                1,
            );
        }
    }

    #[test]
    fn test_cse_pruning_preserves_error_boundaries() {
        // If a candidate also occurs under a protected function, do not count
        // or replace that occurrence. Otherwise it spuriously keeps the child.
        for protected in ["if(a > 0, a + 1, 0)", "is_not_error(a + 1)"] {
            let exprs = check_cse(&["(a + 1) * 2", "(a + 1) * 2", protected], None, 1);
            assert_eq!(exprs[3], parse(protected));
            // Even when the child is retained for two unprotected uses,
            // replacement must not cross into the protected function.
            let exprs = check_cse(&["a + 1", "a + 1", protected], None, 1);
            assert_eq!(exprs[3], parse(protected));
        }
        // The only occurrences of the throwing expression are protected.
        check_cse(&["if(a = 0, 0, 1 / a)", "if(a = 0, 0, 1 / a)"], None, 0);
        check_cse(&["is_not_error(1 / a)", "is_not_error(1 / a)"], None, 0);
    }

    #[test]
    fn test_cse_pruning_keeps_nondeterministic_expressions_inline() {
        let original = BlockOperator::Map {
            exprs: vec![parse("rand() + a"), parse("rand() + a")],
            projections: None,
        };
        let optimized = apply_cse(vec![original], 1).pop().unwrap();
        let BlockOperator::Map { exprs, projections } = optimized else {
            unreachable!()
        };
        assert_eq!(exprs.len(), 2);
        assert!(projections.is_none());
    }

    fn dependent_expr(text: &str, id: usize) -> Expr {
        let raw = parse_raw_expr(
            text,
            &[("a", DataType::Number(NumberDataType::Int64))],
            &BUILTIN_FUNCTIONS,
        );
        ConstantFolder::fold(
            Cow::Owned(check(&raw, &BUILTIN_FUNCTIONS).unwrap()),
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
        )
        .0
        .project_column_ref(|_| Ok(id))
        .unwrap()
    }

    fn check_operators(operators: Vec<BlockOperator>) -> Vec<BlockOperator> {
        let optimized = apply_cse(operators.clone(), 1);
        let ctx = FunctionContext::default();
        let input = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![0, 1, 2, 3])]);
        let execute = |operators: &[BlockOperator]| {
            operators
                .iter()
                .fold(input.clone(), |block, op| op.execute(&ctx, block).unwrap())
        };
        let expected = execute(&operators);
        let actual = execute(&optimized);
        assert_eq!(actual.num_rows(), expected.num_rows());
        assert_eq!(actual.num_columns(), expected.num_columns());
        for column in 0..expected.num_columns() {
            assert_eq!(
                actual.get_by_offset(column).value(),
                expected.get_by_offset(column).value(),
                "column {column}, optimized={optimized:?}"
            );
        }
        optimized
    }

    #[test]
    fn test_cse_preserves_dependent_map_offsets() {
        let t = parse("(a + 1) * 2");
        let p = parse("((a + 1) * 2) + 3");
        let original = BlockOperator::Map {
            exprs: vec![
                parse("a + 100"),
                t.clone(),
                p.clone(),
                p,
                dependent_expr("a", 2), // Original T: input width + expression index 1.
            ],
            projections: None,
        };
        for projections in [
            None,
            Some(BTreeSet::from([0, 2, 5])),
            Some(BTreeSet::from([5])),
        ] {
            let mut original = original.clone();
            let BlockOperator::Map { projections: p, .. } = &mut original else {
                unreachable!()
            };
            *p = projections;
            let optimized = check_operators(vec![original]);
            let BlockOperator::Map { exprs, .. } = &optimized[0] else {
                unreachable!()
            };
            assert_eq!(exprs.len(), 7); // T and P retained; C pruned.
            assert!(matches!(&exprs[6], Expr::ColumnRef(column) if column.id == 4));
        }
    }

    #[test]
    fn test_cse_keeps_dependent_candidates_after_their_producers() {
        for shared_inputs in [false, true] {
            let mut exprs = vec![parse("a + 100")];
            if shared_inputs {
                exprs.extend([parse("(a + 1) * 2"), parse("(a + 1) * 2")]);
            }
            // These repeated expressions require column 1, which does not exist
            // at the start of the Map. They cannot become prepended candidates.
            exprs.extend([
                dependent_expr("(a + 7) * 3", 1),
                dependent_expr("(a + 7) * 3", 1),
                dependent_expr("CAST(a AS STRING)", 1),
                dependent_expr("if(a > 0, a + 1, 0)", 1),
                dependent_expr("is_not_error(1 / a)", 1),
            ]);
            let original_len = exprs.len();
            let optimized = check_operators(vec![BlockOperator::Map {
                exprs,
                projections: None,
            }]);
            let BlockOperator::Map { exprs, .. } = &optimized[0] else {
                unreachable!()
            };
            assert_eq!(exprs.len(), original_len + usize::from(shared_inputs));
        }
    }

    #[test]
    fn test_cse_remaps_lambda_args_not_local_columns() {
        let body = dependent_expr("a + 1", 1).as_remote_expr();
        let mut lambda = Expr::LambdaFunctionCall(expr::LambdaFunctionCall {
            span: None,
            name: "array_transform".to_string(),
            args: vec![dependent_expr("[a]", 1)],
            lambda_expr: Box::new(body.clone()),
            lambda_display: "x -> x + 1".to_string(),
            return_type: DataType::Array(Box::new(DataType::Number(NumberDataType::Int64))),
        });
        remap_map_columns(&mut lambda, 1, 2);
        let Expr::LambdaFunctionCall(lambda) = lambda else {
            unreachable!()
        };
        assert_eq!(lambda.args[0], dependent_expr("[a]", 3));
        assert_eq!(*lambda.lambda_expr, body);
    }

    #[test]
    fn test_cse_tracks_map_output_width_between_operators() {
        for first in [vec![parse("a + 1"), parse("a + 1")], vec![
            parse("a + 1"),
            parse("a + 2"),
        ]] {
            for projections in [None, Some(BTreeSet::from([2]))] {
                let id = if projections.is_some() { 0 } else { 2 };
                let optimized = check_operators(vec![
                    BlockOperator::Map {
                        exprs: first.clone(),
                        projections,
                    },
                    BlockOperator::Map {
                        exprs: vec![dependent_expr("a * 3", id), dependent_expr("a * 3", id)],
                        projections: Some(BTreeSet::from([id])),
                    },
                    BlockOperator::Project {
                        projection: vec![0, 0],
                    },
                    BlockOperator::Map {
                        exprs: vec![dependent_expr("a + 4", 1), dependent_expr("a + 4", 1)],
                        projections: None,
                    },
                ]);
                for index in [1, 3] {
                    let BlockOperator::Map { exprs, .. } = &optimized[index] else {
                        unreachable!()
                    };
                    assert_eq!(exprs.len(), 3);
                }
            }
        }
    }

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
