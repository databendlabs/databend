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

use std::io::Write;

use databend_common_expression::Column;
use databend_common_expression::Domain;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::TestContext;
use super::run_ast;
use super::run_ast_with_context;

#[test]
fn test_control() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("control.txt").unwrap();

    test_if(file);
    test_is_not_null(file);
    test_is_not_error(file);
}

#[test]
fn test_if_accepts_non_strict_decimal_columns() {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_expression::Expr;
    use databend_common_expression::Scalar;
    use databend_common_expression::type_check::check_function;
    use databend_common_functions::BUILTIN_FUNCTIONS;

    // Decimal(10, 2) carried by a Decimal128 column is a non-strict
    // representation: the typed `if` selection cannot downcast it and must fall
    // back to the generic selection, which normalizes it to Decimal64.
    let size = DecimalSize::new(10, 2).unwrap();
    for nullable in [false, true] {
        for legacy_then in [false, true] {
            let legacy =
                Column::Decimal(DecimalColumn::Decimal128(vec![123_i128, 456].into(), size));
            let column = if nullable {
                NullableColumn::new_column(legacy, vec![true, false].into())
            } else {
                legacy
            };
            let data_type = column.data_type();
            let block = DataBlock::new_from_columns(vec![
                BooleanType::from_data(vec![legacy_then, !legacy_then]),
                column,
            ]);
            let condition = Expr::ColumnRef(ColumnRef {
                span: None,
                id: 0,
                data_type: DataType::Boolean,
                display_name: "condition".into(),
            });
            let value = Expr::ColumnRef(ColumnRef {
                span: None,
                id: 1,
                data_type: data_type.clone(),
                display_name: "value".into(),
            });
            let zero = Expr::constant(Scalar::default_value(&data_type), Some(data_type));
            let args = if legacy_then {
                vec![condition, value, zero]
            } else {
                vec![condition, zero, value]
            };
            let expr = check_function(None, "if", &[], &args, &BUILTIN_FUNCTIONS).unwrap();
            let ctx = FunctionContext::default();
            let result = Evaluator::new(&block, &ctx, &BUILTIN_FUNCTIONS)
                .run(&expr)
                .unwrap();
            let expected_first = Scalar::Decimal(DecimalScalar::Decimal64(123, size));
            let expected_second = if nullable {
                Scalar::Null
            } else {
                Scalar::Decimal(DecimalScalar::Decimal64(0, size))
            };
            assert_eq!(result.index(0).unwrap(), expected_first.as_ref());
            assert_eq!(result.index(1).unwrap(), expected_second.as_ref());
        }
    }
}

#[test]
fn test_if_with_opaque_result() {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_expression::Expr;
    use databend_common_expression::Scalar;
    use databend_common_expression::type_check::check_function;
    use databend_common_functions::BUILTIN_FUNCTIONS;

    // `Scalar::default_value` has no `Opaque` value: a branch that no enabled
    // row reaches, and the inner value of a NULL branch, must not need one.
    let column_ref = |id: usize, data_type: &DataType| {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id,
            data_type: data_type.clone(),
            display_name: format!("c{id}"),
        })
    };
    let data_type = DataType::Opaque(1);
    let nullable_type = data_type.wrap_nullable();
    let then_column = Column::random(&data_type, 4, None);
    let else_column = Column::random(&data_type, 4, None);
    let nullable_then =
        NullableColumn::new_column(then_column.clone(), vec![true, true, false, true].into());
    let null = Expr::constant(Scalar::Null, Some(nullable_type.clone()));

    for cond in [
        vec![true, true, true, true],
        vec![false, false, false, false],
        vec![true, false, true, false],
    ] {
        let block = DataBlock::new_from_columns(vec![
            BooleanType::from_data(cond.clone()),
            then_column.clone(),
            else_column.clone(),
            nullable_then.clone(),
        ]);
        let cases = [
            (
                vec![
                    column_ref(0, &DataType::Boolean),
                    column_ref(1, &data_type),
                    column_ref(2, &data_type),
                ],
                then_column.clone(),
                else_column.clone(),
            ),
            (
                vec![
                    column_ref(0, &DataType::Boolean),
                    column_ref(3, &nullable_type),
                    null.clone(),
                ],
                nullable_then.clone(),
                Column::Null { len: 4 },
            ),
        ];
        for (args, then_expected, else_expected) in cases {
            let expr = check_function(None, "if", &[], &args, &BUILTIN_FUNCTIONS).unwrap();
            let ctx = FunctionContext::default();
            let result = Evaluator::new(&block, &ctx, &BUILTIN_FUNCTIONS)
                .run(&expr)
                .unwrap();
            for (row, take_then) in cond.iter().enumerate() {
                let expected = if *take_then {
                    then_expected.index(row)
                } else {
                    else_expected.index(row)
                };
                assert_eq!(result.index(row), expected, "{cond:?} row {row}");
            }
        }
    }
}

fn test_if(file: &mut impl Write) {
    run_ast(file, "if(false, 1, false, 2, NULL)", &[]);
    run_ast(file, "if(true, 1, NULL, 2, NULL)", &[]);
    run_ast(file, "if(false, 1, true, 2, NULL)", &[]);
    run_ast(file, "if(true, 1, true, 2, NULL)", &[]);
    run_ast(file, "if(true, 1, true, NULL, 2)", &[]);
    run_ast(file, "if(true, 1, NULL)", &[]);
    run_ast(file, "if(false, 1, NULL)", &[]);
    run_ast(file, "if(true, 1, 1 / 0)", &[]);
    run_ast(file, "if(false, 1 / 0, 1)", &[]);
    run_ast(file, "if(false, 1, 1 / 0)", &[]);
    run_ast(file, "if(cond_a, expr_true, expr_else)", &[
        (
            "cond_a",
            Column::Boolean(vec![true, true, false, false].into()),
        ),
        ("expr_true", Int64Type::from_data(vec![1i64, 2, 3, 4])),
        (
            "expr_else",
            Int64Type::from_data_with_validity(vec![5i64, 6, 7, 8], vec![true, false, true, false]),
        ),
    ]);
    run_ast(file, "if(cond_a, expr_true, expr_else)", &[
        (
            "cond_a",
            BooleanType::from_data(vec![false, false, true, true]),
        ),
        ("expr_true", Int64Type::from_data(vec![1i64, 2, 3, 4])),
        (
            "expr_else",
            Int64Type::from_data_with_validity(vec![5i64, 6, 7, 8], vec![true, true, false, false]),
        ),
    ]);
    run_ast(file, "if(cond_a, expr_true, expr_else)", &[
        (
            "cond_a",
            BooleanType::from_data(vec![true, false, true, false]),
        ),
        ("expr_true", Int64Type::from_data(vec![1i64, 2, 3, 4])),
        ("expr_else", Int64Type::from_data(vec![5i64, 6, 7, 8])),
    ]);
    run_ast(file, "if(cond_a, expr_true, expr_else)", &[
        (
            "cond_a",
            BooleanType::from_data(vec![true, false, true, false]),
        ),
        (
            "expr_true",
            Decimal64Type::from_data_with_size(
                [100i64, 200, 300, 400],
                Some(DecimalSize::new_unchecked(15, 2)),
            ),
        ),
        (
            "expr_else",
            Decimal64Type::from_data_with_size(
                [500i64, 600, 700, 800],
                Some(DecimalSize::new_unchecked(15, 2)),
            ),
        ),
    ]);
    run_ast(file, "if(cond_a, expr_a, cond_b, expr_b, expr_else)", &[
        (
            "cond_a",
            Column::Boolean(vec![true, true, false, false].into()),
        ),
        ("expr_a", Int64Type::from_data(vec![1i64, 2, 3, 4])),
        (
            "cond_b",
            BooleanType::from_data_with_validity(vec![true, true, true, true], vec![
                false, true, false, true,
            ]),
        ),
        ("expr_b", Int64Type::from_data(vec![5i64, 6, 7, 8])),
        (
            "expr_else",
            Int64Type::from_data_with_validity(vec![9i64, 10, 11, 12], vec![
                true, true, false, false,
            ]),
        ),
    ]);
    run_ast(file, "if(cond_a, expr_a, cond_b, expr_b, expr_else)", &[
        (
            "cond_a",
            BooleanType::from_data(vec![true, true, false, false]),
        ),
        ("expr_a", Int64Type::from_data(vec![1i64, 2, 3, 4])),
        (
            "cond_b",
            BooleanType::from_data(vec![true, false, true, false]),
        ),
        ("expr_b", Int64Type::from_data(vec![5i64, 6, 7, 8])),
        ("expr_else", Int64Type::from_data(vec![9i64, 10, 11, 12])),
    ]);
    run_ast(file, "if(cond_a, 1 / expr_a, expr_else)", &[
        (
            "cond_a",
            BooleanType::from_data(vec![true, true, false, false]),
        ),
        (
            "expr_a",
            Int64Type::from_data_with_validity(vec![1i64, 0, 0, 4], vec![true, false, true, true]),
        ),
        ("expr_else", Int64Type::from_data(vec![9i64, 10, 11, 12])),
    ]);
    run_ast(file, "if(cond_a, 1 / expr_a, expr_else)", &[
        (
            "cond_a",
            BooleanType::from_data(vec![true, true, true, false]),
        ),
        ("expr_a", Int64Type::from_data(vec![1i64, 2, 0, 4])),
        ("expr_else", Int64Type::from_data(vec![9i64, 10, 11, 12])),
    ]);

    // A column condition that is true (or false, or NULL) on every row: the
    // rows reach a single branch, and the division by zero in the other
    // branch must not be reported.
    run_ast(file, "if(cond_a, expr_a, 1 / expr_zero)", &[
        (
            "cond_a",
            BooleanType::from_data(vec![true, true, true, true]),
        ),
        ("expr_a", Int64Type::from_data(vec![1i64, 2, 3, 4])),
        ("expr_zero", Int64Type::from_data(vec![0i64, 0, 0, 0])),
    ]);
    run_ast(file, "if(cond_a, 1 / expr_zero, expr_a)", &[
        (
            "cond_a",
            BooleanType::from_data_with_validity(vec![false, true, false, true], vec![
                true, false, true, false,
            ]),
        ),
        ("expr_a", Int64Type::from_data(vec![1i64, 2, 3, 4])),
        ("expr_zero", Int64Type::from_data(vec![0i64, 0, 0, 0])),
    ]);
    // The first condition takes every row: the errors in later conditions and
    // branches must not be reported.
    run_ast(
        file,
        "if(cond_a, expr_a, 1 / expr_zero = 1, 1 / expr_zero, expr_a)",
        &[
            (
                "cond_a",
                BooleanType::from_data(vec![true, true, true, true]),
            ),
            ("expr_a", Int64Type::from_data(vec![1i64, 2, 3, 4])),
            ("expr_zero", Int64Type::from_data(vec![0i64, 0, 0, 0])),
        ],
    );
    // The inner `if` runs only for the rows where `cond_a` is true, and there
    // `cond_b` is true as well: its else branch is reached by no enabled row.
    run_ast(
        file,
        "if(cond_a, if(cond_b, expr_a, 1 / expr_zero), expr_else)",
        &[
            (
                "cond_a",
                BooleanType::from_data(vec![true, false, true, false]),
            ),
            (
                "cond_b",
                BooleanType::from_data(vec![true, false, true, true]),
            ),
            ("expr_a", Int64Type::from_data(vec![1i64, 2, 3, 4])),
            ("expr_zero", Int64Type::from_data(vec![0i64, 0, 0, 0])),
            ("expr_else", Int64Type::from_data(vec![9i64, 10, 11, 12])),
        ],
    );
    // String, Boolean and nullable results with mixed conditions.
    run_ast(file, "if(cond_a, expr_true, expr_else)", &[
        (
            "cond_a",
            BooleanType::from_data_with_validity(vec![true, false, true, true], vec![
                true, true, true, false,
            ]),
        ),
        ("expr_true", StringType::from_data(vec!["a", "b", "c", "d"])),
        ("expr_else", StringType::from_data(vec!["w", "x", "y", "z"])),
    ]);
    run_ast(file, "if(cond_a, expr_true, NULL)", &[
        (
            "cond_a",
            BooleanType::from_data(vec![true, false, true, false]),
        ),
        ("expr_true", StringType::from_data(vec!["a", "b", "c", "d"])),
    ]);
    run_ast(file, "if(cond_a, expr_true, expr_else)", &[
        (
            "cond_a",
            BooleanType::from_data(vec![true, false, true, false]),
        ),
        (
            "expr_true",
            BooleanType::from_data(vec![true, true, false, false]),
        ),
        (
            "expr_else",
            BooleanType::from_data_with_validity(vec![true, false, true, false], vec![
                true, true, false, true,
            ]),
        ),
    ]);
}

fn test_is_not_null(file: &mut impl Write) {
    run_ast(file, "is_not_null(1)", &[]);
    run_ast(file, "is_not_null(4096)", &[]);
    run_ast(file, "is_not_null(true)", &[]);
    run_ast(file, "is_not_null(false)", &[]);
    run_ast(file, "is_not_null('string')", &[]);
    run_ast(file, "is_not_null(NULL)", &[]);
    run_ast(file, "is_not_null(null_col)", &[(
        "null_col",
        Column::Null { len: 13 },
    )]);
    run_ast(file, "is_not_null(int64_col)", &[(
        "int64_col",
        Int64Type::from_data(vec![5i64, 6, 7, 8]),
    )]);
    run_ast(file, "is_not_null(nullable_col)", &[(
        "nullable_col",
        Int64Type::from_data_with_validity(vec![9i64, 10, 11, 12], vec![true, true, false, false]),
    )]);
}

fn test_is_not_error(file: &mut impl Write) {
    run_ast(file, "is_not_error(1 / denom)", &[(
        "denom",
        Int64Type::from_data(vec![2i64, 0, -1, 4]),
    )]);
    // The throwing expression is nested inside a comparison: the error must
    // bubble up through the non-boundary `gt` call to reach the boundary.
    run_ast(file, "is_not_error((1 / denom) > 0)", &[(
        "denom",
        Int64Type::from_data(vec![2i64, 0, -1, 4]),
    )]);
    // A scalar subexpression that errors invalidates every row: the constant
    // cast is evaluated in a one-row block, and its error bitmap must
    // broadcast to all rows when merged with the column error set.
    run_ast(file, "is_not_error((1 / denom) + cast('x' as int64))", &[(
        "denom",
        Int64Type::from_data(vec![2i64, 0, -1, 4]),
    )]);
    // A scalar cast inside a partially selected `if` branch invalidates
    // exactly the rows the branch runs on: the scalar error must be expanded
    // over the branch validity instead of being pinned to (or dropped at)
    // row 0.
    run_ast(
        file,
        "is_not_error(if(denom = 4, cast('x' as int64), 1))",
        &[("denom", Int64Type::from_data(vec![2i64, 0, -1, 4]))],
    );
    run_ast(
        file,
        "is_not_error(if(denom > 0, cast('x' as int64), 1))",
        &[("denom", Int64Type::from_data(vec![2i64, 0, -1, 4]))],
    );
    // No row selects the branch, so the failing scalar cast never runs and
    // its error must not leak into the result.
    run_ast(
        file,
        "is_not_error(if(denom > 100, cast('x' as int64), 1))",
        &[("denom", Int64Type::from_data(vec![2i64, 0, -1, 4]))],
    );
    // Same for a scalar function call (not only casts): the all-scalar eval
    // must expand its error over the branch validity as well.
    run_ast(file, "is_not_error(if(denom = 4, 1 % 0, 1))", &[(
        "denom",
        Int64Type::from_data(vec![2i64, 0, -1, 4]),
    )]);
    // A scalar error invalidates every row when there is no partial
    // selection, so the modulo error must poison all rows even though only
    // the division has per-row errors.
    run_ast(file, "is_not_error((1 / denom) + (1 % 0))", &[(
        "denom",
        Int64Type::from_data(vec![2i64, 0, -1, 4]),
    )]);
    // The raising path (no is_not_error) must also observe the scalar error:
    // row 3 executes the branch, so the query must fail instead of silently
    // returning a garbage value.
    run_ast(file, "if(denom = 4, 1 % 0, 1)", &[(
        "denom",
        Int64Type::from_data(vec![2i64, 0, -1, 4]),
    )]);
    // Same shape with an input domain wider than the concrete values, so the
    // optimizer cannot prove the branch dead: at runtime no row selects the
    // branch, and the failing scalar cast must stay silent.
    run_ast_with_context(
        file,
        "is_not_error(if(denom > 100, cast('x' as int64), 1))",
        TestContext {
            entries: &[("denom", Int64Type::from_data(vec![2i64, 0, -1, 4]).into())],
            input_domains: Some(&[(
                "denom",
                Domain::Number(NumberDomain::Int64(SimpleDomain { min: -1, max: 200 })),
            )]),
            func_ctx: FunctionContext::default(),
            strict_eval: true,
        },
    );
}
