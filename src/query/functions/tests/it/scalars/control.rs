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
