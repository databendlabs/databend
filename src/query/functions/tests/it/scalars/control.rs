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

use databend_common_expression::types::*;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_control() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("control.txt").unwrap();

    test_if(file);
    test_is_not_null(file);
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
