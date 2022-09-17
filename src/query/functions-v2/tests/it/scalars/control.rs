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

use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::Column;
use common_expression::ColumnFrom;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_control() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("control.txt").unwrap();

    test_multi_if(file);
    test_is_not_null(file);
}

fn test_multi_if(file: &mut impl Write) {
    run_ast(file, "multi_if(false, 1, false, 2, NULL)", &[]);
    run_ast(file, "multi_if(true, 1, NULL, 2, NULL)", &[]);
    run_ast(file, "multi_if(false, 1, true, 2, NULL)", &[]);
    run_ast(file, "multi_if(true, 1, true, 2, NULL)", &[]);
    run_ast(file, "multi_if(true, 1, true, NULL, 2)", &[]);
    run_ast(file, "multi_if(true, 1, NULL)", &[]);
    run_ast(file, "multi_if(false, 1, NULL)", &[]);
    run_ast(file, "multi_if(cond_a, expr_true, expr_else)", &[
        (
            "cond_a",
            DataType::Boolean,
            Column::Boolean(vec![true, true, false, false].into()),
        ),
        (
            "expr_true",
            DataType::Number(NumberDataType::Int64),
            Column::from_data(vec![1i64, 2, 3, 4]),
        ),
        (
            "expr_else",
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            Column::from_data_with_validity(vec![5i64, 6, 7, 8], vec![true, false, true, false]),
        ),
    ]);
    run_ast(file, "multi_if(cond_a, expr_true, expr_else)", &[
        (
            "cond_a",
            DataType::Boolean,
            Column::from_data(vec![false, false, true, true]),
        ),
        (
            "expr_true",
            DataType::Number(NumberDataType::Int64),
            Column::from_data(vec![1i64, 2, 3, 4]),
        ),
        (
            "expr_else",
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            Column::from_data_with_validity(vec![5i64, 6, 7, 8], vec![true, true, false, false]),
        ),
    ]);
    run_ast(
        file,
        "multi_if(cond_a, expr_a, cond_b, expr_b, expr_else)",
        &[
            (
                "cond_a",
                DataType::Boolean,
                Column::Boolean(vec![true, true, false, false].into()),
            ),
            (
                "expr_a",
                DataType::Number(NumberDataType::Int64),
                Column::from_data(vec![1i64, 2, 3, 4]),
            ),
            (
                "cond_b",
                DataType::Boolean,
                Column::from_data_with_validity(vec![true, true, true, true], vec![
                    false, true, false, true,
                ]),
            ),
            (
                "expr_b",
                DataType::Number(NumberDataType::Int64),
                Column::from_data(vec![5i64, 6, 7, 8]),
            ),
            (
                "expr_else",
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
                Column::from_data_with_validity(vec![9i64, 10, 11, 12], vec![
                    true, true, false, false,
                ]),
            ),
        ],
    );
    run_ast(
        file,
        "multi_if(cond_a, expr_a, cond_b, expr_b, expr_else)",
        &[
            (
                "cond_a",
                DataType::Boolean,
                Column::from_data(vec![true, true, false, false]),
            ),
            (
                "expr_a",
                DataType::Number(NumberDataType::Int64),
                Column::from_data(vec![1i64, 2, 3, 4]),
            ),
            (
                "cond_b",
                DataType::Boolean,
                Column::from_data(vec![true, false, true, false]),
            ),
            (
                "expr_b",
                DataType::Number(NumberDataType::Int64),
                Column::from_data(vec![5i64, 6, 7, 8]),
            ),
            (
                "expr_else",
                DataType::Number(NumberDataType::Int64),
                Column::from_data(vec![9i64, 10, 11, 12]),
            ),
        ],
    );
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
        DataType::Null,
        Column::Null { len: 13 },
    )]);
    run_ast(file, "is_not_null(int64_col)", &[(
        "int64_col",
        DataType::Number(NumberDataType::Int64),
        Column::from_data(vec![5i64, 6, 7, 8]),
    )]);
    run_ast(file, "is_not_null(nullable_col)", &[(
        "nullable_col",
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
        Column::from_data_with_validity(vec![9i64, 10, 11, 12], vec![true, true, false, false]),
    )]);
}
