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

use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::*;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

fn one_null_column() -> Vec<(&'static str, Column)> {
    vec![(
        "a",
        UInt8Type::from_data_with_validity(vec![0_u8], vec![false]),
    )]
}

#[test]
fn test_boolean() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("boolean.txt").unwrap();

    test_and(file);
    test_not(file);
    test_or(file);
    test_xor(file);
    test_is_true(file);
}

fn test_and(file: &mut impl Write) {
    run_ast(file, "true AND false", &[]);
    run_ast(file, "true AND null", &[]);
    run_ast(file, "true AND true", &[]);
    run_ast(file, "false AND false", &[]);
    run_ast(file, "false AND null", &[]);
    run_ast(file, "false AND true", &[]);

    run_ast(file, "true AND 1", &[]);
    run_ast(file, "'a' and 1", &[]);
    run_ast(file, "NOT NOT 'a'", &[]);

    run_ast(file, "(a < 1) AND (a < 1)", one_null_column().as_slice()); // NULL(false)  AND NULL(false)
    run_ast(file, "(a > 1) AND (a < 1)", one_null_column().as_slice()); // NULL(false)  AND NULL(true)
    run_ast(file, "(a < 1) AND (a > 1)", one_null_column().as_slice()); // NULL(true)   AND NULL(false)
    run_ast(file, "(a < 1) AND (a < 1)", one_null_column().as_slice()); // NULL(true)   AND NULL(true)
    run_ast(file, "(a > 1) AND (0 > 1)", one_null_column().as_slice()); // NULL(false)  AND false
    run_ast(file, "(a > 1) AND (0 < 1)", one_null_column().as_slice()); // NULL(false)  AND true
    run_ast(file, "(a < 1) AND (0 > 1)", one_null_column().as_slice()); // NULL(true)   AND false
    run_ast(file, "(a < 1) AND (0 < 1)", one_null_column().as_slice()); // NULL(true)   AND true
    run_ast(file, "(0 > 1) AND (a > 1)", one_null_column().as_slice()); // false        AND NULL(false)
    run_ast(file, "(0 > 1) AND (a < 1)", one_null_column().as_slice()); // false        AND NULL(true)
    run_ast(file, "(0 < 1) AND (a > 1)", one_null_column().as_slice()); // true         AND NULL(false)
    run_ast(file, "(0 < 1) AND (a < 1)", one_null_column().as_slice()); // true         AND NULL(true)
}

fn test_not(file: &mut impl Write) {
    run_ast(file, "NOT a", &[("a", Column::Null { len: 5 })]);
    run_ast(file, "NOT a", &[(
        "a",
        Column::Boolean(vec![true, false, true].into()),
    )]);
    run_ast(file, "NOT a", &[(
        "a",
        NullableColumn::new_column(
            Column::Boolean(vec![true, false, true].into()),
            vec![false, true, false].into(),
        ),
    )]);
    run_ast(file, "NOT a", &[(
        "a",
        NullableColumn::new_column(
            Column::Boolean(vec![false, false, false].into()),
            vec![true, true, false].into(),
        ),
    )]);
}

fn test_or(file: &mut impl Write) {
    run_ast(file, "true OR false", &[]);
    run_ast(file, "null OR false", &[]);

    run_ast(file, "(a < 1) OR (a < 1)", one_null_column().as_slice()); // NULL(false)  OR NULL(false)
    run_ast(file, "(a > 1) OR (a < 1)", one_null_column().as_slice()); // NULL(false)  OR NULL(true)
    run_ast(file, "(a < 1) OR (a > 1)", one_null_column().as_slice()); // NULL(true)   OR NULL(false)
    run_ast(file, "(a < 1) OR (a < 1)", one_null_column().as_slice()); // NULL(true)   OR NULL(true)
    run_ast(file, "(a > 1) OR (0 > 1)", one_null_column().as_slice()); // NULL(false)  OR false
    run_ast(file, "(a > 1) OR (0 < 1)", one_null_column().as_slice()); // NULL(false)  OR true
    run_ast(file, "(a < 1) OR (0 > 1)", one_null_column().as_slice()); // NULL(true)   OR false
    run_ast(file, "(a < 1) OR (0 < 1)", one_null_column().as_slice()); // NULL(true)   OR true
    run_ast(file, "(0 > 1) OR (a > 1)", one_null_column().as_slice()); // false        OR NULL(false)
    run_ast(file, "(0 > 1) OR (a < 1)", one_null_column().as_slice()); // false        OR NULL(true)
    run_ast(file, "(0 < 1) OR (a > 1)", one_null_column().as_slice()); // true         OR NULL(false)
    run_ast(file, "(0 < 1) OR (a < 1)", one_null_column().as_slice()); // true         OR NULL(true)
}

fn test_xor(file: &mut impl Write) {
    run_ast(file, "true XOR false", &[]);
    run_ast(file, "null XOR false", &[]);
}

fn test_is_true(file: &mut impl Write) {
    run_ast(file, "is_true(null)", &[]);
    run_ast(file, "is_true(false)", &[]);
    run_ast(file, "is_true(true)", &[]);

    run_ast(file, "is_true(col)", &[(
        "col",
        BooleanType::from_data(vec![true, false]),
    )]);
    run_ast(file, "is_true(col)", &[(
        "col",
        BooleanType::from_data_with_validity(vec![true, false, true, false], vec![
            true, true, false, false,
        ]),
    )]);
}
