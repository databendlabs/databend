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
use common_expression::utils::ColumnFrom;
use common_expression::Column;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_array() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("array.txt").unwrap();

    test_create(file);
    test_length(file);
    test_get(file);
    test_slice(file);
    test_remove_first(file);
    test_remove_last(file);
    test_contains(file);
}

fn test_create(file: &mut impl Write) {
    run_ast(file, "[]", &[]);
    run_ast(file, "[NULL, 8, -10]", &[]);
    run_ast(file, "[['a', 'b'], []]", &[]);
    run_ast(file, r#"['a', 1, parse_json('{"foo":"bar"}')]"#, &[]);
    run_ast(
        file,
        r#"[parse_json('[]'), parse_json('{"foo":"bar"}')]"#,
        &[],
    );
}

fn test_length(file: &mut impl Write) {
    run_ast(file, "length([])", &[]);
    run_ast(file, "length([1, 2, 3])", &[]);
    run_ast(file, "length([true, false])", &[]);
    run_ast(file, "length(['a', 'b', 'c', 'd'])", &[]);
}

fn test_get(file: &mut impl Write) {
    run_ast(file, "[1, 2]['a']", &[]);
    run_ast(file, "[][1]", &[]);
    run_ast(file, "[][NULL]", &[]);
    run_ast(file, "[true, false][1]", &[]);
    run_ast(file, "['a', 'b', 'c'][2]", &[]);
    run_ast(file, "[1, 2, 3][1]", &[]);
    run_ast(file, "[1, 2, 3][3]", &[]);
    run_ast(file, "[1, null, 3][1]", &[]);
    run_ast(file, "[1, null, 3][2]", &[]);
    run_ast(file, "[1, 2, 3][4]", &[]);
    run_ast(file, "[a, b][idx]", &[
        (
            "a",
            DataType::Number(NumberDataType::Int16),
            Column::from_data(vec![0i16, 1, 2]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::Int16),
            Column::from_data(vec![3i16, 4, 5]),
        ),
        (
            "idx",
            DataType::Number(NumberDataType::UInt16),
            Column::from_data(vec![1u16, 2, 3]),
        ),
    ]);
}

fn test_slice(file: &mut impl Write) {
    run_ast(file, "slice([], 1, 2)", &[]);
    run_ast(file, "slice([1], 1, 2)", &[]);
    run_ast(file, "slice([NULL, 1, 2, 3], 0, 2)", &[]);
    run_ast(file, "slice([0, 1, 2, 3], 1, 2)", &[]);
    run_ast(file, "slice([0, 1, 2, 3], 1, 5)", &[]);
    run_ast(file, "slice(['a', 'b', 'c', 'd'], 0, 2)", &[]);
    run_ast(file, "slice(['a', 'b', 'c', 'd'], 1, 4)", &[]);
    run_ast(file, "slice(['a', 'b', 'c', 'd'], 2, 6)", &[]);
}

fn test_remove_first(file: &mut impl Write) {
    run_ast(file, "remove_first([])", &[]);
    run_ast(file, "remove_first([1])", &[]);
    run_ast(file, "remove_first([0, 1, 2, NULL])", &[]);
    run_ast(file, "remove_first([0, 1, 2, 3])", &[]);
    run_ast(file, "remove_first(['a', 'b', 'c', 'd'])", &[]);
}

fn test_remove_last(file: &mut impl Write) {
    run_ast(file, "remove_last([])", &[]);
    run_ast(file, "remove_last([1])", &[]);
    run_ast(file, "remove_last([0, 1, 2, NULL])", &[]);
    run_ast(file, "remove_last([0, 1, 2, 3])", &[]);
    run_ast(file, "remove_last(['a', 'b', 'c', 'd'])", &[]);
}

fn test_contains(file: &mut impl Write) {
    run_ast(file, "false in (false, true)", &[]);
    run_ast(file, "'33' in ('1', '33', '23', '33')", &[]);
    run_ast(file, "contains([1,2,3], 2)", &[]);

    let columns = [
        (
            "int8_col",
            DataType::Number(NumberDataType::Int8),
            Column::from_data(vec![1i8, 2, 7, 8]),
        ),
        (
            "nullable_col",
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            Column::from_data_with_validity(vec![9i64, 10, 11, 12], vec![true, true, false, false]),
        ),
    ];

    run_ast(file, "int8_col not in (1, 2, 3, 4, 5, null)", &columns);
    run_ast(file, "contains([1,2,null], nullable_col)", &columns);
    run_ast(
        file,
        "contains([(1,'2', 3, false), (1,'2', 4, true), null], (1,'2', 3, false))",
        &columns,
    );
    run_ast(file, "nullable_col in (null, 9, 10, 12)", &columns);
    run_ast(
        file,
        "nullable_col in (1, '9', 3, 10, 12, true, [1,2,3])",
        &columns,
    );
}
