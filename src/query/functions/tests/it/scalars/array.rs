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
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_array() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("array.txt").unwrap();

    test_create(file);
    test_length(file);
    test_range(file);
    test_get(file);
    test_slice(file);
    test_contains(file);
    test_array_remove_first(file);
    test_array_remove_last(file);
    test_array_concat(file);
    test_array_prepend(file);
    test_array_append(file);
    test_array_indexof(file);
    test_array_unique(file);
    test_array_distinct(file);
    test_array_sum(file);
    test_array_avg(file);
    test_array_count(file);
    test_array_max(file);
    test_array_min(file);
    test_array_any(file);
    test_array_stddev_samp(file);
    test_array_stddev_pop(file);
    test_array_median(file);
    test_array_approx_count_distinct(file);
    test_array_kurtosis(file);
    test_array_skewness(file);
    test_array_sort(file);
}

fn test_create(file: &mut impl Write) {
    run_ast(file, "[]", &[]);
    run_ast(file, "['a', 1]", &[]);
    run_ast(file, "[-1, true]", &[]);
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

fn test_range(file: &mut impl Write) {
    run_ast(file, "range(10, 20)", &[]);
    run_ast(file, "range(10, 500000011)", &[]);
}

fn test_get(file: &mut impl Write) {
    run_ast(file, "[1, 2]['a']", &[]);
    run_ast(file, "[][1]", &[]);
    run_ast(file, "[][NULL]", &[]);
    run_ast(file, "[NULL][0]", &[]);
    run_ast(file, "[true, false][1]", &[]);
    run_ast(file, "['a', 'b', 'c'][2]", &[]);
    run_ast(file, "[1, 2, 3][1]", &[]);
    run_ast(file, "[1, 2, 3][3]", &[]);
    run_ast(file, "[1, null, 3][1]", &[]);
    run_ast(file, "[1, null, 3][2]", &[]);
    run_ast(file, "[1, 2, 3][4]", &[]);
    run_ast(file, "[a, b][idx]", &[
        ("a", Int16Type::from_data(vec![0i16, 1, 2])),
        ("b", Int16Type::from_data(vec![3i16, 4, 5])),
        ("idx", UInt16Type::from_data(vec![1u16, 2, 3])),
    ]);
}

fn test_slice(file: &mut impl Write) {
    run_ast(file, "slice([], 1)", &[]);
    run_ast(file, "slice([0, 1, 2, 3], 2)", &[]);
    run_ast(file, "slice(['a', 'b', 'c', 'd'], 3)", &[]);
    run_ast(file, "slice([a, b, c], 2)", &[
        ("a", Int16Type::from_data(vec![0i16, 1, 2])),
        ("b", Int16Type::from_data(vec![3i16, 4, 5])),
        ("c", Int16Type::from_data(vec![7i16, 8, 9])),
    ]);

    run_ast(file, "slice([], 1, 2)", &[]);
    run_ast(file, "slice([1], 1, 2)", &[]);
    run_ast(file, "slice([NULL, 1, 2, 3], 0, 2)", &[]);
    run_ast(file, "slice([0, 1, 2, 3], 1, 2)", &[]);
    run_ast(file, "slice([0, 1, 2, 3], 1, 5)", &[]);
    run_ast(file, "slice(['a', 'b', 'c', 'd'], 0, 2)", &[]);
    run_ast(file, "slice(['a', 'b', 'c', 'd'], 1, 4)", &[]);
    run_ast(file, "slice(['a', 'b', 'c', 'd'], 2, 6)", &[]);
    run_ast(file, "slice([a, b, c], 1, 2)", &[
        ("a", Int16Type::from_data(vec![0i16, 1, 2])),
        ("b", Int16Type::from_data(vec![3i16, 4, 5])),
        ("c", Int16Type::from_data(vec![7i16, 8, 9])),
    ]);
}

fn test_array_remove_first(file: &mut impl Write) {
    run_ast(file, "array_remove_first([])", &[]);
    run_ast(file, "array_remove_first([1])", &[]);
    run_ast(file, "array_remove_first([0, 1, 2, NULL])", &[]);
    run_ast(file, "array_remove_first([0, 1, 2, 3])", &[]);
    run_ast(file, "array_remove_first(['a', 'b', 'c', 'd'])", &[]);
    run_ast(file, "array_remove_first([a, b])", &[
        ("a", Int16Type::from_data(vec![0i16, 1, 2])),
        ("b", Int16Type::from_data(vec![3i16, 4, 5])),
    ]);
}

fn test_array_remove_last(file: &mut impl Write) {
    run_ast(file, "array_remove_last([])", &[]);
    run_ast(file, "array_remove_last([1])", &[]);
    run_ast(file, "array_remove_last([0, 1, 2, NULL])", &[]);
    run_ast(file, "array_remove_last([0, 1, 2, 3])", &[]);
    run_ast(file, "array_remove_last(['a', 'b', 'c', 'd'])", &[]);
    run_ast(file, "array_remove_last([a, b])", &[
        ("a", Int16Type::from_data(vec![0i16, 1, 2])),
        ("b", Int16Type::from_data(vec![3i16, 4, 5])),
    ]);
}

fn test_contains(file: &mut impl Write) {
    run_ast(file, "false in (false, true)", &[]);
    run_ast(file, "'33' in ('1', '33', '23', '33')", &[]);
    run_ast(file, "contains([1,2,3], 2)", &[]);

    let columns = [
        ("int8_col", Int8Type::from_data(vec![1i8, 2, 7, 8])),
        (
            "string_col",
            StringType::from_data(vec![r#"1"#, r#"2"#, r#"5"#, r#"1234"#]),
        ),
        (
            "nullable_col",
            Int64Type::from_data_with_validity(vec![9i64, 10, 11, 12], vec![
                true, true, false, false,
            ]),
        ),
    ];

    run_ast(file, "int8_col not in (1, 2, 3, 4, 5, null)", &columns);
    run_ast(
        file,
        "contains(['5000', '6000', '7000'], string_col)",
        &columns,
    );

    run_ast(file, "contains(['1', '5'], string_col)", &columns);

    run_ast(
        file,
        "contains(['15000', '6000', '7000'], string_col)",
        &columns,
    );

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

fn test_array_concat(file: &mut impl Write) {
    run_ast(file, "array_concat([], [])", &[]);
    run_ast(file, "array_concat([], [1,2])", &[]);
    run_ast(file, "array_concat([false, true], [])", &[]);
    run_ast(file, "array_concat([false, true], [1,2])", &[]);
    run_ast(file, "array_concat([false, true], [true, false])", &[]);
    run_ast(file, "array_concat([1,2,3], ['s', null])", &[]);

    let columns = [
        ("int8_col", Int8Type::from_data(vec![1i8, 2, 7, 8])),
        (
            "nullable_col",
            Int64Type::from_data_with_validity(vec![9i64, 10, 11, 12], vec![
                true, true, false, false,
            ]),
        ),
    ];

    run_ast(
        file,
        "array_concat([1, 2, 3, 4, 5, null], [nullable_col])",
        &columns,
    );
    run_ast(file, "array_concat([1,2,null], [int8_col])", &columns);
}

fn test_array_prepend(file: &mut impl Write) {
    run_ast(file, "array_prepend(1, [])", &[]);
    run_ast(file, "array_prepend(1, [2, 3, NULL, 4])", &[]);
    run_ast(file, "array_prepend('a', ['b', NULL, NULL, 'c', 'd'])", &[]);
    run_ast(
        file,
        "array_prepend(1, CAST([2, 3] AS Array(INT8) NULL))",
        &[],
    );
    run_ast(file, "array_prepend(a, [b, c])", &[
        ("a", Int16Type::from_data(vec![0i16, 1, 2])),
        ("b", Int16Type::from_data(vec![3i16, 4, 5])),
        ("c", Int16Type::from_data(vec![6i16, 7, 8])),
    ]);
}

fn test_array_append(file: &mut impl Write) {
    run_ast(file, "array_append([], 1)", &[]);
    run_ast(file, "array_append([2, 3, NULL, 4], 5)", &[]);
    run_ast(file, "array_append(['b', NULL, NULL, 'c', 'd'], 'e')", &[]);
    run_ast(
        file,
        "array_append(CAST([1, 2] AS Array(INT8) NULL), 3)",
        &[],
    );
    run_ast(file, "array_append([b, c], a)", &[
        ("a", Int16Type::from_data(vec![0i16, 1, 2])),
        ("b", Int16Type::from_data(vec![3i16, 4, 5])),
        ("c", Int16Type::from_data(vec![6i16, 7, 8])),
    ]);
}

fn test_array_indexof(file: &mut impl Write) {
    run_ast(file, "array_indexof([], NULL)", &[]);
    run_ast(file, "array_indexof(NULL, NULL)", &[]);
    run_ast(file, "array_indexof([false, true], false)", &[]);
    run_ast(file, "array_indexof([], false)", &[]);
    run_ast(file, "array_indexof([false, true], null)", &[]);
    run_ast(file, "array_indexof([false, true], 0)", &[]);
    run_ast(file, "array_indexof([1,2,3,'s'], 's')", &[]);
    run_ast(
        file,
        "array_indexof([1::VARIANT,2::VARIANT,3::VARIANT,4::VARIANT], 2::VARIANT)",
        &[],
    );

    let columns = [
        ("int8_col", Int8Type::from_data(vec![1i8, 2, 7, 8])),
        (
            "nullable_col",
            Int64Type::from_data_with_validity(vec![9i64, 10, 11, 12], vec![
                true, true, false, false,
            ]),
        ),
    ];

    run_ast(
        file,
        "array_indexof([1, 2, 3, 4, 5, null], nullable_col)",
        &columns,
    );
    run_ast(file, "array_indexof([9,10,null], int8_col)", &columns);
}

fn test_array_unique(file: &mut impl Write) {
    run_ast(file, "array_unique([])", &[]);
    run_ast(file, "array_unique([1, 1, 2, 2, 3, NULL])", &[]);
    run_ast(
        file,
        "array_unique(['a', NULL, 'a', 'b', NULL, 'c', 'd'])",
        &[],
    );

    run_ast(file, "array_unique([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 1, 2, 4])),
        ("b", Int16Type::from_data(vec![2i16, 1, 2, 4])),
        ("c", Int16Type::from_data(vec![3i16, 1, 3, 4])),
        ("d", Int16Type::from_data(vec![4i16, 2, 3, 4])),
    ]);
}

fn test_array_distinct(file: &mut impl Write) {
    run_ast(file, "array_distinct([])", &[]);
    run_ast(file, "array_distinct([1, 1, 2, 2, 3, NULL])", &[]);
    run_ast(
        file,
        "array_distinct(['a', NULL, 'a', 'b', NULL, 'c', 'd'])",
        &[],
    );

    run_ast(file, "array_distinct([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 1, 2, 4])),
        ("b", Int16Type::from_data(vec![2i16, 1, 2, 4])),
        ("c", Int16Type::from_data(vec![3i16, 1, 3, 4])),
        ("d", Int16Type::from_data(vec![4i16, 2, 3, 4])),
    ]);
}

fn test_array_sum(file: &mut impl Write) {
    run_ast(file, "array_sum([])", &[]);
    run_ast(file, "array_sum([1, 2, 3, 4, 5, 6, 7])", &[]);
    run_ast(file, "array_sum([1, 2, 3, 4, 5, NULL, 6])", &[]);
    run_ast(file, "array_sum([1.2, 3.4, 5.6, 7.8])", &[]);
    run_ast(file, "array_sum([1.2, NULL, 3.4, 5.6, NULL])", &[]);

    run_ast(file, "array_sum([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);

    run_ast(file, "array_sum([a, b, c, d])", &[
        (
            "a",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 0, 4], vec![true, true, false, true]),
        ),
        (
            "b",
            UInt64Type::from_data_with_validity(vec![2u64, 0, 5, 6], vec![true, false, true, true]),
        ),
        (
            "c",
            UInt64Type::from_data_with_validity(vec![3u64, 7, 8, 9], vec![true, true, true, true]),
        ),
        (
            "d",
            UInt64Type::from_data_with_validity(vec![4u64, 6, 5, 0], vec![true, true, true, false]),
        ),
    ]);
}

fn test_array_avg(file: &mut impl Write) {
    run_ast(file, "array_avg([])", &[]);
    run_ast(file, "array_avg([1, 2, 3, 4, 5, 6, 7])", &[]);
    run_ast(file, "array_avg([1, 2, 3, 4, 5, NULL, 6])", &[]);
    run_ast(file, "array_avg([1.2, 3.4, 5.6, 7.8])", &[]);
    run_ast(file, "array_avg([1.2, NULL, 3.4, 5.6, NULL])", &[]);

    run_ast(file, "array_avg([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);

    run_ast(file, "array_avg([a, b, c, d])", &[
        (
            "a",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 0, 4], vec![true, true, false, true]),
        ),
        (
            "b",
            UInt64Type::from_data_with_validity(vec![2u64, 0, 5, 6], vec![true, false, true, true]),
        ),
        (
            "c",
            UInt64Type::from_data_with_validity(vec![3u64, 7, 8, 9], vec![true, true, true, true]),
        ),
        (
            "d",
            UInt64Type::from_data_with_validity(vec![4u64, 6, 5, 0], vec![true, true, true, false]),
        ),
    ]);
}

fn test_array_count(file: &mut impl Write) {
    run_ast(file, "array_count([])", &[]);
    run_ast(file, "array_count([1, 2, 3, 4, 5, 6, 7])", &[]);
    run_ast(file, "array_count([1, 2, 3, 4, 5, NULL, 6])", &[]);
    run_ast(file, "array_count([1.2, 3.4, 5.6, 7.8])", &[]);
    run_ast(file, "array_count([1.2, NULL, 3.4, 5.6, NULL])", &[]);
    run_ast(file, "array_count(['a', 'b', 'c', 'd', 'e'])", &[]);
    run_ast(file, "array_count(['a', 'b', NULL, 'c', 'd', NULL])", &[]);

    run_ast(file, "array_count([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);

    run_ast(file, "array_count([a, b, c, d])", &[
        (
            "a",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 0, 4], vec![true, true, false, true]),
        ),
        (
            "b",
            UInt64Type::from_data_with_validity(vec![2u64, 0, 5, 6], vec![true, false, true, true]),
        ),
        (
            "c",
            UInt64Type::from_data_with_validity(vec![3u64, 7, 8, 9], vec![true, true, true, true]),
        ),
        (
            "d",
            UInt64Type::from_data_with_validity(vec![4u64, 6, 5, 0], vec![true, true, true, false]),
        ),
    ]);
}

fn test_array_max(file: &mut impl Write) {
    run_ast(file, "array_max([])", &[]);
    run_ast(file, "array_max([1, 2, 3, 4, 5, 6, 7])", &[]);
    run_ast(file, "array_max([1, 2, 3, 4, 5, NULL, 6])", &[]);
    run_ast(file, "array_max([1.2, 3.4, 5.6, 7.8])", &[]);
    run_ast(file, "array_max([1.2, NULL, 3.4, 5.6, NULL])", &[]);
    run_ast(file, "array_max(['a', 'b', 'c', 'd', 'e'])", &[]);
    run_ast(file, "array_max(['a', 'b', NULL, 'c', 'd', NULL])", &[]);

    run_ast(file, "array_max([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);

    run_ast(file, "array_max([a, b, c, d])", &[
        (
            "a",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 0, 4], vec![true, true, false, true]),
        ),
        (
            "b",
            UInt64Type::from_data_with_validity(vec![2u64, 0, 5, 6], vec![true, false, true, true]),
        ),
        (
            "c",
            UInt64Type::from_data_with_validity(vec![3u64, 7, 8, 9], vec![true, true, true, true]),
        ),
        (
            "d",
            UInt64Type::from_data_with_validity(vec![4u64, 6, 5, 0], vec![true, true, true, false]),
        ),
    ]);
}

fn test_array_min(file: &mut impl Write) {
    run_ast(file, "array_min([])", &[]);
    run_ast(file, "array_min([1, 2, 3, 4, 5, 6, 7])", &[]);
    run_ast(file, "array_min([1, 2, 3, 4, 5, NULL, 6])", &[]);
    run_ast(file, "array_min([1.2, 3.4, 5.6, 7.8])", &[]);
    run_ast(file, "array_min([1.2, NULL, 3.4, 5.6, NULL])", &[]);
    run_ast(file, "array_min(['a', 'b', 'c', 'd', 'e'])", &[]);
    run_ast(file, "array_min(['a', 'b', NULL, 'c', 'd', NULL])", &[]);

    run_ast(file, "array_min([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);

    run_ast(file, "array_min([a, b, c, d])", &[
        (
            "a",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 0, 4], vec![true, true, false, true]),
        ),
        (
            "b",
            UInt64Type::from_data_with_validity(vec![2u64, 0, 5, 6], vec![true, false, true, true]),
        ),
        (
            "c",
            UInt64Type::from_data_with_validity(vec![3u64, 7, 8, 9], vec![true, true, true, true]),
        ),
        (
            "d",
            UInt64Type::from_data_with_validity(vec![4u64, 6, 5, 0], vec![true, true, true, false]),
        ),
    ]);
}

fn test_array_any(file: &mut impl Write) {
    run_ast(file, "array_any([])", &[]);
    run_ast(file, "array_any([1, 2, 3])", &[]);
    run_ast(file, "array_any([NULL, 3, 2, 1])", &[]);
    run_ast(file, "array_any(['a', 'b', 'c'])", &[]);
    run_ast(file, "array_any([NULL, 'x', 'y', 'z'])", &[]);

    run_ast(file, "array_any([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);

    run_ast(file, "array_any([a, b, c, d])", &[
        (
            "a",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 0, 4], vec![true, true, false, true]),
        ),
        (
            "b",
            UInt64Type::from_data_with_validity(vec![2u64, 0, 5, 6], vec![true, false, true, true]),
        ),
        (
            "c",
            UInt64Type::from_data_with_validity(vec![3u64, 7, 8, 9], vec![true, true, true, true]),
        ),
        (
            "d",
            UInt64Type::from_data_with_validity(vec![4u64, 6, 5, 0], vec![true, true, true, false]),
        ),
    ]);
}

fn test_array_stddev_samp(file: &mut impl Write) {
    run_ast(file, "array_stddev_samp([])", &[]);
    run_ast(file, "array_stddev_samp([1, 2, 3])", &[]);
    run_ast(file, "array_stddev_samp([NULL, 3, 2, 1])", &[]);

    run_ast(file, "array_stddev_samp([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);
}

fn test_array_stddev_pop(file: &mut impl Write) {
    run_ast(file, "array_stddev_pop([])", &[]);
    run_ast(file, "array_stddev_pop([1, 2, 3])", &[]);
    run_ast(file, "array_stddev_pop([NULL, 3, 2, 1])", &[]);

    run_ast(file, "array_stddev_pop([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);
}

fn test_array_median(file: &mut impl Write) {
    run_ast(file, "array_median([])", &[]);
    run_ast(file, "array_median([1, 2, 3])", &[]);
    run_ast(file, "array_median([NULL, 3, 2, 1])", &[]);

    run_ast(file, "array_median([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);
}

fn test_array_approx_count_distinct(file: &mut impl Write) {
    run_ast(file, "array_approx_count_distinct([])", &[]);
    run_ast(file, "array_approx_count_distinct([1, 2, 3])", &[]);
    run_ast(file, "array_approx_count_distinct([NULL, 3, 2, 1])", &[]);
    run_ast(file, "array_approx_count_distinct(['a', 'b', 'c'])", &[]);
    run_ast(
        file,
        "array_approx_count_distinct([NULL, 'x', 'y', 'z'])",
        &[],
    );

    run_ast(file, "array_approx_count_distinct([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);

    run_ast(file, "array_approx_count_distinct([a, b, c, d])", &[
        (
            "a",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 0, 4], vec![true, true, false, true]),
        ),
        (
            "b",
            UInt64Type::from_data_with_validity(vec![2u64, 0, 5, 6], vec![true, false, true, true]),
        ),
        (
            "c",
            UInt64Type::from_data_with_validity(vec![3u64, 7, 8, 9], vec![true, true, true, true]),
        ),
        (
            "d",
            UInt64Type::from_data_with_validity(vec![4u64, 6, 5, 0], vec![true, true, true, false]),
        ),
    ]);
}

fn test_array_kurtosis(file: &mut impl Write) {
    run_ast(file, "array_kurtosis([])", &[]);
    run_ast(file, "array_kurtosis([1, 2, 3])", &[]);
    run_ast(file, "array_kurtosis([NULL, 3, 2, 1])", &[]);

    run_ast(file, "array_kurtosis([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);

    run_ast(file, "array_kurtosis([a, b, c, d])", &[
        (
            "a",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 0, 4], vec![true, true, false, true]),
        ),
        (
            "b",
            UInt64Type::from_data_with_validity(vec![2u64, 0, 5, 6], vec![true, false, true, true]),
        ),
        (
            "c",
            UInt64Type::from_data_with_validity(vec![3u64, 7, 8, 9], vec![true, true, true, true]),
        ),
        (
            "d",
            UInt64Type::from_data_with_validity(vec![4u64, 6, 5, 0], vec![true, true, true, false]),
        ),
    ]);
}

fn test_array_skewness(file: &mut impl Write) {
    run_ast(file, "array_skewness([])", &[]);
    run_ast(file, "array_skewness([1, 2, 3])", &[]);
    run_ast(file, "array_skewness([NULL, 3, 2, 1])", &[]);

    run_ast(file, "array_skewness([a, b, c, d])", &[
        ("a", Int16Type::from_data(vec![1i16, 5, 8, 3])),
        ("b", Int16Type::from_data(vec![2i16, 6, 1, 2])),
        ("c", Int16Type::from_data(vec![3i16, 7, 7, 6])),
        ("d", Int16Type::from_data(vec![4i16, 8, 1, 9])),
    ]);

    run_ast(file, "array_skewness([a, b, c, d])", &[
        (
            "a",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 0, 4], vec![true, true, false, true]),
        ),
        (
            "b",
            UInt64Type::from_data_with_validity(vec![2u64, 0, 5, 6], vec![true, false, true, true]),
        ),
        (
            "c",
            UInt64Type::from_data_with_validity(vec![3u64, 7, 8, 9], vec![true, true, true, true]),
        ),
        (
            "d",
            UInt64Type::from_data_with_validity(vec![4u64, 6, 5, 0], vec![true, true, true, false]),
        ),
    ]);
}

fn test_array_sort(file: &mut impl Write) {
    run_ast(file, "array_sort_asc_null_first([])", &[]);
    run_ast(file, "array_sort_desc_null_first([])", &[]);
    run_ast(file, "array_sort_asc_null_first(NULL)", &[]);
    run_ast(file, "array_sort_asc_null_first([NULL, NULL, NULL])", &[]);
    run_ast(file, "array_sort_desc_null_first([[], [], []])", &[]);
    run_ast(file, "array_sort_asc_null_first([{}, {}, {}])", &[]);
    run_ast(
        file,
        "array_sort_asc_null_first([8, 20, 1, 2, 3, 4, 5, 6, 7])",
        &[],
    );
    run_ast(
        file,
        "array_sort_asc_null_last([8, 20, 1, 2, 3, 4, 5, 6, 7])",
        &[],
    );
    run_ast(
        file,
        "array_sort_desc_null_first([8, 20, 1, 2, 3, 4, 5, 6, 7])",
        &[],
    );
    run_ast(
        file,
        "array_sort_desc_null_last([8, 20, 1, 2, 3, 4, 5, 6, 7])",
        &[],
    );
    run_ast(
        file,
        "array_sort_asc_null_first([1.2, NULL, 3.4, 5.6, '2.2', NULL])",
        &[],
    );
    run_ast(
        file,
        "array_sort_asc_null_last([1.2, NULL, 3.4, 5.6, '2.2', NULL])",
        &[],
    );
    run_ast(
        file,
        "array_sort_desc_null_first([1.2, NULL, 3.4, 5.6, '2.2', NULL])",
        &[],
    );
    run_ast(
        file,
        "array_sort_desc_null_last([1.2, NULL, 3.4, 5.6, '2.2', NULL])",
        &[],
    );
}
