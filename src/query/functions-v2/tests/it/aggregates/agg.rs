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
use common_functions_v2::aggregates::eval_aggr;
use goldenfile::Mint;

use super::run_agg_ast;
use super::simulate_two_groups_group_by;
use super::AggregationSimulator;

#[test]
fn test_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("agg.txt").unwrap();

    test_count(file, eval_aggr);
    test_sum(file, eval_aggr);
    test_avg(file, eval_aggr);
    test_uniq(file, eval_aggr);
    test_agg_if(file, eval_aggr);
    test_agg_distinct(file, eval_aggr);
    test_agg_max(file, eval_aggr);
    test_agg_min(file, eval_aggr);
    test_agg_any(file, eval_aggr);
}

#[test]
fn test_agg_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("agg_group_by.txt").unwrap();

    test_count(file, simulate_two_groups_group_by);
    test_sum(file, simulate_two_groups_group_by);
    test_avg(file, simulate_two_groups_group_by);
    test_uniq(file, simulate_two_groups_group_by);
    test_agg_if(file, simulate_two_groups_group_by);
    test_agg_distinct(file, simulate_two_groups_group_by);
    test_agg_max(file, simulate_two_groups_group_by);
    test_agg_min(file, simulate_two_groups_group_by);
    test_agg_any(file, simulate_two_groups_group_by);
}

fn get_example() -> Vec<(&'static str, DataType, Column)> {
    vec![
        (
            "a",
            DataType::Number(NumberDataType::Int64),
            Column::from_data(vec![4i64, 3, 2, 1]),
        ),
        (
            "b",
            DataType::Number(NumberDataType::UInt64),
            Column::from_data(vec![1u64, 2, 3, 4]),
        ),
        (
            "c",
            DataType::Number(NumberDataType::UInt64),
            Column::from_data(vec![1u64, 2, 1, 3]),
        ),
        (
            "x_null",
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
            Column::from_data_with_validity(vec![1u64, 2, 3, 4], vec![true, true, false, false]),
        ),
        (
            "y_null",
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
            Column::from_data_with_validity(vec![1u64, 2, 3, 4], vec![false, false, true, true]),
        ),
        (
            "all_null",
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
            Column::from_data_with_validity(vec![1u64, 2, 3, 4], vec![false, false, false, false]),
        ),
    ]
}

fn test_count(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "count(1)", get_example().as_slice(), simulator);
    run_agg_ast(file, "count()", get_example().as_slice(), simulator);
    run_agg_ast(file, "count(a)", get_example().as_slice(), simulator);
    run_agg_ast(file, "count(x_null)", get_example().as_slice(), simulator);
}

fn test_sum(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "sum(1)", get_example().as_slice(), simulator);
    run_agg_ast(file, "sum(a)", get_example().as_slice(), simulator);
    run_agg_ast(file, "sum(x_null)", get_example().as_slice(), simulator);
}

fn test_avg(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "avg(1)", get_example().as_slice(), simulator);
    run_agg_ast(file, "avg(a)", get_example().as_slice(), simulator);
    run_agg_ast(file, "avg(x_null)", get_example().as_slice(), simulator);
}

fn test_uniq(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "uniq(1)", get_example().as_slice(), simulator);
    run_agg_ast(file, "uniq(c)", get_example().as_slice(), simulator);
    run_agg_ast(file, "uniq(x_null)", get_example().as_slice(), simulator);
}

fn test_agg_if(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "count_if(1, x_null is null)",
        get_example().as_slice(),
        simulator,
    );
    run_agg_ast(
        file,
        "sum_if(a, x_null is null)",
        get_example().as_slice(),
        simulator,
    );
    run_agg_ast(
        file,
        "sum_if(b, x_null is null)",
        get_example().as_slice(),
        simulator,
    );
}

fn test_agg_distinct(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "sum_distinct(a)", get_example().as_slice(), simulator);
    run_agg_ast(file, "sum_distinct(c)", get_example().as_slice(), simulator);
    run_agg_ast(
        file,
        "sum_distinct(x_null)",
        get_example().as_slice(),
        simulator,
    );
}

fn test_agg_max(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "max(1)", get_example().as_slice(), simulator);
    run_agg_ast(file, "max(NULL)", get_example().as_slice(), simulator);
    run_agg_ast(file, "max(a)", get_example().as_slice(), simulator);
    run_agg_ast(file, "max(b)", get_example().as_slice(), simulator);
    run_agg_ast(file, "max(x_null)", get_example().as_slice(), simulator);
    run_agg_ast(file, "max(all_null)", get_example().as_slice(), simulator);
}

fn test_agg_min(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "min(1)", get_example().as_slice(), simulator);
    run_agg_ast(file, "min(NULL)", get_example().as_slice(), simulator);
    run_agg_ast(file, "min(a)", get_example().as_slice(), simulator);
    run_agg_ast(file, "min(b)", get_example().as_slice(), simulator);
    run_agg_ast(file, "min(x_null)", get_example().as_slice(), simulator);
    run_agg_ast(file, "min(all_null)", get_example().as_slice(), simulator);
}

fn test_agg_any(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "any(1)", get_example().as_slice(), simulator);
    run_agg_ast(file, "any(NULL)", get_example().as_slice(), simulator);
    run_agg_ast(file, "any(a)", get_example().as_slice(), simulator);
    run_agg_ast(file, "any(b)", get_example().as_slice(), simulator);
    run_agg_ast(file, "any(x_null)", get_example().as_slice(), simulator);
    run_agg_ast(file, "any(y_null)", get_example().as_slice(), simulator);
    run_agg_ast(file, "any(all_null)", get_example().as_slice(), simulator);
}
