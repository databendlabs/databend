// Copyright 2021 Datafuse Labs.
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

use super::run_agg_ast;

#[test]
fn test_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("agg.txt").unwrap();

    test_count(file);
    test_sum(file);
    test_avg(file);
    test_uniq(file);
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
            Column::from_data(vec![1u64, 2, 2, 3]),
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
    ]
}

fn test_count(file: &mut impl Write) {
    run_agg_ast(file, "count(1)", get_example().as_slice());
    run_agg_ast(file, "count()", get_example().as_slice());
    run_agg_ast(file, "count(a)", get_example().as_slice());
    run_agg_ast(file, "count(x_null)", get_example().as_slice());
}

fn test_sum(file: &mut impl Write) {
    run_agg_ast(file, "sum(1)", get_example().as_slice());
    run_agg_ast(file, "sum(a)", get_example().as_slice());
    run_agg_ast(file, "sum(x_null)", get_example().as_slice());
}

fn test_avg(file: &mut impl Write) {
    run_agg_ast(file, "avg(1)", get_example().as_slice());
    run_agg_ast(file, "avg(a)", get_example().as_slice());
    run_agg_ast(file, "avg(x_null)", get_example().as_slice());
}

fn test_uniq(file: &mut impl Write) {
    run_agg_ast(file, "uniq(1)", get_example().as_slice());
    run_agg_ast(file, "uniq(c)", get_example().as_slice());
    run_agg_ast(file, "uniq(x_null)", get_example().as_slice());
}
