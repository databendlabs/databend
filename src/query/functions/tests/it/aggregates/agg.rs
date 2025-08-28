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

use databend_common_exception::Result;
use databend_common_expression::types::number::Int64Type;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::BitmapType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_functions::aggregates::eval_aggr_for_test;
use databend_common_functions::aggregates::AggregateFunctionSortDesc;
use goldenfile::Mint;
use itertools::Itertools;
use roaring::RoaringTreemap;

use super::run_agg_ast;
use super::simulate_two_groups_group_by;
use super::AggregationSimulator;

fn eval_aggr(
    name: &str,
    params: Vec<databend_common_expression::Scalar>,
    columns: &[Column],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let block_entries: Vec<BlockEntry> = columns.iter().map(|c| c.clone().into()).collect();
    eval_aggr_for_test(name, params, &block_entries, rows, true, sort_descs)
}

#[test]
fn test_aggr_funtions() {
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
    test_agg_arg_min(file, eval_aggr);
    test_agg_arg_max(file, eval_aggr);
    test_agg_covar_samp(file, eval_aggr);
    test_agg_covar_pop(file, eval_aggr);
    test_agg_retention(file, eval_aggr);
    test_agg_stddev(file, eval_aggr);
    test_agg_kurtosis(file, eval_aggr);
    test_agg_skewness(file, eval_aggr);
    test_agg_window_funnel(file, eval_aggr);
    test_agg_approx_count_distinct(file, eval_aggr);
    test_agg_quantile_disc(file, eval_aggr);
    test_agg_quantile_cont(file, eval_aggr);
    test_agg_quantile_tdigest(file, eval_aggr);
    // FIXME
    test_agg_quantile_tdigest_weighted(file, |name, params, columns, rows, _sort_descs| {
        let block_entries: Vec<BlockEntry> = columns.iter().map(|c| c.clone().into()).collect();
        eval_aggr_for_test(name, params, &block_entries, rows, false, vec![])
    });
    test_agg_median(file, eval_aggr);
    test_agg_median_tdigest(file, eval_aggr);
    test_agg_array_agg(file, eval_aggr);
    test_agg_string_agg(file, eval_aggr);
    test_agg_list_agg(file, eval_aggr);
    test_agg_bitmap_count(file, eval_aggr);
    test_agg_bitmap(file, eval_aggr);
    test_agg_group_array_moving_avg(file, eval_aggr);
    test_agg_group_array_moving_sum(file, eval_aggr);
    test_agg_histogram(file, eval_aggr);
    test_agg_json_agg(file, eval_aggr);
    test_agg_json_array_agg(file, eval_aggr);
    test_agg_json_object_agg(file, eval_aggr);
    test_agg_mode(file, eval_aggr);
    test_agg_st_collect(file, eval_aggr);
}

#[test]
fn test_aggr_functions_group_by() {
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
    test_agg_arg_min(file, simulate_two_groups_group_by);
    test_agg_arg_max(file, simulate_two_groups_group_by);
    test_agg_covar_samp(file, simulate_two_groups_group_by);
    test_agg_covar_pop(file, simulate_two_groups_group_by);
    test_agg_retention(file, simulate_two_groups_group_by);
    test_agg_stddev(file, simulate_two_groups_group_by);
    test_agg_kurtosis(file, simulate_two_groups_group_by);
    test_agg_skewness(file, simulate_two_groups_group_by);
    test_agg_quantile_disc(file, simulate_two_groups_group_by);
    test_agg_quantile_cont(file, simulate_two_groups_group_by);
    test_agg_quantile_tdigest(file, simulate_two_groups_group_by);
    test_agg_median(file, simulate_two_groups_group_by);
    test_agg_median_tdigest(file, simulate_two_groups_group_by);
    test_agg_window_funnel(file, simulate_two_groups_group_by);
    test_agg_approx_count_distinct(file, simulate_two_groups_group_by);
    test_agg_array_agg(file, simulate_two_groups_group_by);
    test_agg_string_agg(file, simulate_two_groups_group_by);
    test_agg_list_agg(file, simulate_two_groups_group_by);
    test_agg_bitmap_count(file, simulate_two_groups_group_by);
    test_agg_bitmap(file, simulate_two_groups_group_by);
    test_agg_group_array_moving_avg(file, eval_aggr);
    test_agg_group_array_moving_sum(file, eval_aggr);
    test_agg_json_agg(file, eval_aggr);
    test_agg_json_array_agg(file, eval_aggr);
    test_agg_json_object_agg(file, eval_aggr);
    test_agg_mode(file, simulate_two_groups_group_by);
    test_agg_st_collect(file, eval_aggr);
}

fn gen_bitmap_data() -> Column {
    // construct bitmap column with 4 row:
    // 0..5, 1..6, 2..7, 3..8
    const N: u64 = 4;
    let rbs_iter = (0..N).map(|i| {
        let mut rb = RoaringTreemap::new();
        rb.insert_range(i..(i + 5));
        rb
    });

    let rbs = rbs_iter
        .map(|rb| {
            let mut data = Vec::new();
            rb.serialize_into(&mut data).unwrap();
            data
        })
        .collect_vec();

    BitmapType::from_data(rbs)
}

fn get_example() -> Vec<(&'static str, Column)> {
    vec![
        ("a", Int64Type::from_data(vec![4i64, 3, 2, 1])),
        ("b", UInt64Type::from_data(vec![1u64, 2, 3, 4])),
        ("c", UInt64Type::from_data(vec![1u64, 2, 1, 3])),
        ("d", UInt64Type::from_data(vec![1u64, 1, 1, 1])),
        (
            "x_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                true, true, false, false,
            ]),
        ),
        (
            "y_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                false, false, true, true,
            ]),
        ),
        (
            "all_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                false, false, false, false,
            ]),
        ),
        ("dt", TimestampType::from_data(vec![1i64, 0, 2, 3])),
        (
            "event1",
            BooleanType::from_data(vec![true, false, false, false]),
        ),
        (
            "event2",
            BooleanType::from_data(vec![false, false, false, false]),
        ),
        (
            "event3",
            BooleanType::from_data(vec![false, false, false, false]),
        ),
        ("s", StringType::from_data(vec!["abc", "def", "opq", "xyz"])),
        (
            "s_null",
            StringType::from_data_with_validity(vec!["a", "", "c", "d"], vec![
                true, false, true, true,
            ]),
        ),
        ("bm", gen_bitmap_data()),
        (
            "dec",
            Decimal64Type::from_opt_data_with_size(
                vec![Some(110), Some(220), None, Some(330)],
                Some(DecimalSize::new_unchecked(15, 2)),
            ),
        ),
    ]
}

fn get_geometry_example() -> Vec<(&'static str, Column)> {
    vec![
        (
            "point",
            StringType::from_data(vec!["POINT(1 1)", "POINT(2 2)", "POINT(3 3)", "POINT(4 4)"]),
        ),
        (
            "point_null",
            StringType::from_data_with_validity(
                vec!["POINT(1 1)", "", "POINT(3 3)", "POINT(4 4)"],
                vec![true, false, true, true],
            ),
        ),
        (
            "line_string",
            StringType::from_data(vec![
                "LINESTRING(0 0, 1 1)",
                "LINESTRING(1 1, 2 2)",
                "LINESTRING(2 2, 3 3)",
                "LINESTRING(3 3, 4 4)",
            ]),
        ),
        (
            "line_string_null",
            StringType::from_data_with_validity(
                vec![
                    "LINESTRING(0 0, 1 1)",
                    "",
                    "LINESTRING(2 2, 3 3)",
                    "LINESTRING(3 3, 4 4)",
                ],
                vec![true, false, true, true],
            ),
        ),
        (
            "polygon",
            StringType::from_data(vec![
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))",
                "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))",
                "POLYGON((3 3, 4 3, 4 4, 3 4, 3 3))",
            ]),
        ),
        (
            "mixed_geom",
            StringType::from_data(vec![
                "POINT(0 0)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))",
                "POINT(4 4)",
            ]),
        ),
        (
            "mixed_geom_null",
            StringType::from_data_with_validity(
                vec![
                    "POINT(0 0)",
                    "",
                    "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))",
                    "POINT(4 4)",
                ],
                vec![true, false, true, true],
            ),
        ),
        (
            "point_4326",
            StringType::from_data(vec![
                "SRID=4326;POINT(116.3 39.9)",
                "SRID=4326;POINT(121.4 31.2)",
                "SRID=4326;POINT(113.2 23.1)",
                "SRID=4326;POINT(114.1 22.5)",
            ]),
        ),
        (
            "line_string_4326",
            StringType::from_data(vec![
                "SRID=4326;LINESTRING(116.3 39.9, 121.4 31.2)",
                "SRID=4326;LINESTRING(121.4 31.2, 113.2 23.1)",
                "SRID=4326;LINESTRING(113.2 23.1, 114.1 22.5)",
                "SRID=4326;LINESTRING(114.1 22.5, 116.3 39.9)",
            ]),
        ),
        (
            "polygon_4326",
            StringType::from_data(vec![
                "SRID=4326;POLYGON((116 39, 117 39, 117 40, 116 40, 116 39))",
                "SRID=4326;POLYGON((121 31, 122 31, 122 32, 121 32, 121 31))",
                "SRID=4326;POLYGON((113 23, 114 23, 114 24, 113 24, 113 23))",
                "SRID=4326;POLYGON((114 22, 115 22, 115 23, 114 23, 114 22))",
            ]),
        ),
        (
            "mixed_3857",
            StringType::from_data(vec![
                "SRID=3857;POINT(12947889.3 4852834.1)",
                "SRID=3857;LINESTRING(13515330.8 3642091.4, 12600089.2 2632873.5)",
                "SRID=3857;POLYGON((12700000 2600000, 12800000 2600000, 12800000 2700000, 12700000 2700000, 12700000 2600000))",
                "SRID=3857;POINT(12959772.9 2551529.8)",
            ]),
        ),
        (
            "mixed_srid",
            StringType::from_data(vec![
                "SRID=4326;POINT(116.3 39.9)",
                "SRID=3857;POINT(12947889.3 4852834.1)",
                "SRID=4326;LINESTRING(121.4 31.2, 113.2 23.1)",
                "SRID=3857;POLYGON((12700000 2600000, 12800000 2600000, 12800000 2700000, 12700000 2700000, 12700000 2600000))",
            ]),
        ),
        (
            "mixed_srid_null",
            StringType::from_data_with_validity(
                vec![
                    "SRID=4326;POINT(116.3 39.9)",
                    "",
                    "SRID=4326;LINESTRING(121.4 31.2, 113.2 23.1)",
                    "SRID=3857;POLYGON((12700000 2600000, 12800000 2600000, 12800000 2700000, 12700000 2700000, 12700000 2600000))",
                ],
                vec![true, false, true, true],
            ),
        ),
    ]
}

fn test_count(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "count(1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(file, "count()", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "count(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "count(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "sum0(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "count(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_sum(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "sum(1)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(file, "sum(a)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "sum(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "sum(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_avg(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "avg(1)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(file, "avg(a)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "avg(dec)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "avg(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "avg(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_uniq(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "uniq(1)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(file, "uniq(c)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "uniq(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "uniq(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_if(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "count_if(1, x_null is null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "sum_if(a, x_null is null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "sum_if(b, x_null is null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_distinct(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "sum_distinct(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "sum_distinct(c)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "sum_distinct(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "sum_distinct(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_max(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "max(1)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "max(NULL)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(file, "max(a)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(file, "max(b)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "max(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "max(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_min(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "min(1)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "min(NULL)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(file, "min(a)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(file, "min(b)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "min(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "min(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_any(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "any(1)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "any(NULL)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(file, "any(a)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(file, "any(b)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "any(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "any(y_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "any(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_arg_min(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "arg_min(a, b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "arg_min(b, a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "arg_min(y_null, a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "arg_min(a, y_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "arg_min(all_null, a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "arg_min(a, all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_arg_max(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "arg_max(a, b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "arg_max(b, a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "arg_max(y_null, a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "arg_max(a, y_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "arg_max(all_null, a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "arg_max(a, all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_covar_samp(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "covar_samp(a, b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "covar_samp(a, x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "covar_samp(a, all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_covar_pop(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "covar_pop(a, b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "covar_pop(a, x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "covar_pop(a, all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_retention(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "retention(a > 1, b > 1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "retention(a > 1, b > 1, x_null > 1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "retention(a > 1, b > 1, x_null > 1, all_null > 1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_stddev(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "stddev_pop(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "stddev(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );

    run_agg_ast(
        file,
        "stddev_samp(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "stddev_samp(dec)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "stddev_samp(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_kurtosis(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "kurtosis(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "kurtosis(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_skewness(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "skewness(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "skewness(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_quantile_disc(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "quantile_cont(0.8)(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "quantile_cont(0.8)(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_quantile_cont(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "quantile_cont(0.8)(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "quantile_cont(0.8)(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_median(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "median(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "median(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_median_tdigest(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "median_tdigest(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "median_tdigest(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_window_funnel(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "window_funnel(2)(dt, event1, event2, event3)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_approx_count_distinct(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "approx_count_distinct(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "approx_count_distinct(b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "approx_count_distinct(null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_array_agg(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "array_agg(1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "array_agg('a')",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "array_agg(NULL)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "array_agg(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "array_agg(b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "array_agg(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "array_agg(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "array_agg(dt)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "array_agg(event1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "array_agg(dec)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_string_agg(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "string_agg(s)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "string_agg(s_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "string_agg(s, '|')",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "string_agg(s_null, '-')",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_list_agg(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "listagg(s)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "listagg(s_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "listagg(s, '|')",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "listagg(s_null, '-')",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_bitmap_count(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "bitmap_and_count(bm)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "bitmap_or_count(bm)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "bitmap_xor_count(bm)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "bitmap_not_count(bm)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "intersect_count(1, 2, 3, 4)(bm, b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "intersect_count(1, 2)(bm, b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_bitmap(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "bitmap_union(bm)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "bitmap_intersect(bm)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_quantile_tdigest(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "quantile_tdigest(0.8)(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "quantile_tdigest(0.8)(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_quantile_tdigest_weighted(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "quantile_tdigest_weighted(0.8)(a, b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "quantile_tdigest_weighted(0.8)(x_null, b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_group_array_moving_avg(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "group_array_moving_avg(1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_avg('a')",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_avg(NULL)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_avg(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_avg(2)(b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_avg(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_avg(1)(y_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_avg(dec)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_avg(2)(dec)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_group_array_moving_sum(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "group_array_moving_sum(1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_sum('a')",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_sum(NULL)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_sum(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_sum(2)(b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_sum(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_sum(1)(y_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_sum(dec)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "group_array_moving_sum(2)(dec)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_histogram(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "histogram(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "histogram(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "histogram(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "histogram(a, 1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_json_agg(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "json_agg(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_agg(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_agg(dec)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_json_array_agg(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "json_array_agg(1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_array_agg('a')",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_array_agg(NULL)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_array_agg(a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_array_agg(b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_array_agg(x_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_array_agg(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_array_agg(dt)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_array_agg(event1)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_array_agg(dec)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_json_object_agg(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "json_object_agg('k', 'a')",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_object_agg(s, a)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_object_agg(s_null, b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_object_agg(a, b)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "json_object_agg(s, dec)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_mode(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(file, "mode(1)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "mode(NULL)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(file, "mode(d)", get_example().as_slice(), simulator, vec![]);
    run_agg_ast(
        file,
        "mode(all_null)",
        get_example().as_slice(),
        simulator,
        vec![],
    );
}

fn test_agg_st_collect(file: &mut impl Write, simulator: impl AggregationSimulator) {
    run_agg_ast(
        file,
        "st_collect(to_geometry('point(10 20)'))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry('srid=4326;linestring(10 20, 40 50)'))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(NULL)",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );

    run_agg_ast(
        file,
        "st_collect(to_geometry(point))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(point_null))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(line_string))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(line_string_null))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(polygon))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(mixed_geom))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(mixed_geom_null))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(point_4326))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(line_string_4326))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(polygon_4326))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(mixed_3857))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(mixed_srid))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
    run_agg_ast(
        file,
        "st_collect(to_geometry(mixed_srid_null))",
        get_geometry_example().as_slice(),
        simulator,
        vec![],
    );
}
