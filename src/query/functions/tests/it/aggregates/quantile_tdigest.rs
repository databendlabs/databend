// Copyright 2026 Datafuse Labs.
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

use bumpalo::Bump;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::get_states_layout;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::number::UInt8Type;
use databend_common_expression::types::number::UInt64Type;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::aggregates::AggregateFunctionSortDesc;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::eval_legacy_aggregate_for_test;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

struct StateDropGuard {
    func: AggregateFunctionRef,
    loc: Box<[databend_common_expression::AggrStateLoc]>,
    addrs: Vec<StateAddr>,
}

impl Drop for StateDropGuard {
    fn drop(&mut self) {
        drop_guard(|| {
            if self.func.need_manual_drop_state() {
                for addr in &self.addrs {
                    unsafe {
                        self.func.drop_state(AggrState::new(*addr, &self.loc));
                    }
                }
            }
        });
    }
}

fn simulate_accumulate_matches_rows(
    name: &str,
    params: Vec<databend_common_expression::Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let factory = AggregateFunctionFactory::instance();
    let arguments = entries.iter().map(BlockEntry::data_type).collect();
    let func = factory.get(name, params.clone(), arguments, sort_descs.clone())?;
    let data_type = func.return_type()?;
    let states_layout = get_states_layout(std::slice::from_ref(&func))?;
    let loc = states_layout.states_loc[0].clone();

    let arena = Bump::new();
    let batch_addr: StateAddr = arena.alloc_layout(states_layout.layout).into();
    let rows_addr: StateAddr = arena.alloc_layout(states_layout.layout).into();
    let batch_state = AggrState::new(batch_addr, &loc);
    let rows_state = AggrState::new(rows_addr, &loc);
    func.init_state(batch_state);
    func.init_state(rows_state);
    let _drop_guard = StateDropGuard {
        func: func.clone(),
        loc: loc.clone(),
        addrs: vec![batch_addr, rows_addr],
    };

    func.accumulate(batch_state, entries.into(), None, rows)?;
    for row in 0..rows {
        func.accumulate_row(rows_state, entries.into(), row)?;
    }

    let mut batch_builder = ColumnBuilder::with_capacity(&data_type, 1);
    func.merge_result(batch_state, false, &mut batch_builder)?;
    let batch_column = batch_builder.build();

    let mut rows_builder = ColumnBuilder::with_capacity(&data_type, 1);
    func.merge_result(rows_state, false, &mut rows_builder)?;
    let rows_column = rows_builder.build();

    assert_eq!(batch_column, rows_column);

    let batch_roundtrip = eval_legacy_aggregate_for_test(
        name,
        params.clone(),
        entries,
        rows,
        false,
        true,
        sort_descs.clone(),
    )?;
    let rows_roundtrip =
        eval_legacy_aggregate_for_test(name, params, entries, rows, true, true, sort_descs)?;
    assert_eq!(batch_roundtrip.0, batch_column);
    assert_eq!(rows_roundtrip.0, rows_column);

    Ok((batch_column, data_type))
}

#[test]
fn test_quantile_tdigest_edge_cases() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("quantile_tdigest.txt").unwrap();

    test_tdigest_empty_input(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_singleton_input(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_min_max_endpoints(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_weighted_min_max_endpoints(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_weighted_interior_interpolation(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_median_names(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_singleton_boundaries(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_weighted_merged_centroid(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_weighted_zero_weight(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_weighted_tail_boundary(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_merge_empty_right(file, simulate_merge_empty_right);
    test_tdigest_weighted_nan_input(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_weighted_zero_weight_nan(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_weighted_merge_nan_only_right(file, simulate_merge_last_row_into_left);
    test_tdigest_nan_input(file, simulate_accumulate_matches_rows_and_legacy);
    test_tdigest_merge_nan_only_right(file, simulate_merge_last_row_into_left);
    test_tdigest_merge_with_uncompressed_left(file, simulate_merge_into_uncompressed_left);
    test_tdigest_group_by_nan_input(file, simulate_accumulate_keys_matches_rows_and_legacy);
    test_tdigest_weighted_group_by_nan_input(
        file,
        simulate_accumulate_keys_matches_rows_and_legacy,
    );
}

fn test_tdigest_empty_input(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [("v", Int64Type::from_data(Vec::<i64>::new()).into())];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0.5)(v)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_singleton_input(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [("v", Int64Type::from_data(vec![42_i64]).into())];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0.5)(v)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_min_max_endpoints(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let positive_values = [("v", Int64Type::from_data(vec![1_i64, 2, 3]).into())];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0, 1)(v)",
        &positive_values,
        simulator,
        vec![],
    );

    let negative_values = [("v", Int64Type::from_data(vec![-3_i64, -2, -1]).into())];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0, 1)(v)",
        &negative_values,
        simulator,
        vec![],
    );
}

fn test_tdigest_weighted_min_max_endpoints(
    file: &mut impl Write,
    simulator: impl AggregationSimulator,
) {
    let positive_values = [
        ("v", Int64Type::from_data(vec![1_i64, 5, 9]).into()),
        ("w", UInt64Type::from_data(vec![2_u64, 1, 1]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0, 1)(v, w)",
        &positive_values,
        simulator,
        vec![],
    );

    let negative_values = [
        ("v", Int64Type::from_data(vec![-9_i64, -5, -1]).into()),
        ("w", UInt64Type::from_data(vec![1_u64, 2, 1]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0, 1)(v, w)",
        &negative_values,
        simulator,
        vec![],
    );
}

fn test_tdigest_weighted_interior_interpolation(
    file: &mut impl Write,
    simulator: impl AggregationSimulator,
) {
    let columns = [
        ("v", Int64Type::from_data(vec![0_i64, 10]).into()),
        ("w", UInt64Type::from_data(vec![2_u64, 2]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.5)(v, w)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_median_names(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let values = [("v", Int64Type::from_data(vec![9_i64, 1, 5, 3, 7]).into())];
    write_aggregate_expr_case(file, "median_tdigest(v)", &values, simulator, vec![]);

    let weighted_values = [
        ("v", Int64Type::from_data(vec![9_i64, 1, 5, 3, 7]).into()),
        ("w", UInt64Type::from_data(vec![1_u64, 2, 1, 3, 1]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "median_tdigest_weighted(v, w)",
        &weighted_values,
        simulator,
        vec![],
    );
}

fn test_tdigest_singleton_boundaries(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [("v", Int64Type::from_data(vec![0_i64, 10, 20, 30]).into())];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0.4, 0.6)(v)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_weighted_merged_centroid(
    file: &mut impl Write,
    simulator: impl AggregationSimulator,
) {
    let columns = [
        (
            "v",
            Float64Type::from_data(vec![0.0_f64, 50.0, 50.1, 100.0]).into(),
        ),
        ("w", UInt64Type::from_data(vec![499_u64, 1, 1, 499]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.5)(v, w)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_weighted_zero_weight(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        ("v", Int64Type::from_data(vec![0_i64, 10]).into()),
        ("w", UInt64Type::from_data(vec![0_u64, 1]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0)(v, w)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_weighted_tail_boundary(
    file: &mut impl Write,
    simulator: impl AggregationSimulator,
) {
    let columns = [
        ("v", Int64Type::from_data(vec![0_i64, 1, 2]).into()),
        ("w", UInt64Type::from_data(vec![1_u64, 1, 2]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.75)(v, w)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_merge_empty_right(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [("v", Int64Type::from_data(vec![1_i64, 2, 3]).into())];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0.5)(v)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_weighted_nan_input(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "v",
            Float64Type::from_data(vec![1.0_f64, f64::NAN, 2.0]).into(),
        ),
        ("w", UInt64Type::from_data(vec![1_u64, 1, 1]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.5)(v, w)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_weighted_zero_weight_nan(
    file: &mut impl Write,
    simulator: impl AggregationSimulator,
) {
    let columns = [
        ("v", Float64Type::from_data(vec![f64::NAN, 10.0_f64]).into()),
        ("w", UInt64Type::from_data(vec![0_u64, 1]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0)(v, w)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_weighted_merge_nan_only_right(
    file: &mut impl Write,
    simulator: impl AggregationSimulator,
) {
    let columns = [
        ("v", Float64Type::from_data(vec![1.0_f64, f64::NAN]).into()),
        ("w", UInt64Type::from_data(vec![1_u64, 1]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.5)(v, w)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_nan_input(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [(
        "v",
        Float64Type::from_data(vec![1.0_f64, f64::NAN, 2.0]).into(),
    )];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0.5)(v)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_merge_nan_only_right(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [("v", Float64Type::from_data(vec![1.0_f64, f64::NAN]).into())];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0.5)(v)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_merge_with_uncompressed_left(
    file: &mut impl Write,
    simulator: impl AggregationSimulator,
) {
    let columns = [
        ("v", Int64Type::from_data(vec![0_i64, 100, 50]).into()),
        ("w", UInt64Type::from_data(vec![1_u64, 1, 10]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.5)(v, w)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_group_by_nan_input(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [(
        "v",
        Float64Type::from_data(vec![1.0_f64, 10.0, f64::NAN, 20.0]).into(),
    )];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0.5)(v)",
        &columns,
        simulator,
        vec![],
    );
}

fn test_tdigest_weighted_group_by_nan_input(
    file: &mut impl Write,
    simulator: impl AggregationSimulator,
) {
    let columns = [
        (
            "v",
            Float64Type::from_data(vec![1.0_f64, 10.0, f64::NAN, 20.0]).into(),
        ),
        ("w", UInt64Type::from_data(vec![1_u64, 1, 1, 1]).into()),
    ];
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.5)(v, w)",
        &columns,
        simulator,
        vec![],
    );
}

fn simulate_accumulate_keys_matches_rows(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let factory = AggregateFunctionFactory::instance();
    let arguments = entries.iter().map(BlockEntry::data_type).collect();
    let func = factory.get(name, params, arguments, sort_descs)?;
    let data_type = func.return_type()?;
    let states_layout = get_states_layout(std::slice::from_ref(&func))?;
    let loc = states_layout.states_loc[0].clone();

    let arena = Bump::new();
    let keys_left_addr: StateAddr = arena.alloc_layout(states_layout.layout).into();
    let keys_right_addr: StateAddr = arena.alloc_layout(states_layout.layout).into();
    let rows_left_addr: StateAddr = arena.alloc_layout(states_layout.layout).into();
    let rows_right_addr: StateAddr = arena.alloc_layout(states_layout.layout).into();

    let keys_left = AggrState::new(keys_left_addr, &loc);
    let keys_right = AggrState::new(keys_right_addr, &loc);
    let rows_left = AggrState::new(rows_left_addr, &loc);
    let rows_right = AggrState::new(rows_right_addr, &loc);
    for state in [keys_left, keys_right, rows_left, rows_right] {
        func.init_state(state);
    }
    let _drop_guard = StateDropGuard {
        func: func.clone(),
        loc: loc.clone(),
        addrs: vec![
            keys_left_addr,
            keys_right_addr,
            rows_left_addr,
            rows_right_addr,
        ],
    };

    let places = (0..rows)
        .map(|i| {
            if i % 2 == 0 {
                keys_left_addr
            } else {
                keys_right_addr
            }
        })
        .collect::<Vec<_>>();
    func.accumulate_keys(&places, &loc, entries.into(), rows)?;

    for row in 0..rows {
        let state = if row % 2 == 0 { rows_left } else { rows_right };
        func.accumulate_row(state, entries.into(), row)?;
    }

    let mut keys_builder = ColumnBuilder::with_capacity(&data_type, 2);
    func.merge_result(keys_left, false, &mut keys_builder)?;
    func.merge_result(keys_right, false, &mut keys_builder)?;
    let keys_column = keys_builder.build();

    let mut rows_builder = ColumnBuilder::with_capacity(&data_type, 2);
    func.merge_result(rows_left, false, &mut rows_builder)?;
    func.merge_result(rows_right, false, &mut rows_builder)?;
    let rows_column = rows_builder.build();

    assert_eq!(keys_column, rows_column);
    Ok((keys_column, data_type))
}

fn simulate_accumulate_matches_rows_and_legacy(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let legacy =
        simulate_accumulate_matches_rows(name, params.clone(), entries, rows, sort_descs.clone())?;
    let standard = eval_legacy_aggregate(name, params, entries, rows, sort_descs)?;

    assert_eq!(
        standard, legacy,
        "legacy accumulate result mismatch for {name}({args_type:?})"
    );
    Ok(legacy)
}

fn simulate_accumulate_keys_matches_rows_and_legacy(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    let legacy = simulate_accumulate_keys_matches_rows(
        name,
        params.clone(),
        entries,
        rows,
        sort_descs.clone(),
    )?;
    let standard = simulate_two_groups_group_by(name, params, entries, rows, sort_descs)?;

    assert_eq!(
        standard, legacy,
        "legacy group-by result mismatch for {name}({args_type:?})"
    );
    Ok(legacy)
}

fn simulate_merge_last_row_into_left(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    assert!(rows > 1);
    simulate_merge_split(name, params, entries, rows, sort_descs, rows - 1)
}

fn simulate_merge_empty_right(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    simulate_merge_split(name, params, entries, rows, sort_descs, rows)
}

fn simulate_merge_into_uncompressed_left(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<(Column, DataType)> {
    assert_eq!(rows, 3);
    simulate_merge_split(name, params, entries, rows, sort_descs, 2)
}

fn simulate_merge_split(
    name: &str,
    params: Vec<Scalar>,
    entries: &[BlockEntry],
    rows: usize,
    sort_descs: Vec<AggregateFunctionSortDesc>,
    right_start: usize,
) -> Result<(Column, DataType)> {
    let factory = AggregateFunctionFactory::instance();
    let arguments = entries.iter().map(BlockEntry::data_type).collect();
    let func = factory.get(name, params, arguments, sort_descs)?;
    let data_type = func.return_type()?;
    let states_layout = get_states_layout(std::slice::from_ref(&func))?;
    let loc = states_layout.states_loc[0].clone();

    let arena = Bump::new();
    let left_addr: StateAddr = arena.alloc_layout(states_layout.layout).into();
    let right_addr: StateAddr = arena.alloc_layout(states_layout.layout).into();
    let left = AggrState::new(left_addr, &loc);
    let right = AggrState::new(right_addr, &loc);
    func.init_state(left);
    func.init_state(right);
    let _drop_guard = StateDropGuard {
        func: func.clone(),
        loc: loc.clone(),
        addrs: vec![left_addr, right_addr],
    };

    for row in 0..right_start {
        func.accumulate_row(left, entries.into(), row)?;
    }
    for row in right_start..rows {
        func.accumulate_row(right, entries.into(), row)?;
    }
    func.merge_states(left, right)?;

    let mut builder = ColumnBuilder::with_capacity(&data_type, 1);
    func.merge_result(left, false, &mut builder)?;
    Ok((builder.build(), data_type))
}

fn run_quantile_tdigest_general(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "b",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 3, 4])
                .into(),
        ),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![110_i64, 220, 330, 440],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        (
            "x_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![true, true, false, false],
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "quantile_tdigest(0.8)(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0.8)(dec)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "median_tdigest(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0.8)(NULL)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "quantile_tdigest(0.8)(x_null)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_quantile_tdigest_general_cases() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("quantile_tdigest_general.txt").unwrap();
    run_quantile_tdigest_general(file, eval_legacy_aggregate);
}

#[test]
fn test_quantile_tdigest_general_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("quantile_tdigest_general_group_by.txt")
        .unwrap();
    run_quantile_tdigest_general(file, simulate_two_groups_group_by);
}

fn run_quantile_tdigest_weighted_general(
    file: &mut impl Write,
    simulator: impl AggregationSimulator,
) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "b",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 3, 4])
                .into(),
        ),
        (
            "f",
            Float64Type::from_data(vec![1.25_f64, -2.5, 3.75, 4.5]).into(),
        ),
        ("w8", UInt8Type::from_data(vec![1_u8, 2, 3, 4]).into()),
        (
            "x_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![true, true, false, false],
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.8)(a, b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0, 0.5, 1)(a, b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "median_tdigest_weighted(a, b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.8)(f, w8)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.8)(NULL, b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.8)(x_null, b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted(0.8)(a, a)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_quantile_tdigest_weighted_general_cases() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("quantile_tdigest_weighted_general.txt")
        .unwrap();
    run_quantile_tdigest_weighted_general(file, eval_legacy_aggregate);
}

#[test]
fn test_quantile_tdigest_weighted_general_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("quantile_tdigest_weighted_general_group_by.txt")
        .unwrap();
    run_quantile_tdigest_weighted_general(file, simulate_two_groups_group_by);
}
