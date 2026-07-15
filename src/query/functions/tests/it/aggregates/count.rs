use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate_without_each_row;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_count_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "const_int",
            databend_common_expression::BlockEntry::new_const_column_arg::<
                databend_common_expression::types::Int32Type,
            >(5, 4),
        ),
        (
            "const_int_null",
            databend_common_expression::BlockEntry::new_const_column_arg::<
                databend_common_expression::types::NullableType<
                    databend_common_expression::types::Int32Type,
                >,
            >(None, 4),
        ),
        (
            "x_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![true, true, false, false],
            )
            .into(),
        ),
        (
            "all_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![false, false, false, false],
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "count(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count(const_int)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count(const_int_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count()", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count_state(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count_state(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum0(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "count(all_null)", columns, simulator, vec![]);
}

#[test]
fn test_count() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("count.txt").unwrap();
    run_count_cases(file, eval_legacy_aggregate_without_each_row);
}

#[test]
fn test_count_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("count_group_by.txt").unwrap();
    run_count_cases(file, simulate_two_groups_group_by);
}
