use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_aggregate_distinct_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "c",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 1, 3])
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

    write_aggregate_expr_case(file, "sum_distinct(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(c)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg_distinct(c)", columns, simulator, vec![]);
}

#[test]
fn test_aggregate_distinct() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("aggregate_distinct.txt").unwrap();
    run_aggregate_distinct_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_aggregate_distinct_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("aggregate_distinct_group_by.txt")
        .unwrap();
    run_aggregate_distinct_cases(file, simulate_two_groups_group_by);
}
