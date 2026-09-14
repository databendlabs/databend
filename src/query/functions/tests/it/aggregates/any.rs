use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_any_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        ("a", Int64Type::from_data(vec![4i64, 3, 2, 1]).into()),
        ("b", UInt64Type::from_data(vec![1u64, 2, 3, 4]).into()),
        (
            "s",
            StringType::from_data(vec!["delta", "bravo", "charlie", "alpha"]).into(),
        ),
        (
            "event",
            BooleanType::from_data(vec![true, false, true, false]).into(),
        ),
        (
            "x_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                true, true, false, false,
            ])
            .into(),
        ),
        (
            "y_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                false, false, true, true,
            ])
            .into(),
        ),
        (
            "all_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                false, false, false, false,
            ])
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "any(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "any(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "any(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "any(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "any(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "any(event)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "any_value(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "any(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "any(y_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "any(all_null)", columns, simulator, vec![]);
}

fn run_any_distinct_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [("b", UInt64Type::from_data(vec![1u64, 2, 3, 4]).into())];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "any_distinct(b)", columns, simulator, vec![]);
}

#[test]
fn test_any() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("any.txt").unwrap();
    run_any_cases(file, eval_legacy_aggregate);
    run_any_distinct_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_any_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("any_group_by.txt").unwrap();
    run_any_cases(file, simulate_two_groups_group_by);
}
