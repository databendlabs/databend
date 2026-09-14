use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_max_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        ("a", Int64Type::from_data(vec![4i64, 3, 2, 1]).into()),
        ("b", UInt64Type::from_data(vec![1u64, 2, 3, 4]).into()),
        (
            "s",
            StringType::from_data(vec!["delta", "bravo", "charlie", "alpha"]).into(),
        ),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![400_i64, 300, 200, 100],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
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
            "all_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                false, false, false, false,
            ])
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "max(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "max(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "max(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "max(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "max(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "max(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "max(event)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "max(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "max(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "max_distinct(b)", columns, simulator, vec![]);
}

#[test]
fn test_max() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("max.txt").unwrap();
    run_max_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_max_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("max_group_by.txt").unwrap();
    run_max_cases(file, simulate_two_groups_group_by);
}
