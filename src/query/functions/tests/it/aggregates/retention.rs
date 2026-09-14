use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_retention_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
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

    write_aggregate_expr_case(file, "retention(a > 1, b > 1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "retention(a < 0, b > 1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "retention(NULL, b > 1)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "retention(a > 1, b > 1, x_null > 1)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "retention(a > 1, b > 1, x_null > 1, all_null > 1)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "retention_if(a > 1, b > 1, a > 1)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "retention_state(a > 1, b > 1)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "retention_distinct(a > 1, b > 1)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_retention() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("retention.txt").unwrap();
    run_retention_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_retention_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("retention_group_by.txt").unwrap();
    run_retention_cases(file, simulate_two_groups_group_by);
}
