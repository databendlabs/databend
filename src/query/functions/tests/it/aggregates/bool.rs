use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::Case;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_bool_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "flag",
            databend_common_expression::types::BooleanType::from_data_with_validity(
                vec![true, false, true, false],
                vec![true, false, true, false],
            )
            .into(),
        ),
        (
            "flag_mixed",
            databend_common_expression::types::BooleanType::from_data(vec![
                true, false, true, false,
            ])
            .into(),
        ),
        (
            "all_null",
            databend_common_expression::types::BooleanType::from_data_with_validity(
                vec![true, false, true, false],
                vec![false, false, false, false],
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "bool_and(flag)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "bool_or(flag)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "bool_and(flag_mixed)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "bool_or(flag_mixed)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "bool_and(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "bool_or(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "bool_and_distinct(flag)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "bool_or_distinct(flag)", columns, simulator, vec![]);
}

#[test]
fn test_bool() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("bool.txt").unwrap();
    run_bool_cases(file, eval_aggregate);
}

#[test]
fn test_bool_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("bool_group_by.txt").unwrap();
    run_bool_cases(file, simulate_two_groups_group_by);
}

#[test]
fn test_state_baselines() {
    // boolean.rs: AND/OR kernels and nullable input handling; no type expansion.
    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "bool_and(x0)",
            arguments: vec!["Boolean"],
            result: "Nullable(Boolean)",
            state: "Tuple(Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "bool_and(x0)",
            arguments: vec!["Nullable(Boolean)"],
            result: "Nullable(Boolean)",
            state: "Tuple(Boolean, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "bool_or(x0)",
            arguments: vec!["Boolean"],
            result: "Nullable(Boolean)",
            state: "Tuple(Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "bool_or(x0)",
            arguments: vec!["Nullable(Boolean)"],
            result: "Nullable(Boolean)",
            state: "Tuple(Boolean, Boolean, Boolean)",
        },
    ]);
}
