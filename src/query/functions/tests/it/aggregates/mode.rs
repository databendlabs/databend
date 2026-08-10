use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_mode_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        ("d", UInt64Type::from_data(vec![1u64, 1, 1, 1]).into()),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![110, 110, 110, 110],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        (
            "s",
            StringType::from_data(vec!["abc", "abc", "abc", "abc"]).into(),
        ),
        (
            "json",
            StringType::from_data(vec![r#"{"k":1}"#, r#"{"k":1}"#, r#"{"k":1}"#, r#"{"k":1}"#])
                .into(),
        ),
        (
            "x_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![2u64, 2, 2, 3],
                vec![true, false, true, true],
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

    write_aggregate_expr_case(file, "mode(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(d)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(parse_json(json))", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(all_null)", columns, simulator, vec![]);
}

#[test]
fn test_mode() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("mode.txt").unwrap();
    run_mode_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_mode_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("mode_group_by.txt").unwrap();
    run_mode_cases(file, simulate_two_groups_group_by);
}
