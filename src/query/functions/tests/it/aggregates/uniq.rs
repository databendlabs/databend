use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_uniq_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
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
            "c",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 1, 3])
                .into(),
        ),
        (
            "d",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 1, 1, 1])
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
        (
            "s",
            databend_common_expression::types::StringType::from_data(vec![
                "abc", "def", "opq", "xyz",
            ])
            .into(),
        ),
        (
            "s_null",
            databend_common_expression::types::StringType::from_data_with_validity(
                vec!["a", "", "c", "d"],
                vec![true, false, true, true],
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "uniq(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(c)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(s_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(a, c)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(all_null)", columns, simulator, vec![]);
}

#[test]
fn test_uniq() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("uniq.txt").unwrap();
    run_uniq_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_uniq_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("uniq_group_by.txt").unwrap();
    run_uniq_cases(file, simulate_two_groups_group_by);
}
