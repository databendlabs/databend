use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_histogram_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        ("b", UInt64Type::from_data(vec![1u64, 2, 3, 4]).into()),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![400, 300, 200, 100],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        ("s", StringType::from_data(vec!["d", "c", "b", "a"]).into()),
        ("dt", TimestampType::from_data(vec![40, 30, 20, 10]).into()),
        ("date_col", DateType::from_data(vec![4, 3, 2, 1]).into()),
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

    write_aggregate_expr_case(file, "histogram(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(a, 1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(dec, 2)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(s, 2)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(dt, 2)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(date_col, 2)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(x_null, 2)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(all_null, 2)", columns, simulator, vec![]);
}

#[test]
fn test_histogram() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("histogram.txt").unwrap();
    run_histogram_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_histogram_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("histogram_group_by.txt").unwrap();
    run_histogram_cases(file, simulate_two_groups_group_by);
}
