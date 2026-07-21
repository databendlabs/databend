use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::UInt64Type;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_skewness_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        ("flat", UInt64Type::from_data(vec![7, 7, 7, 7]).into()),
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

    write_aggregate_expr_case(file, "skewness(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "skewness(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "skewness(flat)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "skewness(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "skewness(x_null)", columns, simulator, vec![]);
}

#[test]
fn test_skewness() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("skewness.txt").unwrap();
    run_skewness_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_skewness_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("skewness_group_by.txt").unwrap();
    run_skewness_cases(file, simulate_two_groups_group_by);
}
