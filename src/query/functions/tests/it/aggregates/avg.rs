use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int32Type;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_avg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "i32_col",
            Int32Type::from_data(vec![-10, 20, -30, 40]).into(),
        ),
        (
            "f",
            Float64Type::from_data(vec![1.25, -2.5, 3.75, 4.5]).into(),
        ),
        (
            "c",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 1, 3])
                .into(),
        ),
        (
            "dec",
            databend_common_expression::types::Decimal64Type::from_opt_data_with_size(
                vec![Some(110), Some(220), None, Some(330)],
                Some(databend_common_expression::types::DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        (
            "dec128",
            Decimal128Type::from_data_with_size(
                vec![1000_i128, 2000, 3000, 4000],
                Some(DecimalSize::new_unchecked(20, 2)),
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

    write_aggregate_expr_case(file, "avg(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg(i32_col)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg(f)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg(dec128)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg_distinct(c)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg_distinct(dec)", columns, simulator, vec![]);
}

#[test]
fn test_avg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("avg.txt").unwrap();
    run_avg_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_avg_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("avg_group_by.txt").unwrap();
    run_avg_cases(file, simulate_two_groups_group_by);
}
