use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::UInt64Type;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_covar_samp_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        ("a", Int64Type::from_data(vec![4i64, 3, 2, 1]).into()),
        ("b", UInt64Type::from_data(vec![1u64, 2, 3, 4]).into()),
        (
            "f",
            Float64Type::from_data(vec![-1.0, 0.5, 2.0, 3.5]).into(),
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
        (
            "one_valid",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                true, false, false, false,
            ])
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "covar_samp(a, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "var_samp(a, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "covar_samp(f, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "covar_samp(a, NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "covar_samp(a, x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "covar_samp(x_null, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "covar_samp(a, one_valid)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "covar_samp(a, all_null)", columns, simulator, vec![]);
}

#[test]
fn test_covar_samp() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("covar_samp.txt").unwrap();
    run_covar_samp_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_covar_samp_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("covar_samp_group_by.txt").unwrap();
    run_covar_samp_cases(file, simulate_two_groups_group_by);
}
