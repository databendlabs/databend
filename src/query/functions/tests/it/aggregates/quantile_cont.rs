use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_quantile_cont_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        ("a", Int64Type::from_data(vec![4i64, 3, 2, 1]).into()),
        (
            "x_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                true, true, false, false,
            ])
            .into(),
        ),
        (
            "x_all_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                false, false, false, false,
            ])
            .into(),
        ),
        (
            "d",
            Decimal64Type::from_data_with_size(
                vec![400_i64, 300, 200, 100],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "quantile_cont(0.8)(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "quantile_cont(0.0, 0.5, 1.0)(a)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "quantile_cont(0.8)(d)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "quantile_cont(0.8)(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "quantile_cont(0.8)(x_all_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "quantile_cont(0.0, 0.5, 1.0)(x_all_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "quantile_cont(0.8)(x_null)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_quantile_cont() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("quantile_cont.txt").unwrap();
    run_quantile_cont_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_quantile_cont_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("quantile_cont_group_by.txt").unwrap();
    run_quantile_cont_cases(file, simulate_two_groups_group_by);
}
