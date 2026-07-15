use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_range_bound_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            UInt64Type::from_data(vec![9, 1, 5, 3, 7, 11, 13]).into(),
        ),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![900, 100, 500, 300, 700, 1100, 1300],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        (
            "s",
            StringType::from_data(vec!["i", "a", "e", "c", "g", "k", "m"]).into(),
        ),
        (
            "date_col",
            DateType::from_data(vec![9, 1, 5, 3, 7, 11, 13]).into(),
        ),
        (
            "x_null",
            databend_common_expression::types::UInt64Type::from_data_with_validity(
                vec![9, 1, 5, 3, 7, 11, 13],
                vec![true, true, false, true, true, false, true],
            )
            .into(),
        ),
        (
            "all_null",
            databend_common_expression::types::UInt64Type::from_data_with_validity(
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![false, false, false, false, false, false, false],
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "range_bound(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "range_bound(4)(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "range_bound(4, 10)(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "range_bound(4, 10)(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "range_bound(4, 10)(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "range_bound(4, 10)(date_col)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "range_bound(4, 10)(to_binary(s))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "range_bound(4, 10)(x_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "range_bound(4, 10)(all_null)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_range_bound() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("range_bound.txt").unwrap();
    run_range_bound_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_range_bound_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("range_bound_group_by.txt").unwrap();
    run_range_bound_cases(file, simulate_two_groups_group_by);
}
