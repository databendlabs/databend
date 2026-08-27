use std::io::Write;

use databend_common_expression::BlockEntry;
use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_min_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        ("a", Int64Type::from_data(vec![4i64, 3, 2, 1]).into()),
        (
            "const_int",
            BlockEntry::new_const_column_arg::<NumberType<i32>>(5, 4),
        ),
        (
            "const_int_null",
            BlockEntry::new_const_column_arg::<NullableType<NumberType<i32>>>(None, 4),
        ),
        ("b", UInt64Type::from_data(vec![1u64, 2, 3, 4]).into()),
        (
            "s",
            StringType::from_data(vec!["delta", "bravo", "charlie", "alpha"]).into(),
        ),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![400_i64, 300, 200, 100],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        (
            "event",
            BooleanType::from_data(vec![true, false, true, false]).into(),
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
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "min(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min(const_int)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min(const_int_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min(event)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "min_distinct(b)", columns, simulator, vec![]);
}

#[test]
fn test_min() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("min.txt").unwrap();
    run_min_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_min_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("min_group_by.txt").unwrap();
    run_min_cases(file, simulate_two_groups_group_by);
}
