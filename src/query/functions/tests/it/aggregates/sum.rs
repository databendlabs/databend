use std::io::Write;

use databend_common_column::types::months_days_micros;
use databend_common_expression::FromData;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::IntervalType;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_sum_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "b",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 1, 3])
                .into(),
        ),
        (
            "f",
            Float64Type::from_data(vec![1.25, -2.5, 3.75, 4.5]).into(),
        ),
        (
            "dec",
            Decimal64Type::from_opt_data_with_size(
                vec![Some(110), Some(220), None, Some(330)],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        (
            "dec128",
            Decimal128Type::from_data_with_size(
                vec![123_i128, -45, 600, 22],
                Some(DecimalSize::new_unchecked(38, 2)),
            )
            .into(),
        ),
        (
            "interval_col",
            IntervalType::from_data(vec![
                months_days_micros::new(1, 2, 3),
                months_days_micros::new(0, 3, 4),
                months_days_micros::new(2, 0, 5),
                months_days_micros::new(0, 1, 6),
            ])
            .into(),
        ),
        (
            "cond",
            BooleanType::from_data(vec![true, false, true, false]).into(),
        ),
        (
            "cond_nullable",
            BooleanType::from_data_with_validity(vec![true, true, false, true], vec![
                true, false, true, true,
            ])
            .into(),
        ),
        (
            "const_int",
            databend_common_expression::BlockEntry::new_const_column_arg::<
                databend_common_expression::types::Int32Type,
            >(5, 4),
        ),
        (
            "const_int_null",
            databend_common_expression::BlockEntry::new_const_column_arg::<
                databend_common_expression::types::NullableType<
                    databend_common_expression::types::Int32Type,
                >,
            >(None, 4),
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

    write_aggregate_expr_case(file, "sum(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum(const_int)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum(const_int_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum(f)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum(dec128)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum(interval_col)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_state(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_state(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "sum_distinct(interval_col)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "sum_if(b, cond)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_if(b, cond_nullable)", columns, simulator, vec![]);
    // Do not add `sum_if(b, false)` or `sum_if(b, NULL)` to this legacy-backed
    // golden until the old row path is fixed. Legacy batch returns NULL for an
    // always-false predicate, but legacy per-row marks the nullable result flag
    // before nested `_if` rejects the row and returns nullable 0.
    write_aggregate_expr_case(file, "sum0(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum0(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum0_state(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum0(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_zero(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_zero(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum0_distinct(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "sum_zero_distinct(all_null)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_sum() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("sum.txt").unwrap();
    run_sum_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_sum_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("sum_group_by.txt").unwrap();
    run_sum_cases(file, simulate_two_groups_group_by);
}
