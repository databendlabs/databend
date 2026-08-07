use std::io::Write;

use databend_common_column::types::months_days_micros;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt64Type;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_function_v2_support::assert_v2_matches_legacy;
use super::aggregate_function_v2_support::assert_v2_read_only_matches_legacy;
use super::aggregate_function_v2_support::assert_v2_serialized_read_only_matches_legacy;
use super::aggregate_function_v2_support::eval_v2_aggr;
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
            "i32_col",
            Int32Type::from_data(vec![-10, 20, -30, 40]).into(),
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
    write_aggregate_expr_case(file, "sum(i32_col)", columns, simulator, vec![]);
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
    write_aggregate_expr_case(file, "sum_distinct(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "sum_distinct(interval_col)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "sum_if(b, cond)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_if(b, cond_nullable)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "sum_if(x_null, cond_nullable)",
        columns,
        simulator,
        vec![],
    );
    // Do not add `sum_if(b, false)` or `sum_if(b, NULL)` to this legacy-backed
    // golden until the old row path is fixed. Legacy batch returns NULL for an
    // always-false predicate, but legacy per-row marks the nullable result flag
    // before nested `_if` rejects the row and returns nullable 0.
    write_aggregate_expr_case(file, "sum0(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum0(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum0_state(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_zero_state(x_null)", columns, simulator, vec![]);
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

#[test]
fn test_v2_sum_zero_uint64_matches_expected_sum() -> Result<()> {
    let entries = [UInt64Type::from_data(vec![1, 2, 3, 4]).into()];
    let direct_v2 = eval_v2_aggr("sum0", &entries, 4, false)?;
    let serialized_v2 = eval_v2_aggr("sum0", &entries, 4, true)?;

    assert_eq!(
        unsafe { direct_v2.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::UInt64(10))
    );
    assert_eq!(serialized_v2, direct_v2);
    Ok(())
}

#[test]
fn test_v2_sum_if_null_condition_matches_legacy_sum_if() -> Result<()> {
    let entries = [
        UInt64Type::from_data(vec![10, 20, 30]).into(),
        BlockEntry::new_const_column(DataType::Null, Scalar::Null, 3),
    ];
    assert_v2_matches_legacy("sum_if", &entries, 3)
}

#[test]
fn test_v2_sum_if_always_false_returns_null() -> Result<()> {
    let entries = [
        UInt64Type::from_data(vec![10, 20, 30]).into(),
        BooleanType::from_data(vec![false, false, false]).into(),
    ];
    let expected = (
        UInt64Type::from_opt_data(vec![None]),
        DataType::Nullable(Box::new(UInt64Type::data_type())),
    );

    let direct_v2 = eval_v2_aggr("sum_if", &entries, 3, false)?;
    let serialized_v2 = eval_v2_aggr("sum_if", &entries, 3, true)?;
    assert_eq!(direct_v2, expected);
    assert_eq!(serialized_v2, expected);
    Ok(())
}

#[test]
fn test_v2_sum_if_suffix_names_are_case_insensitive() -> Result<()> {
    let values = NullableColumn::new_column(
        UInt64Type::from_data(vec![10, 20, 10, 40, 20]),
        Bitmap::from([true, false, true, true, true]),
    );
    let conditions = BooleanType::from_data(vec![true, false, true, true, false]);
    let entries = [values.into(), conditions.into()];

    assert_v2_matches_legacy("SUM_IF", &entries, 5)
}

#[test]
fn test_v2_sum_suffix_names_are_case_insensitive() -> Result<()> {
    let values = NullableColumn::new_column(
        UInt64Type::from_data(vec![10, 20, 10, 40, 20]),
        Bitmap::from([true, false, true, true, true]),
    );
    let entries = [values.into()];

    assert_v2_matches_legacy("Sum_State", &entries, 5)
}

#[test]
fn test_v2_sum_decorator_read_only_finalization_matches_legacy() -> Result<()> {
    let values = NullableColumn::new_column(
        UInt64Type::from_data(vec![10, 20, 10, 40, 20]),
        Bitmap::from([true, false, true, true, true]),
    );
    let values: BlockEntry = values.into();
    let if_values: BlockEntry = UInt64Type::from_data(vec![10, 20, 10, 40, 20]).into();
    let conditions: BlockEntry =
        BooleanType::from_data(vec![true, false, true, true, false]).into();
    let if_entries = [if_values, conditions];

    assert_v2_read_only_matches_legacy("sum_if", vec![], &if_entries, 5)?;
    assert_v2_read_only_matches_legacy("sum_distinct", vec![], std::slice::from_ref(&values), 5)?;
    assert_v2_read_only_matches_legacy("sum_state", vec![], std::slice::from_ref(&values), 5)?;
    assert_v2_serialized_read_only_matches_legacy("sum_if", vec![], &if_entries, 5)?;
    assert_v2_serialized_read_only_matches_legacy(
        "sum_distinct",
        vec![],
        std::slice::from_ref(&values),
        5,
    )
}
