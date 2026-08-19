use std::io::Write;

use databend_common_column::types::months_days_micros;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::aggregate::aggregate_function_v2 as v2;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::aggregates::aggregate_function_v2_registry::AGGR_REGISTRY;
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

fn eval_v2_state_merge(
    name: &str,
    params: &[Scalar],
    state: &(Column, DataType),
) -> Result<(Column, DataType)> {
    eval_v2_state_merge_entry(name, params, &state.1, BlockEntry::from(state.0.clone()))
}

fn eval_v2_state_merge_entry(
    name: &str,
    params: &[Scalar],
    state_type: &DataType,
    state: BlockEntry,
) -> Result<(Column, DataType)> {
    let args_type = [state_type.clone()];
    let function = AGGR_REGISTRY.resolve(v2::AggregateFunctionRequest {
        name,
        params,
        args_type: &args_type,
        distinct: false,
        order_by: &[],
    })?;
    let return_type = function.signature().return_type.clone();
    let owner = v2::AggregateStateOwner::new(vec![function.clone()])?;
    let entries = [state];
    function.accumulate(v2::AccumulateInput {
        state: owner.state(0),
        columns: (&entries).into(),
        validity: None,
        order_by: &[],
    })?;

    let mut builder = ColumnBuilder::with_capacity(&return_type, 1);
    function.merge_result(v2::MergeResultInput {
        state: owner.state(0),
        builder: &mut builder,
    })?;
    Ok((builder.build(), return_type))
}

#[test]
fn test_v2_sum_merge_and_merge_state() -> Result<()> {
    let entries = [UInt64Type::from_data(vec![1, 2, 3, 4]).into()];
    let state = eval_v2_aggr("sum_state", &entries, 4, false)?;
    assert!(matches!(state.1, DataType::AggregateState(_)));

    let merged = eval_v2_state_merge("sum_merge", &[], &state)?;
    assert_eq!(
        unsafe { merged.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::UInt64(10))
    );

    let merged_state = eval_v2_state_merge("sum_merge_state", &[], &state)?;
    assert_eq!(merged_state.1, state.1);
    let merged_again = eval_v2_state_merge("sum_merge", &[], &merged_state)?;
    assert_eq!(merged_again, merged);

    let DataType::AggregateState(state_metadata) = &state.1 else {
        unreachable!()
    };
    let physical_state = (state.0.clone(), state_metadata.physical_type().clone());
    let merged_legacy_state = eval_v2_state_merge("sum_merge", &[], &physical_state)?;
    assert_eq!(merged_legacy_state, merged);
    Ok(())
}

#[test]
fn test_v2_sum_merge_null_state_and_const_state() -> Result<()> {
    let null_entries = [BlockEntry::new_const_column(
        DataType::Null,
        Scalar::Null,
        4,
    )];
    let null_state = eval_v2_aggr("sum_state", &null_entries, 4, false)?;
    let merged_null_state = eval_v2_state_merge("sum_merge_state", &[], &null_state)?;
    assert_eq!(merged_null_state.1, null_state.1);
    let merged_null = eval_v2_state_merge("sum_merge", &[], &null_state)?;
    assert_eq!(unsafe { merged_null.0.index_unchecked(0) }, ScalarRef::Null);

    let DataType::AggregateState(state_metadata) = &null_state.1 else {
        unreachable!()
    };
    assert_eq!(state_metadata.physical_type(), &null_state.0.data_type());

    let sum0_state = eval_v2_aggr("sum0_state", &null_entries, 4, false)?;
    let merged_sum0 = eval_v2_state_merge("sum0_merge", &[], &sum0_state)?;
    assert_eq!(
        unsafe { merged_sum0.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::UInt64(0))
    );

    let entries = [UInt64Type::from_data(vec![1, 2, 3, 4]).into()];
    let state = eval_v2_aggr("sum_state", &entries, 4, false)?;
    let state_scalar = unsafe { state.0.index_unchecked(0) }.to_owned();
    let const_state = BlockEntry::new_const_column(state.1.clone(), state_scalar, 2);
    let merged = eval_v2_state_merge_entry("sum_merge", &[], &state.1, const_state)?;
    assert_eq!(
        unsafe { merged.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::UInt64(20))
    );
    Ok(())
}

#[test]
fn test_v2_sum_merge_keys_skips_nullable_states() -> Result<()> {
    let left = eval_v2_aggr(
        "sum_state",
        &[UInt64Type::from_data(vec![1, 2]).into()],
        2,
        false,
    )?;
    let right = eval_v2_aggr(
        "sum_state",
        &[UInt64Type::from_data(vec![3, 4]).into()],
        2,
        false,
    )?;
    assert_eq!(left.1, right.1);

    let mut state_builder = ColumnBuilder::with_capacity(&left.1, 2);
    state_builder.push(unsafe { left.0.index_unchecked(0) });
    state_builder.push(unsafe { right.0.index_unchecked(0) });
    let states = state_builder
        .build()
        .wrap_nullable(Some(Bitmap::from([true, false])));

    let nullable_state_type = DataType::Nullable(Box::new(left.1));
    let function = AGGR_REGISTRY.resolve(v2::AggregateFunctionRequest {
        name: "sum_merge",
        params: &[],
        args_type: &[nullable_state_type],
        distinct: false,
        order_by: &[],
    })?;
    let first = v2::AggregateStateOwner::new(vec![function.clone()])?;
    let second = v2::AggregateStateOwner::new(vec![function.clone()])?;
    let places = [first.state(0).addr, second.state(0).addr];
    let entries = [BlockEntry::from(states)];
    function.accumulate_keys(v2::AccumulateKeysInput {
        states: v2::AggregateStateSet::new(&places, first.state(0).loc),
        columns: (&entries).into(),
        order_by: &[],
    })?;

    for (owner, expected) in [(&first, Some(3)), (&second, None)] {
        let mut builder = ColumnBuilder::with_capacity(&function.signature().return_type, 1);
        function.merge_result(v2::MergeResultInput {
            state: owner.state(0),
            builder: &mut builder,
        })?;
        let result = builder.build();
        let expected = expected.map_or(ScalarRef::Null, |value| {
            ScalarRef::Number(NumberScalar::UInt64(value))
        });
        assert_eq!(unsafe { result.index_unchecked(0) }, expected);
    }
    Ok(())
}

#[test]
fn test_v2_merge_rejects_mismatched_state_function() -> Result<()> {
    let entries = [UInt64Type::from_data(vec![1, 2, 3, 4]).into()];
    let (_, state_type) = eval_v2_aggr("sum_state", &entries, 4, false)?;
    let error = match AGGR_REGISTRY.resolve(v2::AggregateFunctionRequest {
        name: "avg_merge",
        params: &[],
        args_type: &[state_type],
        distinct: false,
        order_by: &[],
    }) {
        Ok(_) => panic!("avg_merge should reject sum state metadata"),
        Err(error) => error,
    };
    assert_eq!(error.code(), 1010);
    assert!(error.message().contains("cannot be merged"));
    Ok(())
}

/// Legacy states carry no metadata. Migrated aggregates recover a short list of
/// concrete signatures from their own state layout, while the merge layer still
/// verifies that rebuilding the signature reproduces that layout.
#[test]
fn test_v2_merge_resolves_legacy_physical_state() -> Result<()> {
    for (case, state_name, merge_name, entry) in [
        (
            "unsigned sum",
            "sum_state",
            "sum_merge",
            UInt64Type::from_data(vec![1, 2, 3, 4]).into(),
        ),
        (
            "signed sum",
            "sum_state",
            "sum_merge",
            Int64Type::from_data(vec![-4, 3, -2, 1]).into(),
        ),
        (
            "float sum",
            "sum_state",
            "sum_merge",
            Float64Type::from_data(vec![1.25, -2.5, 3.75, 4.5]).into(),
        ),
        (
            "interval sum",
            "sum_state",
            "sum_merge",
            IntervalType::from_data(vec![
                months_days_micros::new(1, 2, 3),
                months_days_micros::new(0, 3, 4),
                months_days_micros::new(2, 0, 5),
                months_days_micros::new(0, 1, 6),
            ])
            .into(),
        ),
        (
            "sum merge state",
            "sum_state",
            "sum_merge_state",
            UInt64Type::from_data(vec![1, 2, 3, 4]).into(),
        ),
        (
            "count",
            "count_state",
            "count_merge",
            UInt64Type::from_data(vec![1, 2, 3, 4]).into(),
        ),
        (
            "max string",
            "max_state",
            "max_merge",
            StringType::from_data(vec!["delta", "bravo", "charlie", "alpha"]).into(),
        ),
        (
            "min string",
            "min_state",
            "min_merge",
            StringType::from_data(vec!["delta", "bravo", "charlie", "alpha"]).into(),
        ),
    ] {
        let entries = [entry];
        let (state_column, state_type) = eval_v2_aggr(state_name, &entries, 4, false)?;
        let DataType::AggregateState(metadata) = &state_type else {
            panic!("{state_name} should return an AggregateState type");
        };

        // Strip the metadata to emulate a pre-v182 column, then merge it.
        let physical_type = metadata.physical_type().clone();
        let merged = eval_v2_state_merge_entry(
            merge_name,
            &[],
            &physical_type,
            BlockEntry::from(state_column.clone()),
        )?;
        // And confirm the metadata-carrying column resolves to the same result.
        let merged_with_metadata = eval_v2_state_merge_entry(
            merge_name,
            &[],
            &state_type,
            BlockEntry::from(state_column),
        )?;
        assert_eq!(
            unsafe { merged.0.index_unchecked(0) },
            unsafe { merged_with_metadata.0.index_unchecked(0) },
            "{case}: {merge_name} disagreed between legacy and metadata-carrying state"
        );
    }
    Ok(())
}

/// Once an aggregate supplies a precise resolver, an empty candidate set is a
/// definitive refusal. In particular, a decimal sum state retains its scale
/// and storage width but not the original precision, so merge must not fall
/// back to guessing the accumulator type as the original argument.
#[test]
fn test_v2_sum_merge_does_not_fallback_after_precise_resolver() -> Result<()> {
    let entries = [Decimal64Type::from_opt_data_with_size(
        vec![Some(110), Some(220), None, Some(330)],
        Some(DecimalSize::new_unchecked(15, 2)),
    )
    .into()];
    let (_, state_type) = eval_v2_aggr("sum_state", &entries, 4, false)?;
    let DataType::AggregateState(metadata) = state_type else {
        panic!("sum_state should return an AggregateState type");
    };
    let physical_type = metadata.physical_type().clone();

    let error = match AGGR_REGISTRY.resolve(v2::AggregateFunctionRequest {
        name: "sum_merge",
        params: &[],
        args_type: std::slice::from_ref(&physical_type),
        distinct: false,
        order_by: &[],
    }) {
        Ok(_) => panic!("sum_merge should reject an ambiguous legacy decimal state"),
        Err(error) => error,
    };
    assert_eq!(error.code(), 1010);
    assert!(error.message().contains("Cannot infer"));
    Ok(())
}

/// States whose physical layout maps to no concrete signature must still be
/// refused rather than silently resolving to some unrelated signature.
#[test]
fn test_v2_merge_rejects_unrecoverable_legacy_state() -> Result<()> {
    for state_type in [
        // A layout no aggregate serializes to.
        DataType::Tuple(vec![DataType::String, DataType::String, DataType::String]),
        // Bare, non-tuple layouts are never produced by serialization.
        UInt64Type::data_type(),
    ] {
        let error = match AGGR_REGISTRY.resolve(v2::AggregateFunctionRequest {
            name: "sum_merge",
            params: &[],
            args_type: std::slice::from_ref(&state_type),
            distinct: false,
            order_by: &[],
        }) {
            Ok(_) => panic!("sum_merge should reject state type {state_type}"),
            Err(error) => error,
        };
        assert_eq!(error.code(), 1010);
        assert!(error.message().contains("Cannot infer"));
    }
    Ok(())
}

/// Intervals keep months, days and microseconds as separate components, so
/// accumulating must add them component-wise rather than folding through
/// `total_micros` (which would normalise a month to 30 days). Merging peer
/// states does collapse to a microsecond total, and the two paths are
/// deliberately asymmetric; this pins both.
#[test]
fn test_v2_sum_interval_accumulates_component_wise() -> Result<()> {
    let entries = [IntervalType::from_data(vec![
        months_days_micros::new(1, 2, 3),
        months_days_micros::new(0, 3, 4),
        months_days_micros::new(2, 0, 5),
        months_days_micros::new(0, 1, 6),
    ])
    .into()];

    // Direct accumulation keeps the components separate: 3 months, 6 days, 18us.
    let (column, _) = eval_v2_aggr("sum", &entries, 4, false)?;
    let ScalarRef::Interval(total) = (unsafe { column.index_unchecked(0) }) else {
        panic!("sum(interval) should return an interval");
    };
    assert_eq!(
        (total.months(), total.days(), total.microseconds()),
        (3, 6, 18),
        "interval sum must not be collapsed into microseconds"
    );

    // Round-tripping through serialization merges peer states, which folds the
    // components into a single microsecond total.
    let (column, _) = eval_v2_aggr("sum", &entries, 4, true)?;
    let ScalarRef::Interval(merged) = (unsafe { column.index_unchecked(0) }) else {
        panic!("sum(interval) should return an interval");
    };
    let expected_micros = (3 * 30 + 6) * months_days_micros::MICROS_PER_DAY + 18;
    assert_eq!(merged.total_micros(), expected_micros);
    Ok(())
}
