use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::FromData;
use databend_common_expression::aggregate_function::DistinctPolicy;
use databend_common_expression::aggregate_function::EagerAggregation;
use databend_common_expression::aggregate_function::RawAggregateCall;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_aggregate_distinct_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "c",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 1, 3])
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

    write_aggregate_expr_case(file, "sum_distinct(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(c)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg_distinct(c)", columns, simulator, vec![]);
}

#[test]
fn test_aggregate_distinct() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("aggregate_distinct.txt").unwrap();
    run_aggregate_distinct_cases(file, eval_aggregate);
}

#[test]
fn test_aggregate_distinct_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("aggregate_distinct_group_by.txt")
        .unwrap();
    run_aggregate_distinct_cases(file, simulate_two_groups_group_by);
}

#[test]
fn test_semantic_distinct_resolves_visible_target_name() -> Result<()> {
    let args_type = [DataType::Number(NumberDataType::UInt64)];
    for (base, target) in [
        ("count", "count_distinct"),
        ("sum", "sum_distinct"),
        ("avg", "avg_distinct"),
        ("array_agg", "array_agg_distinct"),
        ("list", "list_distinct"),
        ("LIST", "list_distinct"),
        ("SUM_ZERO", "sum_zero_distinct"),
    ] {
        assert_eq!(
            AGGR_REGISTRY
                .descriptor(base)
                .unwrap()
                .features()
                .distinct_policy
                .target_for(base),
            Some(target)
        );
        let semantic = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: base,
            params: &[],
            args_type: &args_type,
            distinct: true,
            order_by: &[],
        })?;
        assert_eq!(semantic.signature().name, target);
        assert!(!semantic.signature().distinct);

        let explicit = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: target,
            params: &[],
            args_type: &args_type,
            distinct: false,
            order_by: &[],
        })?;
        assert_eq!(explicit.signature().name, target);
    }

    assert_eq!(
        AGGR_REGISTRY
            .descriptor("min")
            .unwrap()
            .features()
            .distinct_policy,
        DistinctPolicy::Idempotent
    );
    let min = AGGR_REGISTRY.resolve(RawAggregateCall {
        name: "min",
        params: &[],
        args_type: &args_type,
        distinct: true,
        order_by: &[],
    })?;
    assert_eq!(min.signature().name, "min");
    assert!(!min.signature().distinct);

    let explicit_min_distinct = AGGR_REGISTRY.resolve(RawAggregateCall {
        name: "min_distinct",
        params: &[],
        args_type: &args_type,
        distinct: false,
        order_by: &[],
    })?;
    assert_eq!(explicit_min_distinct.signature().name, "min_distinct");

    for intrinsic_name in ["uniq", "approx_count_distinct"] {
        assert!(
            AGGR_REGISTRY
                .resolve(RawAggregateCall {
                    name: intrinsic_name,
                    params: &[],
                    args_type: &args_type,
                    distinct: true,
                    order_by: &[],
                })
                .is_err()
        );
    }

    let count_multiple_args = AGGR_REGISTRY.resolve(RawAggregateCall {
        name: "count",
        params: &[],
        args_type: &[args_type[0].clone(), args_type[0].clone()],
        distinct: true,
        order_by: &[],
    })?;
    assert_eq!(count_multiple_args.signature().name, "count_distinct");
    Ok(())
}

#[test]
fn test_eager_aggregation_strategies() -> Result<()> {
    for (name, strategy) in [
        ("sum", EagerAggregation::Sum),
        ("count", EagerAggregation::Count),
        ("min", EagerAggregation::MinMax),
        ("max", EagerAggregation::MinMax),
        ("min_distinct", EagerAggregation::MinMax),
        ("max_distinct", EagerAggregation::MinMax),
        ("sum_distinct", EagerAggregation::Unsupported),
        ("count_distinct", EagerAggregation::Unsupported),
        ("avg_distinct", EagerAggregation::Unsupported),
        ("uniq", EagerAggregation::Unsupported),
        ("avg", EagerAggregation::Unsupported),
        ("stddev_pop", EagerAggregation::Unsupported),
        ("std", EagerAggregation::Unsupported),
        ("sum0", EagerAggregation::Unsupported),
        ("sum_zero", EagerAggregation::Unsupported),
        ("sum_state", EagerAggregation::Unsupported),
        ("count_if", EagerAggregation::Unsupported),
    ] {
        assert_eq!(
            AGGR_REGISTRY
                .descriptor(name)
                .unwrap()
                .features()
                .eager_aggregation,
            strategy,
            "{name}"
        );
        let args_type = if name == "count_if" {
            DataType::Boolean
        } else {
            DataType::Number(NumberDataType::UInt64)
        };
        let call = AGGR_REGISTRY.resolve(RawAggregateCall {
            name,
            params: &[],
            args_type: &[args_type],
            distinct: false,
            order_by: &[],
        })?;
        assert_eq!(call.features().eager_aggregation, strategy, "{name}");
    }
    Ok(())
}

#[test]
fn test_distinct_float_equality() -> Result<()> {
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::Float64Type;
    use databend_common_expression::types::NumberScalar;

    use super::support::eval_aggregate_for_test;

    let values = Float64Type::from_data(vec![
        -0.0f64,
        0.0f64,
        f64::from_bits(0x7ff8000000000001),
        f64::from_bits(0xfff8000000000002),
    ]);
    for name in ["count_distinct", "uniq"] {
        for each_row in [false, true] {
            for with_serialize in [false, true] {
                let (result, _) = eval_aggregate_for_test(
                    name,
                    vec![],
                    &[values.clone().into()],
                    4,
                    each_row,
                    with_serialize,
                    vec![],
                )?;
                assert_eq!(
                    result.index(0).unwrap(),
                    ScalarRef::Number(NumberScalar::UInt64(2))
                );
                // Multi-argument DISTINCT retains bytewise row equality, including
                // signed zeros and different NaN payloads.
                let (result, _) = eval_aggregate_for_test(
                    name,
                    vec![],
                    &[
                        values.clone().into(),
                        databend_common_expression::types::BooleanType::from_data(vec![true; 4])
                            .into(),
                    ],
                    4,
                    each_row,
                    with_serialize,
                    vec![],
                )?;
                assert_eq!(
                    result.index(0).unwrap(),
                    ScalarRef::Number(NumberScalar::UInt64(4))
                );
            }
        }
    }
    Ok(())
}

#[test]
fn test_distinct_unary_any_fallback() -> Result<()> {
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::StringType;

    use super::support::eval_aggregate_for_test;

    let values = StringType::from_data_with_validity(vec!["a", "b", "a", "ignored"], vec![
        true, true, true, false,
    ]);
    for each_row in [false, true] {
        for with_serialize in [false, true] {
            let (result, _) = eval_aggregate_for_test(
                "count_distinct",
                vec![],
                &[values.clone().into()],
                4,
                each_row,
                with_serialize,
                vec![],
            )?;
            assert_eq!(
                result.index(0).unwrap(),
                ScalarRef::Number(NumberScalar::UInt64(2))
            );
        }
    }
    Ok(())
}

#[test]
fn test_count_distinct_rows() -> Result<()> {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;

    use super::support::eval_aggregate_for_test;

    let entries = [
        BlockEntry::from(UInt64Type::from_data_with_validity(
            vec![1u64, 1, 2, 0, 2, 1],
            vec![true, true, true, false, true, true],
        )),
        BlockEntry::from(StringType::from_data_with_validity(
            vec!["a", "a", "b", "b", "", "a"],
            vec![true, true, true, true, false, true],
        )),
    ];
    for each_row in [false, true] {
        for with_serialize in [false, true] {
            let (result, _) = eval_aggregate_for_test(
                "count_distinct",
                vec![],
                &entries,
                6,
                each_row,
                with_serialize,
                vec![],
            )?;
            assert_eq!(
                result.index(0).unwrap(),
                ScalarRef::Number(NumberScalar::UInt64(2))
            );
        }
    }
    let (groups, _) = simulate_two_groups_group_by("count_distinct", vec![], &entries, 6, vec![])?;
    assert_eq!(
        groups.index(0).unwrap(),
        ScalarRef::Number(NumberScalar::UInt64(2))
    );
    assert_eq!(
        groups.index(1).unwrap(),
        ScalarRef::Number(NumberScalar::UInt64(1))
    );
    Ok(())
}
