use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::aggregate_function::DistinctPolicy;
use databend_common_expression::aggregate_function::EagerAggregation;
use databend_common_expression::aggregate_function::RawAggregateCall;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_expression::types::number::UInt64Type;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::eval_aggregate_for_test;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_aggregate_distinct_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        ("a", Int64Type::from_data(vec![4i64, 3, 2, 1]).into()),
        ("c", UInt64Type::from_data(vec![1u64, 2, 1, 3]).into()),
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

    write_aggregate_expr_case(file, "sum_distinct(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(c)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "sum_distinct(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "avg_distinct(c)", columns, simulator, vec![]);

    let columns = [
        (
            "n",
            Int64Type::from_opt_data(vec![Some(1), Some(1), Some(1), Some(2), Some(3), None])
                .into(),
        ),
        (
            "all_null",
            Int64Type::from_opt_data(vec![None::<i64>; 6]).into(),
        ),
        (
            "s",
            StringType::from_opt_data(vec![
                Some("a"),
                Some("a"),
                Some("a"),
                Some("b"),
                Some("c"),
                None,
            ])
            .into(),
        ),
        (
            "point",
            StringType::from_opt_data(vec![
                Some("SRID=4326;POINT(1 2)"),
                None,
                Some("SRID=4326;POINT(1 2)"),
                Some("SRID=4326;POINT(1 2)"),
                None,
                Some("SRID=4326;POINT(1 2)"),
            ])
            .into(),
        ),
    ];
    for name in ["json_agg", "json_array_agg"] {
        for arg in ["n", "all_null", "NULL", "s"] {
            write_aggregate_expr_case(
                file,
                &format!("{name}_distinct({arg})"),
                &columns,
                simulator,
                vec![],
            );
        }
        let empty = [("n", Int64Type::from_data(vec![]).into())];
        write_aggregate_expr_case(
            file,
            &format!("{name}_distinct(n)"),
            &empty,
            simulator,
            vec![],
        );
    }
    let empty = [("n", Int64Type::from_data(vec![]).into())];
    for name in [
        "quantile",
        "quantile_disc",
        "quantile_cont",
        "median",
        "std",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "skewness",
        "histogram",
    ] {
        for arg in ["n", "all_null", "NULL"] {
            write_aggregate_expr_case(
                file,
                &format!("{name}_distinct({arg})"),
                &columns,
                simulator,
                vec![],
            );
        }
        write_aggregate_expr_case(
            file,
            &format!("{name}_distinct(n)"),
            &empty,
            simulator,
            vec![],
        );
    }
    // Exercise numeric set dispatch and Decimal fallback without duplicating
    // the same fixture for each physical width.
    for arg in [
        "try_cast(n as float64)",
        "try_cast(n as decimal(15, 2))",
        "try_cast(n as decimal(30, 2))",
        "try_cast(n as decimal(60, 2))",
    ] {
        for name in [
            "quantile_disc",
            "quantile_cont",
            "stddev_pop",
            "skewness",
            "histogram",
        ] {
            write_aggregate_expr_case(
                file,
                &format!("{name}_distinct({arg})"),
                &columns,
                simulator,
                vec![],
            );
        }
    }
    for expr in [
        "quantile_distinct(0.25, 0.75)(n)",
        "quantile_disc_distinct(0.25, 0.75)(n)",
        "quantile_cont_distinct(0.25, 0.75)(n)",
        "histogram_distinct(2)(n)",
        "histogram_distinct(2)(s)",
        "st_collect_distinct(to_geometry(point))",
        "st_collect_distinct(to_geometry(try_cast(all_null as string)))",
        "st_collect_distinct(NULL)",
    ] {
        write_aggregate_expr_case(file, expr, &columns, simulator, vec![]);
    }
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
        ("quantile", "quantile_distinct"),
        ("quantile_disc", "quantile_disc_distinct"),
        ("quantile_cont", "quantile_cont_distinct"),
        ("median", "median_distinct"),
        ("std", "std_distinct"),
        ("stddev", "stddev_distinct"),
        ("stddev_pop", "stddev_pop_distinct"),
        ("stddev_samp", "stddev_samp_distinct"),
        ("skewness", "skewness_distinct"),
        ("histogram", "histogram_distinct"),
        ("json_agg", "json_agg_distinct"),
        ("json_array_agg", "json_array_agg_distinct"),
        ("st_collect", "st_collect_distinct"),
    ] {
        let args_type = if base == "st_collect" {
            [DataType::Geometry]
        } else {
            args_type.clone()
        };
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
        for arg_type in [
            args_type[0].clone(),
            args_type[0].clone().wrap_nullable(),
            DataType::Null,
        ] {
            // Plain sum_zero explicitly rejects a statically Null argument.
            if base == "SUM_ZERO" && arg_type.is_null() {
                continue;
            }
            let args_type = [arg_type];
            let plain = AGGR_REGISTRY.resolve(RawAggregateCall {
                name: base,
                params: &[],
                args_type: &args_type,
                distinct: false,
                order_by: &[],
            })?;
            let distinct = AGGR_REGISTRY.resolve(RawAggregateCall {
                name: base,
                params: &[],
                args_type: &args_type,
                distinct: true,
                order_by: &[],
            })?;
            assert_eq!(
                plain.signature().return_type,
                distinct.signature().return_type,
                "{base}({args_type:?})"
            );
        }
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
