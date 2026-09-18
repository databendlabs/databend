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
        "quantile_tdigest",
        "median_tdigest",
        "uniq",
        "median",
        "std",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "skewness",
        "kurtosis",
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
            "quantile_tdigest",
            "median_tdigest",
            "stddev_pop",
            "skewness",
            "kurtosis",
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
        "quantile_tdigest_distinct(0.25, 0.75)(n)",
        "uniq_distinct(n, s)",
        "histogram_distinct(2)(n)",
        "histogram_distinct(2)(s)",
        "st_collect_distinct(to_geometry(point))",
        "st_collect_distinct(to_geometry(try_cast(all_null as string)))",
        "st_collect_distinct(NULL)",
    ] {
        write_aggregate_expr_case(file, expr, &columns, simulator, vec![]);
    }
    // Two unique pairs keep exact float comparisons in the golden simulator stable.
    // Overlapping multi-column keys are checked with a tolerance below.
    let pairs = [
        (
            "x",
            Int64Type::from_opt_data(vec![Some(1), Some(1), Some(1), Some(3), None, Some(9)])
                .into(),
        ),
        (
            "y",
            UInt64Type::from_opt_data(vec![Some(1), Some(1), Some(1), Some(3), Some(5), None])
                .into(),
        ),
    ];
    let empty_pairs = [
        ("x", Int64Type::from_data(vec![]).into()),
        ("y", UInt64Type::from_data(vec![]).into()),
    ];
    for name in [
        "covar_pop",
        "covar_samp",
        "var_pop",
        "var_samp",
        "variance_pop",
        "variance_samp",
        "quantile_tdigest_weighted",
        "median_tdigest_weighted",
    ] {
        for args in [
            "x, y",
            "NULL, y",
            "x, NULL",
            "NULL, NULL",
            "try_cast(x as float64), y",
        ] {
            write_aggregate_expr_case(
                file,
                &format!("{name}_distinct({args})"),
                &pairs,
                simulator,
                vec![],
            );
        }
        write_aggregate_expr_case(
            file,
            &format!("{name}_distinct(x, y)"),
            &empty_pairs,
            simulator,
            vec![],
        );
    }
    write_aggregate_expr_case(
        file,
        "quantile_tdigest_weighted_distinct(0.25, 0.75)(x, y)",
        &pairs,
        simulator,
        vec![],
    );
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
        ("quantile_tdigest", "quantile_tdigest_distinct"),
        ("median_tdigest", "median_tdigest_distinct"),
        ("std", "std_distinct"),
        ("stddev", "stddev_distinct"),
        ("stddev_pop", "stddev_pop_distinct"),
        ("stddev_samp", "stddev_samp_distinct"),
        ("skewness", "skewness_distinct"),
        ("kurtosis", "kurtosis_distinct"),
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

    for base in ["min", "uniq", "json_object_agg"] {
        let args_type = if base == "json_object_agg" {
            vec![DataType::String, args_type[0].clone()]
        } else {
            args_type.to_vec()
        };
        assert_eq!(
            AGGR_REGISTRY
                .descriptor(base)
                .unwrap()
                .features()
                .distinct_policy,
            DistinctPolicy::Idempotent
        );
        let semantic = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: base,
            params: &[],
            args_type: &args_type,
            distinct: true,
            order_by: &[],
        })?;
        assert_eq!(semantic.signature().name, base);
        assert!(!semantic.signature().distinct);

        let explicit_distinct = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: &format!("{base}_distinct"),
            params: &[],
            args_type: &args_type,
            distinct: false,
            order_by: &[],
        })?;
        assert_eq!(
            explicit_distinct.signature().name,
            format!("{base}_distinct")
        );
    }

    assert!(
        AGGR_REGISTRY
            .resolve(RawAggregateCall {
                name: "approx_count_distinct",
                params: &[],
                args_type: &args_type,
                distinct: true,
                order_by: &[],
            })
            .is_err()
    );

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
fn test_aggregate_route_documentation_visibility() {
    for name in ["count", "sum"] {
        assert!(
            !AGGR_REGISTRY.descriptor(name).unwrap().features().hide_doc,
            "{name}"
        );
    }
    for name in ["count_distinct", "sum_distinct", "count_if", "sum_state"] {
        assert!(
            AGGR_REGISTRY.descriptor(name).unwrap().features().hide_doc,
            "{name}"
        );
    }
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

#[test]
fn test_multi_arg_distinct_merge_and_replay() -> Result<()> {
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::aggregate_function::*;

    // The first partition repeats (1, 1); the second overlaps it and adds (1, 2).
    // Keeping distinct first/second columns independently would lose a pair.
    let left: Vec<BlockEntry> = vec![
        Int64Type::from_data(vec![1, 1, 3]).into(),
        UInt64Type::from_data(vec![1, 1, 1]).into(),
    ];
    let right: Vec<BlockEntry> = vec![
        Int64Type::from_data(vec![1, 1]).into(),
        UInt64Type::from_data(vec![1, 2]).into(),
    ];
    let unique: Vec<BlockEntry> = vec![
        Int64Type::from_data(vec![1, 3, 1]).into(),
        UInt64Type::from_data(vec![1, 1, 2]).into(),
    ];
    for base in [
        "covar_pop",
        "covar_samp",
        "var_pop",
        "var_samp",
        "variance_pop",
        "variance_samp",
        "quantile_tdigest_weighted",
        "median_tdigest_weighted",
    ] {
        let args_type = left.iter().map(BlockEntry::data_type).collect::<Vec<_>>();
        let name = format!("{base}_distinct");
        let function = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: base,
            params: &[],
            args_type: &args_type,
            distinct: true,
            order_by: &[],
        })?;
        assert_eq!(function.signature().name, name);
        let explicit = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: &name,
            params: &[],
            args_type: &args_type,
            distinct: false,
            order_by: &[],
        })?;
        assert_eq!(function.signature(), explicit.signature());
        let expected = eval_aggregate(base, vec![], &unique, 3, vec![])?.0;
        for serialized in [false, true] {
            let owner = AggregateStateOwner::new(vec![function.clone()])?;
            let rhs = AggregateStateOwner::new(vec![function.clone()])?;
            function.accumulate(AccumulateInput {
                state: owner.state(0),
                columns: left.as_slice().into(),
                validity: None,
            })?;
            function.accumulate(AccumulateInput {
                state: rhs.state(0),
                columns: right.as_slice().into(),
                validity: None,
            })?;
            // Finalizing before a merge must not make the nested cache authoritative.
            let mut builder = ColumnBuilder::with_capacity(&function.signature().return_type, 1);
            function.merge_result(MergeResultInput {
                state: owner.state(0),
                builder: &mut builder,
            })?;
            for _ in 0..2 {
                if serialized {
                    let mut builder = ColumnBuilder::with_capacity(&function.state_data_type(), 1);
                    function.serialize(SerializeInput {
                        states: rhs.state_set(0),
                        builders: builder.as_tuple_mut().unwrap(),
                    })?;
                    function.merge_serialized(MergeSerializedInput {
                        states: owner.state_set(0),
                        state: &builder.build().into(),
                        filter: None,
                    })?;
                } else {
                    function.merge_states(MergeStatesInput {
                        state: owner.state(0),
                        rhs: rhs.state(0),
                    })?;
                }
                for read_only in [true, false, true] {
                    let mut builder =
                        ColumnBuilder::with_capacity(&function.signature().return_type, 1);
                    let input = MergeResultInput {
                        state: owner.state(0),
                        builder: &mut builder,
                    };
                    if read_only {
                        function.merge_result_read_only(input)?;
                    } else {
                        function.merge_result(input)?;
                    }
                    let actual = builder.build();
                    let ScalarRef::Number(NumberScalar::Float64(actual)) = actual.index(0).unwrap()
                    else {
                        panic!("expected float")
                    };
                    let ScalarRef::Number(NumberScalar::Float64(expected)) =
                        expected.index(0).unwrap()
                    else {
                        panic!("expected float")
                    };
                    assert!(
                        (actual.0 - expected.0).abs() < 1e-12,
                        "{base}: {actual:?} != {expected:?}"
                    );
                }
            }
            // New input after finalization must also rebuild without double counting.
            function.accumulate(AccumulateInput {
                state: owner.state(0),
                columns: right.as_slice().into(),
                validity: None,
            })?;
            let mut builder = ColumnBuilder::with_capacity(&function.signature().return_type, 1);
            function.merge_result(MergeResultInput {
                state: owner.state(0),
                builder: &mut builder,
            })?;
            let result = builder.build();
            let ScalarRef::Number(NumberScalar::Float64(actual)) = result.index(0).unwrap() else {
                panic!("expected float")
            };
            let ScalarRef::Number(NumberScalar::Float64(expected)) = expected.index(0).unwrap()
            else {
                panic!("expected float")
            };
            assert!((actual.0 - expected.0).abs() < 1e-12);
        }
    }
    Ok(())
}

#[test]
fn test_kurtosis_distinct_values() -> Result<()> {
    let values = Int64Type::from_opt_data(vec![
        Some(1),
        Some(1),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        None,
    ]);
    for each_row in [false, true] {
        for with_serialize in [false, true] {
            let (result, _) = eval_aggregate_for_test(
                "kurtosis_distinct",
                vec![],
                &[values.clone().into()],
                7,
                each_row,
                with_serialize,
                vec![],
            )?;
            let ScalarRef::Number(NumberScalar::Float64(value)) = result.index(0).unwrap() else {
                panic!("expected kurtosis value");
            };
            // The four equally weighted values have corrected excess kurtosis -1.2.
            assert!((value.0 + 1.2).abs() < 1e-12);
        }
    }
    Ok(())
}

#[test]
fn test_window_funnel_distinct_is_idempotent() -> Result<()> {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::BooleanType;

    // Shuffled, repeated rows; one row satisfies two stages at the same timestamp.
    let entries: Vec<BlockEntry> = vec![
        UInt64Type::from_data(vec![2, 2, 0, 0]).into(),
        BooleanType::from_data(vec![false, false, true, true]).into(),
        BooleanType::from_data(vec![false, false, true, true]).into(),
        BooleanType::from_data(vec![true, true, false, false]).into(),
    ];
    let args_type = entries
        .iter()
        .map(BlockEntry::data_type)
        .collect::<Vec<_>>();
    assert_eq!(
        AGGR_REGISTRY
            .descriptor("window_funnel")
            .unwrap()
            .features()
            .distinct_policy,
        DistinctPolicy::Idempotent
    );
    for (window, expected) in [(0, 2), (1, 2), (2, 3)] {
        let params = vec![Scalar::Number(NumberScalar::UInt64(window))];
        let semantic = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: "window_funnel",
            params: &params,
            args_type: &args_type,
            distinct: true,
            order_by: &[],
        })?;
        assert_eq!(semantic.signature().name, "window_funnel");
        assert!(!semantic.signature().distinct);
        for name in ["window_funnel", "window_funnel_distinct"] {
            for each_row in [false, true] {
                for with_serialize in [false, true] {
                    let (result, _) = eval_aggregate_for_test(
                        name,
                        params.clone(),
                        &entries,
                        4,
                        each_row,
                        with_serialize,
                        vec![],
                    )?;
                    assert_eq!(
                        result.index(0).unwrap(),
                        ScalarRef::Number(NumberScalar::UInt8(expected))
                    );
                }
            }
            let (groups, _) =
                simulate_two_groups_group_by(name, params.clone(), &entries, 4, vec![])?;
            for row in 0..2 {
                assert_eq!(
                    groups.index(row).unwrap(),
                    ScalarRef::Number(NumberScalar::UInt8(expected))
                );
            }
        }
    }
    Ok(())
}
