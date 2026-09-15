use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::Symbol;
use databend_common_expression::aggregate_function::AggregateBoundOrderByItem;
use databend_common_expression::aggregate_function::AggregateBoundOrderBySource;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::bitmap_column;
use super::support::eval_aggregate;
use super::support::eval_v2_aggr;
use super::support::geometry_columns;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_array_agg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let mut columns: Vec<(&str, databend_common_expression::BlockEntry)> = vec![
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
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
            "b",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 3, 4])
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
        (
            "date_col",
            databend_common_expression::types::DateType::from_data(vec![1, 2, 1, 3]).into(),
        ),
        (
            "dt",
            databend_common_expression::types::TimestampType::from_data(vec![1i64, 0, 2, 3]).into(),
        ),
        (
            "event1",
            databend_common_expression::types::BooleanType::from_data(vec![
                true, false, false, false,
            ])
            .into(),
        ),
        (
            "dec",
            databend_common_expression::types::Decimal64Type::from_opt_data_with_size(
                vec![Some(110), Some(220), None, Some(330)],
                Some(databend_common_expression::types::DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        (
            "s",
            databend_common_expression::types::StringType::from_data(vec![
                "abc", "def", "opq", "xyz",
            ])
            .into(),
        ),
        (
            "s_null",
            databend_common_expression::types::StringType::from_data_with_validity(
                vec!["abc", "", "opq", "xyz"],
                vec![true, false, true, true],
            )
            .into(),
        ),
        (
            "json",
            databend_common_expression::types::StringType::from_data(vec![
                r#"{"k1":"v1","k2":"v2"}"#,
                r#"[1,2,3,"abc"]"#,
                r#"99999"#,
                r#""xyz""#,
            ])
            .into(),
        ),
    ];
    columns.push(("bitmap", bitmap_column().into()));
    columns.extend(geometry_columns());
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "array_agg(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(const_int)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "array_agg(const_int_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "array_agg('a')", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg([])", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg({})", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "array_agg(if(event1, [], NULL))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "array_agg(if(event1, {}, NULL))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "array_agg(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg_state(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(a)", columns, simulator, vec![
        AggregateBoundOrderByItem {
            index: Symbol::new(0),
            source: AggregateBoundOrderBySource::Argument { index: 0 },
            data_type: columns[0].1.data_type(),
            nulls_first: false,
            asc: true,
        },
    ]);
    write_aggregate_expr_case(file, "array_agg(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "list(a)", columns, simulator, vec![
        AggregateBoundOrderByItem {
            index: Symbol::new(0),
            source: AggregateBoundOrderBySource::Argument { index: 0 },
            data_type: columns[0].1.data_type(),
            nulls_first: false,
            asc: true,
        },
    ]);
    write_aggregate_expr_case(file, "array_agg(date_col)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(dt)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "array_agg(to_interval('1 day'))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "array_agg(event1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(s_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(to_binary(s))", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "array_agg(to_binary(s_null))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "array_agg(bitmap)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "array_agg(parse_json(json))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "array_agg(to_geometry(point))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "array_agg(to_geography(point_4326))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "array_agg([b, b])", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg(map([s], [a]))", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "array_agg((a, s))", columns, simulator, vec![]);
}

#[test]
fn test_array_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("array_agg.txt").unwrap();
    run_array_agg_cases(file, eval_aggregate);
}

#[test]
fn test_array_agg_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("array_agg_group_by.txt").unwrap();
    run_array_agg_cases(file, simulate_two_groups_group_by);
}

#[test]
fn test_v2_array_agg_if_applies_predicate() -> Result<()> {
    let entries = [
        UInt64Type::from_data(vec![1, 2, 3, 4]).into(),
        BooleanType::from_data(vec![true, false, false, false]).into(),
    ];
    let return_type = DataType::Array(Box::new(UInt64Type::data_type()));
    let expected = (
        Column::Array(Box::new(
            ArrayColumn::<UInt64Type>::new(Buffer::from(vec![1u64]), Buffer::from(vec![0, 1]))
                .upcast(&return_type),
        )),
        return_type,
    );

    let direct_v2 = eval_v2_aggr("array_agg_if", &entries, 4, false)?;
    let serialized_v2 = eval_v2_aggr("array_agg_if", &entries, 4, true)?;
    assert_eq!(direct_v2, expected);
    assert_eq!(serialized_v2, expected);
    Ok(())
}

// array_agg.rs: Core values, binary-backed values, Boolean, and AnyType
// use different state paths. Keep decimal widths and one nullable representative.
#[test]
fn test_state_baselines() {
    use super::support::Case;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Boolean"],
            result: "Array(Boolean)",
            state: "Tuple(Array(Boolean))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Date"],
            result: "Array(Date)",
            state: "Tuple(Array(Date))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Float64"],
            result: "Array(Float64)",
            state: "Tuple(Array(Float64))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Int64"],
            result: "Array(Int64)",
            state: "Tuple(Array(Int64))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Interval"],
            result: "Array(Interval)",
            state: "Tuple(Array(Interval))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["String"],
            result: "Array(String)",
            state: "Tuple(Array(String))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Timestamp"],
            result: "Array(Timestamp)",
            state: "Tuple(Array(Timestamp))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Variant"],
            result: "Array(Variant)",
            state: "Tuple(Array(Variant))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Array(Int64)"],
            result: "Array(Array(Int64))",
            state: "Tuple(Array(Array(Int64)))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "Array(Decimal(15, 2))",
            state: "Tuple(Array(Decimal(15, 2)))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Decimal(38, 6)"],
            result: "Array(Decimal(38, 6))",
            state: "Tuple(Array(Decimal(38, 6)))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Decimal(76, 12)"],
            result: "Array(Decimal(76, 12))",
            state: "Tuple(Array(Decimal(76, 12)))",
        },
        Case::Metadata {
            expression: "array_agg(x0)",
            arguments: vec!["Nullable(Int64)"],
            result: "Array(Int64)",
            state: "Tuple(Array(Int64))",
        },
    ]);
}

#[test]
fn test_array_agg_distinct() -> databend_common_exception::Result<()> {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::UInt64Type;

    use super::support::eval_aggregate_for_test;

    let values = UInt64Type::from_data_with_validity(vec![2u64, 1, 2, 1, 0, 0], vec![
        true, true, true, true, false, false,
    ]);
    for name in ["array_agg_distinct", "list_distinct"] {
        for each_row in [false, true] {
            for with_serialize in [false, true] {
                for (entry, expected) in [
                    (BlockEntry::from(values.clone()), vec![1u64, 2]),
                    (
                        BlockEntry::new_const_column(
                            UInt64Type::data_type().wrap_nullable(),
                            Scalar::Null,
                            6,
                        ),
                        vec![],
                    ),
                    (
                        BlockEntry::new_const_column(DataType::Null, Scalar::Null, 6),
                        vec![],
                    ),
                    (
                        BlockEntry::from(UInt64Type::from_data(Vec::<u64>::new())),
                        vec![],
                    ),
                ] {
                    let expected_type = if entry.data_type().is_null() {
                        DataType::EmptyArray
                    } else {
                        DataType::Array(Box::new(UInt64Type::data_type()))
                    };
                    let rows = entry.len();
                    let (result, return_type) = eval_aggregate_for_test(
                        name,
                        vec![],
                        &[entry],
                        rows,
                        each_row,
                        with_serialize,
                        vec![],
                    )?;
                    assert_eq!(return_type, expected_type);
                    if expected_type == DataType::EmptyArray {
                        assert_eq!(result.index(0).unwrap(), ScalarRef::EmptyArray);
                        continue;
                    }
                    let ScalarRef::Array(array) = result.index(0).unwrap() else {
                        panic!("array aggregate must return an array");
                    };
                    let mut actual = array
                        .iter()
                        .map(|value| value.to_string())
                        .collect::<Vec<_>>();
                    actual.sort();
                    assert_eq!(
                        actual,
                        expected.iter().map(ToString::to_string).collect::<Vec<_>>(),
                        "{name}, each_row={each_row}, with_serialize={with_serialize}"
                    );
                }
            }
        }
        let (groups, _) =
            simulate_two_groups_group_by(name, vec![], &[values.clone().into()], 6, vec![])?;
        for (row, expected) in ["2", "1"].into_iter().enumerate() {
            let ScalarRef::Array(array) = groups.index(row).unwrap() else {
                panic!("array aggregate must return an array");
            };
            assert_eq!(array.len(), 1);
            assert_eq!(array.index(0).unwrap().to_string(), expected);
        }
    }
    Ok(())
}

#[test]
fn test_array_agg_null_specialization() -> Result<()> {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;

    use super::support::eval_aggregate_for_test;

    for rows in [0, 4] {
        let entry = BlockEntry::new_const_column(DataType::Null, Scalar::Null, rows);
        for name in ["array_agg", "list", "array_agg_distinct", "list_distinct"] {
            for order_by in [vec![], vec![AggregateBoundOrderByItem {
                index: Symbol::new(0),
                source: AggregateBoundOrderBySource::Argument { index: 0 },
                data_type: DataType::Null,
                nulls_first: false,
                asc: true,
            }]] {
                for each_row in [false, true] {
                    for with_serialize in [false, true] {
                        let (result, return_type) = eval_aggregate_for_test(
                            name,
                            vec![],
                            std::slice::from_ref(&entry),
                            rows,
                            each_row,
                            with_serialize,
                            order_by.clone(),
                        )?;
                        assert_eq!(return_type, DataType::EmptyArray);
                        assert_eq!(result.index(0).unwrap(), ScalarRef::EmptyArray);
                    }
                }
            }
        }
    }
    let entry = BlockEntry::new_const_column(DataType::Null, Scalar::Null, 4);
    let (result, _) = simulate_two_groups_group_by("array_agg", vec![], &[entry], 4, vec![])?;
    for group in 0..2 {
        assert_eq!(result.index(group).unwrap(), ScalarRef::EmptyArray);
    }
    Ok(())
}

#[test]
fn test_array_agg_distinct_order_by() -> Result<()> {
    use databend_common_expression::ScalarRef;
    let values = UInt64Type::from_data_with_validity(vec![3u64, 1, 3, 2, 0], vec![
        true, true, true, true, false,
    ]);
    for name in ["array_agg_distinct", "list_distinct"] {
        for asc in [true, false] {
            let order = vec![AggregateBoundOrderByItem {
                index: Symbol::new(0),
                source: AggregateBoundOrderBySource::Argument { index: 0 },
                data_type: UInt64Type::data_type().wrap_nullable(),
                nulls_first: false,
                asc,
            }];
            let (groups, _) = simulate_two_groups_group_by(
                name,
                vec![],
                &[values.clone().into()],
                5,
                order.clone(),
            )?;
            assert_eq!(
                groups.index(0).unwrap(),
                ScalarRef::Array(UInt64Type::from_data(vec![3u64]))
            );
            assert_eq!(
                groups.index(1).unwrap(),
                ScalarRef::Array(UInt64Type::from_data(if asc {
                    vec![1u64, 2]
                } else {
                    vec![2u64, 1]
                },))
            );
            for each_row in [false, true] {
                for with_serialize in [false, true] {
                    let (result, _) = super::support::eval_aggregate_for_test(
                        name,
                        vec![],
                        &[values.clone().into()],
                        5,
                        each_row,
                        with_serialize,
                        order.clone(),
                    )?;
                    let expected = UInt64Type::from_data(if asc {
                        vec![1u64, 2, 3]
                    } else {
                        vec![3u64, 2, 1]
                    });
                    assert_eq!(result.index(0).unwrap(), ScalarRef::Array(expected));
                }
            }
        }
    }
    Ok(())
}

#[test]
fn test_array_agg_distinct_rejects_independent_sort_key() {
    let order = vec![AggregateBoundOrderByItem {
        index: Symbol::new(1),
        source: AggregateBoundOrderBySource::Derived,
        data_type: UInt64Type::data_type(),
        nulls_first: false,
        asc: true,
    }];
    let error = super::support::eval_aggregate_for_test(
        "array_agg_distinct",
        vec![],
        &[UInt64Type::from_data(vec![1u64]).into()],
        1,
        false,
        false,
        order,
    )
    .unwrap_err();
    assert!(
        error
            .message()
            .contains("ORDER BY must reference its argument")
    );
}

#[test]
fn test_array_agg_any_state_skips_null() -> Result<()> {
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;

    let nested = UInt64Type::from_data_with_validity(vec![1u64, 0], vec![true, false]);
    let cases = [
        (DataType::Boolean, Scalar::Boolean(true)),
        (
            DataType::Array(Box::new(UInt64Type::data_type().wrap_nullable())),
            Scalar::Array(nested),
        ),
        (
            DataType::Tuple(vec![
                DataType::Boolean,
                UInt64Type::data_type().wrap_nullable(),
            ]),
            Scalar::Tuple(vec![Scalar::Boolean(true), Scalar::Null]),
        ),
    ];
    for (data_type, value) in cases {
        for all_null in [false, true] {
            let mut input = ColumnBuilder::with_capacity(&data_type.clone().wrap_nullable(), 4);
            let mut expected = ColumnBuilder::with_capacity(&data_type, 2);
            for selected in [true, false, false, true] {
                if selected && !all_null {
                    input.push(value.as_ref());
                    expected.push(value.as_ref());
                } else {
                    input.push(ScalarRef::Null);
                }
            }
            let entry = input.build().into();
            let expected = expected.build();
            for name in ["array_agg", "list"] {
                for each_row in [false, true] {
                    for with_serialize in [false, true] {
                        let (result, return_type) = super::support::eval_aggregate_for_test(
                            name,
                            vec![],
                            std::slice::from_ref(&entry),
                            4,
                            each_row,
                            with_serialize,
                            vec![],
                        )?;
                        assert_eq!(return_type, DataType::Array(Box::new(data_type.clone())));
                        assert_eq!(result.index(0).unwrap(), ScalarRef::Array(expected.clone()));
                    }
                }
                let (groups, _) = simulate_two_groups_group_by(
                    name,
                    vec![],
                    std::slice::from_ref(&entry),
                    4,
                    vec![],
                )?;
                let mut expected_group = ColumnBuilder::with_capacity(&data_type, 1);
                if !all_null {
                    expected_group.push(value.as_ref());
                }
                let expected_group = expected_group.build();
                for row in 0..2 {
                    assert_eq!(
                        groups.index(row).unwrap(),
                        ScalarRef::Array(expected_group.clone())
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn test_distinct_state_uses_native_columns() -> Result<()> {
    use databend_common_expression::aggregate_function::RawAggregateCall;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberDataType;
    use databend_common_functions::aggregates::AGGR_REGISTRY;

    for argument in [
        DataType::Number(NumberDataType::Int64).wrap_nullable(),
        DataType::Number(NumberDataType::Float32),
        DataType::Number(NumberDataType::Float64),
        DataType::Decimal(DecimalSize::new_unchecked(76, 12)),
        DataType::String,
        DataType::Variant,
    ] {
        let element = match argument {
            DataType::String | DataType::Variant | DataType::Decimal(_) => DataType::Binary,
            _ => argument.remove_nullable(),
        };
        let function = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: "array_agg",
            params: &[],
            args_type: &[argument],
            distinct: true,
            order_by: &[],
        })?;
        assert_eq!(
            function.state_data_type(),
            DataType::Tuple(vec![DataType::Array(Box::new(element))])
        );
    }
    Ok(())
}
