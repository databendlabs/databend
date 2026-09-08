use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_uniq_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        (
            "b",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 3, 4])
                .into(),
        ),
        (
            "c",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 1, 3])
                .into(),
        ),
        (
            "d",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 1, 1, 1])
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
            "s",
            databend_common_expression::types::StringType::from_data(vec![
                "abc", "def", "opq", "xyz",
            ])
            .into(),
        ),
        (
            "s_null",
            databend_common_expression::types::StringType::from_data_with_validity(
                vec!["a", "", "c", "d"],
                vec![true, false, true, true],
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "uniq(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(c)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(s_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(a, c)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "uniq(all_null)", columns, simulator, vec![]);
}

#[test]
fn test_uniq() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("uniq.txt").unwrap();
    run_uniq_cases(file, eval_aggregate);
}

#[test]
fn test_uniq_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("uniq_group_by.txt").unwrap();
    run_uniq_cases(file, simulate_two_groups_group_by);
}

#[test]
fn test_uniq_finalize_then_accumulate_and_merge() -> databend_common_exception::Result<()> {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::aggregate::aggregate_function::*;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::StringType;
    use databend_common_functions::aggregates::AGGR_REGISTRY;

    let entries = [BlockEntry::from(StringType::from_data(vec!["a", "b", "a"]))];
    let args = [entries[0].data_type()];
    let function = AGGR_REGISTRY.resolve(RawAggregateCall {
        name: "uniq",
        params: &[],
        args_type: &args,
        distinct: false,
        order_by: &[],
    })?;
    let owner = AggregateStateOwner::new(vec![function.clone()])?;
    let places = vec![owner.state(0).addr; 3];
    function.accumulate_keys(AccumulateKeysInput {
        states: AggregateStateSet::new(&places, owner.state(0).loc),
        columns: entries.as_slice().into(),
    })?;
    let result = |state| -> databend_common_exception::Result<Scalar> {
        let mut builder = ColumnBuilder::with_capacity(&function.signature().return_type, 1);
        function.merge_result_read_only(MergeResultInput {
            state,
            builder: &mut builder,
        })?;
        Ok(builder.build().index(0).unwrap().to_owned())
    };
    assert_eq!(
        result(owner.state(0))?,
        Scalar::Number(NumberScalar::UInt64(2))
    );
    assert_eq!(
        result(owner.state(0))?,
        Scalar::Number(NumberScalar::UInt64(2))
    );
    let entries = [BlockEntry::from(StringType::from_data(vec!["b", "c"]))];
    for row in 0..2 {
        function.accumulate_row(AccumulateRowInput {
            state: owner.state(0),
            columns: entries.as_slice().into(),
            row,
        })?;
    }
    let mut builder = ColumnBuilder::with_capacity(&function.state_data_type(), 1);
    function.serialize(SerializeInput {
        states: owner.state_set(0),
        builders: builder.as_tuple_mut().unwrap(),
    })?;
    let serialized: BlockEntry = builder.build().into();
    let merged = AggregateStateOwner::new(vec![function.clone()])?;
    function.merge_serialized(MergeSerializedInput {
        states: merged.state_set(0),
        state: &serialized,
        filter: None,
    })?;
    function.merge_states(MergeStatesInput {
        state: merged.state(0),
        rhs: owner.state(0),
    })?;
    assert_eq!(
        result(merged.state(0))?.as_ref(),
        ScalarRef::Number(NumberScalar::UInt64(3))
    );
    Ok(())
}

// uniq.rs: typed sets, SipHash string fingerprints, and generic row keys.
// Historical samples retain decimal, multi-argument and NULL/dedup boundaries.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::BooleanType;
    use databend_common_expression::types::DateType;
    use databend_common_expression::types::Decimal64Type;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::Float64Type;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::TimestampType;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::binary;
    use super::support::bytes;
    use super::support::tuple;
    use super::support::uint64;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "uniq(x0)",
            arguments: vec!["Tuple(String, Int64)"],
            result: "UInt64",
            state: "Tuple(Array(Binary))",
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Boolean"],
            result: "UInt64",
            state: "Tuple(Array(Binary))",
            samples: vec![
                Sample {
                    label: "boolean/false/empty",
                    inputs: vec![BooleanType::from_data(Vec::<bool>::new())],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(
                        Vec::<Vec<u8>>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "boolean/false/mixed",
                    inputs: vec![BooleanType::from_data(vec![false, false, true, false])],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(vec![
                        bytes("AQAAAAkB"),
                        bytes("AQAAAAkA"),
                    ]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Date"],
            result: "UInt64",
            state: "Tuple(Array(Date))",
            samples: vec![
                Sample {
                    label: "date/false/empty",
                    inputs: vec![DateType::from_data(Vec::<i32>::new())],
                    state: tuple(vec![Scalar::Array(DateType::from_data(Vec::<i32>::new()))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "date/false/mixed",
                    inputs: vec![DateType::from_data(vec![0, 0, 1, 0])],
                    state: tuple(vec![Scalar::Array(DateType::from_data(vec![0, 1]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Float64"],
            result: "UInt64",
            state: "Tuple(Array(Float64))",
            samples: vec![
                Sample {
                    label: "float/false/empty",
                    inputs: vec![Float64Type::from_data(Vec::<f64>::new())],
                    state: tuple(vec![Scalar::Array(Float64Type::from_data(
                        Vec::<f64>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "float/false/mixed",
                    inputs: vec![Float64Type::from_data(vec![-0.0, 0.0, f64::NAN, -0.0])],
                    state: tuple(vec![Scalar::Array(Float64Type::from_data(vec![
                        0.0,
                        f64::NAN,
                    ]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Int64"],
            result: "UInt64",
            state: "Tuple(Array(Int64))",
            samples: vec![
                Sample {
                    label: "int/false/empty",
                    inputs: vec![Int64Type::from_data(Vec::<i64>::new())],
                    state: tuple(vec![Scalar::Array(Int64Type::from_data(Vec::<i64>::new()))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "int/false/mixed",
                    inputs: vec![Int64Type::from_data(vec![0, 0, 1, 0])],
                    state: tuple(vec![Scalar::Array(Int64Type::from_data(vec![0, 1]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["String"],
            result: "UInt64",
            state: "Tuple(Binary)",
            samples: vec![
                Sample {
                    label: "string/false/empty",
                    inputs: vec![StringType::from_data(Vec::<&str>::new())],
                    state: tuple(vec![binary("AA==")]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/false/mixed",
                    inputs: vec![StringType::from_data(vec![
                        "重复-0", "", "重复-1", "重复-0",
                    ])],
                    state: tuple(vec![binary(
                        "A1c/DrAQb455LSMjuMC02M/TnO5HOCd0EGD7KJlzgg0yUEnXR4Cj4H1CAqtH1M7y9A==",
                    )]),
                    result: uint64(3),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Timestamp"],
            result: "UInt64",
            state: "Tuple(Array(Timestamp))",
            samples: vec![
                Sample {
                    label: "timestamp/false/empty",
                    inputs: vec![TimestampType::from_data(Vec::<i64>::new())],
                    state: tuple(vec![Scalar::Array(TimestampType::from_data(
                        Vec::<i64>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "timestamp/false/mixed",
                    inputs: vec![TimestampType::from_data(vec![0, 0, 1, 0])],
                    state: tuple(vec![Scalar::Array(TimestampType::from_data(vec![0, 1]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "UInt64",
            state: "Tuple(Array(Binary))",
            samples: vec![
                Sample {
                    label: "decimal/false/empty",
                    inputs: vec![Decimal64Type::from_data_with_size(
                        Vec::<i64>::new(),
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(
                        Vec::<Vec<u8>>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal/false/mixed",
                    inputs: vec![Decimal64Type::from_data_with_size(
                        vec![0, 0, 1, 0],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(vec![
                        bytes("AQAAAAQAAAAAAAAAAAAPAg=="),
                        bytes("AQAAAAQAAQAAAAAAAAAPAg=="),
                    ]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Nullable(Boolean)"],
            result: "UInt64",
            state: "Tuple(Array(Binary))",
            samples: vec![
                Sample {
                    label: "boolean/true/empty",
                    inputs: vec![BooleanType::from_opt_data(Vec::<Option<bool>>::new())],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(
                        Vec::<Vec<u8>>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "boolean/true/nulls",
                    inputs: vec![BooleanType::from_opt_data(vec![None::<bool>; 2])],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(
                        Vec::<Vec<u8>>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "boolean/true/mixed",
                    inputs: vec![BooleanType::from_opt_data(vec![
                        Some(false),
                        None,
                        Some(true),
                        Some(false),
                    ])],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(vec![
                        bytes("AQAAAAkB"),
                        bytes("AQAAAAkA"),
                    ]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Nullable(Date)"],
            result: "UInt64",
            state: "Tuple(Array(Date))",
            samples: vec![
                Sample {
                    label: "date/true/empty",
                    inputs: vec![DateType::from_opt_data(Vec::<Option<i32>>::new())],
                    state: tuple(vec![Scalar::Array(DateType::from_data(Vec::<i32>::new()))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "date/true/nulls",
                    inputs: vec![DateType::from_opt_data(vec![None::<i32>; 2])],
                    state: tuple(vec![Scalar::Array(DateType::from_data(Vec::<i32>::new()))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "date/true/mixed",
                    inputs: vec![DateType::from_opt_data(vec![
                        Some(0),
                        None,
                        Some(1),
                        Some(0),
                    ])],
                    state: tuple(vec![Scalar::Array(DateType::from_data(vec![0, 1]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Nullable(Float64)"],
            result: "UInt64",
            state: "Tuple(Array(Float64))",
            samples: vec![
                Sample {
                    label: "float/true/empty",
                    inputs: vec![Float64Type::from_opt_data(Vec::<Option<f64>>::new())],
                    state: tuple(vec![Scalar::Array(Float64Type::from_data(
                        Vec::<f64>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "float/true/nulls",
                    inputs: vec![Float64Type::from_opt_data(vec![None::<f64>; 2])],
                    state: tuple(vec![Scalar::Array(Float64Type::from_data(
                        Vec::<f64>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "float/true/mixed",
                    inputs: vec![Float64Type::from_opt_data(vec![
                        Some(-0.0),
                        None,
                        Some(f64::NAN),
                        Some(-0.0),
                    ])],
                    state: tuple(vec![Scalar::Array(Float64Type::from_data(vec![
                        0.0,
                        f64::NAN,
                    ]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Nullable(Int64)"],
            result: "UInt64",
            state: "Tuple(Array(Int64))",
            samples: vec![
                Sample {
                    label: "int/true/empty",
                    inputs: vec![Int64Type::from_opt_data(Vec::<Option<i64>>::new())],
                    state: tuple(vec![Scalar::Array(Int64Type::from_data(Vec::<i64>::new()))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "int/true/nulls",
                    inputs: vec![Int64Type::from_opt_data(vec![None::<i64>; 2])],
                    state: tuple(vec![Scalar::Array(Int64Type::from_data(Vec::<i64>::new()))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "int/true/mixed",
                    inputs: vec![Int64Type::from_opt_data(vec![
                        Some(0),
                        None,
                        Some(1),
                        Some(0),
                    ])],
                    state: tuple(vec![Scalar::Array(Int64Type::from_data(vec![0, 1]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Nullable(String)"],
            result: "UInt64",
            state: "Tuple(Binary)",
            samples: vec![
                Sample {
                    label: "string/true/empty",
                    inputs: vec![StringType::from_opt_data(Vec::<Option<&str>>::new())],
                    state: tuple(vec![binary("AA==")]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/true/nulls",
                    inputs: vec![StringType::from_opt_data(vec![None::<&str>; 2])],
                    state: tuple(vec![binary("AA==")]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/true/mixed",
                    inputs: vec![StringType::from_opt_data(vec![
                        Some("重复-0"),
                        None,
                        Some("重复-1"),
                        Some("重复-0"),
                    ])],
                    state: tuple(vec![binary("Alc/DrAQb455LSMjuMC02M/TnO5HOCd0EGD7KJlzgg0y")]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Nullable(Timestamp)"],
            result: "UInt64",
            state: "Tuple(Array(Timestamp))",
            samples: vec![
                Sample {
                    label: "timestamp/true/empty",
                    inputs: vec![TimestampType::from_opt_data(Vec::<Option<i64>>::new())],
                    state: tuple(vec![Scalar::Array(TimestampType::from_data(
                        Vec::<i64>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "timestamp/true/nulls",
                    inputs: vec![TimestampType::from_opt_data(vec![None::<i64>; 2])],
                    state: tuple(vec![Scalar::Array(TimestampType::from_data(
                        Vec::<i64>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "timestamp/true/mixed",
                    inputs: vec![TimestampType::from_opt_data(vec![
                        Some(0),
                        None,
                        Some(1),
                        Some(0),
                    ])],
                    state: tuple(vec![Scalar::Array(TimestampType::from_data(vec![0, 1]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0)",
            arguments: vec!["Nullable(Decimal(15, 2))"],
            result: "UInt64",
            state: "Tuple(Array(Binary))",
            samples: vec![
                Sample {
                    label: "decimal/true/empty",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        Vec::<Option<i64>>::new(),
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(
                        Vec::<Vec<u8>>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal/true/nulls",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        vec![None::<i64>; 2],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(
                        Vec::<Vec<u8>>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal/true/mixed",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        vec![Some(0), None, Some(1), Some(0)],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(vec![
                        bytes("AQAAAAQAAAAAAAAAAAAPAg=="),
                        bytes("AQAAAAQAAQAAAAAAAAAPAg=="),
                    ]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0, x1)",
            arguments: vec!["String", "Int64"],
            result: "UInt64",
            state: "Tuple(Array(Binary))",
            samples: vec![
                Sample {
                    label: "multi/false/empty",
                    inputs: vec![
                        StringType::from_data(Vec::<&str>::new()),
                        Int64Type::from_data(Vec::<i64>::new()),
                    ],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(
                        Vec::<Vec<u8>>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "multi/false/mixed",
                    inputs: vec![
                        StringType::from_data(vec!["重复-0", "", "重复-1", "重复-0"]),
                        Int64Type::from_data(vec![0, 0, 1, 0]),
                    ],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(vec![
                        bytes("AgAAAAsAAAAAAwcAAAAAAAAAAA=="),
                        bytes("AgAAAAsIAAAA6YeN5aSNLTEDBwEAAAAAAAAA"),
                        bytes("AgAAAAsIAAAA6YeN5aSNLTADBwAAAAAAAAAA"),
                    ]))]),
                    result: uint64(3),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "uniq(x0, x1)",
            arguments: vec!["Nullable(String)", "Nullable(Int64)"],
            result: "UInt64",
            state: "Tuple(Array(Binary))",
            samples: vec![
                Sample {
                    label: "multi/true/empty",
                    inputs: vec![
                        StringType::from_opt_data(Vec::<Option<&str>>::new()),
                        Int64Type::from_opt_data(Vec::<Option<i64>>::new()),
                    ],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(
                        Vec::<Vec<u8>>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "multi/true/nulls",
                    inputs: vec![
                        StringType::from_opt_data(vec![None::<&str>; 2]),
                        Int64Type::from_opt_data(vec![None::<i64>; 2]),
                    ],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(
                        Vec::<Vec<u8>>::new(),
                    ))]),
                    result: uint64(0),
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "multi/true/mixed",
                    inputs: vec![
                        StringType::from_opt_data(vec![
                            Some("重复-0"),
                            None,
                            Some("重复-1"),
                            Some("重复-0"),
                        ]),
                        Int64Type::from_opt_data(vec![Some(0), None, Some(1), Some(0)]),
                    ],
                    state: tuple(vec![Scalar::Array(BinaryType::from_data(vec![
                        bytes("AgAAAAsIAAAA6YeN5aSNLTADBwAAAAAAAAAA"),
                        bytes("AgAAAAsIAAAA6YeN5aSNLTEDBwEAAAAAAAAA"),
                    ]))]),
                    result: uint64(2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
    ]);
}
