use databend_common_exception::Result;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::aggregate_function::RawAggregateCall;
use databend_common_expression::types::AggregateStateDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::aggregates::AGGR_REGISTRY;

use super::support::eval_v2_aggr;

#[test]
fn test_v2_min_max_any_uint64_matches_expected_values() -> Result<()> {
    let entries = [UInt64Type::from_data(vec![9, 2, 5, 7]).into()];

    let min = eval_v2_aggr("min", &entries, 4, false)?;
    let min_serialized = eval_v2_aggr("min", &entries, 4, true)?;
    assert_eq!(
        unsafe { min.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::UInt64(2))
    );
    assert_eq!(min_serialized, min);

    let max = eval_v2_aggr("max", &entries, 4, false)?;
    let max_serialized = eval_v2_aggr("max", &entries, 4, true)?;
    assert_eq!(
        unsafe { max.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::UInt64(9))
    );
    assert_eq!(max_serialized, max);

    let any = eval_v2_aggr("any", &entries, 4, false)?;
    let any_serialized = eval_v2_aggr("any", &entries, 4, true)?;
    assert_eq!(
        unsafe { any.0.index_unchecked(0) },
        ScalarRef::Number(NumberScalar::UInt64(9))
    );
    assert_eq!(any_serialized, any);
    Ok(())
}

#[test]
fn test_v2_min_max_any_heap_states_require_manual_drop() -> Result<()> {
    let aggregate_state = DataType::AggregateState(Box::new(AggregateStateDataType {
        function_name: "test".to_string(),
        params: vec![],
        argument_types: vec![],
        state_type: Box::new(DataType::Binary),
    }));

    for data_type in [DataType::Binary, aggregate_state] {
        let function = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: "min",
            params: &[],
            args_type: &[data_type],
            distinct: false,
            order_by: &[],
        })?;
        assert!(function.state().need_manual_drop());
    }
    Ok(())
}

// min_max_any.rs: comparison policies, typed scalars, AnyType fallback, and
// nullable String/Decimal wire layouts. Retain all decimal-width history samples.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::Decimal64Type;
    use databend_common_expression::types::Decimal128Type;
    use databend_common_expression::types::Decimal256Type;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::i256;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::decimal64;
    use super::support::decimal128;
    use super::support::decimal256;
    use super::support::string;
    use super::support::tuple;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "any(x0)",
            arguments: vec!["Boolean"],
            result: "Nullable(Boolean)",
            state: "Tuple(Boolean, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "any(x0)",
            arguments: vec!["Date"],
            result: "Nullable(Date)",
            state: "Tuple(Boolean, Date, Boolean)",
        },
        Case::Metadata {
            expression: "any(x0)",
            arguments: vec!["Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Boolean, Float64, Boolean)",
        },
        Case::Metadata {
            expression: "any(x0)",
            arguments: vec!["Int64"],
            result: "Nullable(Int64)",
            state: "Tuple(Boolean, Int64, Boolean)",
        },
        Case::Metadata {
            expression: "any(x0)",
            arguments: vec!["Timestamp"],
            result: "Nullable(Timestamp)",
            state: "Tuple(Boolean, Timestamp, Boolean)",
        },
        Case::Metadata {
            expression: "any(x0)",
            arguments: vec!["Decimal(38, 6)"],
            result: "Nullable(Decimal(38, 6))",
            state: "Tuple(Nullable(Decimal(38, 0)), Boolean)",
        },
        Case::Metadata {
            expression: "any(x0)",
            arguments: vec!["Decimal(76, 12)"],
            result: "Nullable(Decimal(76, 12))",
            state: "Tuple(Nullable(Decimal(76, 0)), Boolean)",
        },
        Case::Metadata {
            expression: "any(x0)",
            arguments: vec!["Nullable(Int64)"],
            result: "Nullable(Int64)",
            state: "Tuple(Boolean, Int64, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "any(x0)",
            arguments: vec!["Tuple(String, Int64)"],
            result: "Nullable(Tuple(String, Int64))",
            state: "Tuple(Boolean, Tuple(String, Int64), Boolean)",
        },
        Case::Metadata {
            expression: "max(x0)",
            arguments: vec!["Boolean"],
            result: "Nullable(Boolean)",
            state: "Tuple(Boolean, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "max(x0)",
            arguments: vec!["Date"],
            result: "Nullable(Date)",
            state: "Tuple(Boolean, Date, Boolean)",
        },
        Case::Metadata {
            expression: "max(x0)",
            arguments: vec!["Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Boolean, Float64, Boolean)",
        },
        Case::Metadata {
            expression: "max(x0)",
            arguments: vec!["Int64"],
            result: "Nullable(Int64)",
            state: "Tuple(Boolean, Int64, Boolean)",
        },
        Case::Metadata {
            expression: "max(x0)",
            arguments: vec!["Timestamp"],
            result: "Nullable(Timestamp)",
            state: "Tuple(Boolean, Timestamp, Boolean)",
        },
        Case::Metadata {
            expression: "max(x0)",
            arguments: vec!["Decimal(38, 6)"],
            result: "Nullable(Decimal(38, 6))",
            state: "Tuple(Nullable(Decimal(38, 0)), Boolean)",
        },
        Case::Metadata {
            expression: "max(x0)",
            arguments: vec!["Decimal(76, 12)"],
            result: "Nullable(Decimal(76, 12))",
            state: "Tuple(Nullable(Decimal(76, 0)), Boolean)",
        },
        Case::Metadata {
            expression: "max(x0)",
            arguments: vec!["Nullable(Int64)"],
            result: "Nullable(Int64)",
            state: "Tuple(Boolean, Int64, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "max(x0)",
            arguments: vec!["Tuple(String, Int64)"],
            result: "Nullable(Tuple(String, Int64))",
            state: "Tuple(Boolean, Tuple(String, Int64), Boolean)",
        },
        Case::Metadata {
            expression: "min(x0)",
            arguments: vec!["Boolean"],
            result: "Nullable(Boolean)",
            state: "Tuple(Boolean, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "min(x0)",
            arguments: vec!["Date"],
            result: "Nullable(Date)",
            state: "Tuple(Boolean, Date, Boolean)",
        },
        Case::Metadata {
            expression: "min(x0)",
            arguments: vec!["Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Boolean, Float64, Boolean)",
        },
        Case::Metadata {
            expression: "min(x0)",
            arguments: vec!["Int64"],
            result: "Nullable(Int64)",
            state: "Tuple(Boolean, Int64, Boolean)",
        },
        Case::Metadata {
            expression: "min(x0)",
            arguments: vec!["Timestamp"],
            result: "Nullable(Timestamp)",
            state: "Tuple(Boolean, Timestamp, Boolean)",
        },
        Case::Metadata {
            expression: "min(x0)",
            arguments: vec!["Decimal(38, 6)"],
            result: "Nullable(Decimal(38, 6))",
            state: "Tuple(Nullable(Decimal(38, 0)), Boolean)",
        },
        Case::Metadata {
            expression: "min(x0)",
            arguments: vec!["Decimal(76, 12)"],
            result: "Nullable(Decimal(76, 12))",
            state: "Tuple(Nullable(Decimal(76, 0)), Boolean)",
        },
        Case::Metadata {
            expression: "min(x0)",
            arguments: vec!["Nullable(Int64)"],
            result: "Nullable(Int64)",
            state: "Tuple(Boolean, Int64, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "min(x0)",
            arguments: vec!["Tuple(String, Int64)"],
            result: "Nullable(Tuple(String, Int64))",
            state: "Tuple(Boolean, Tuple(String, Int64), Boolean)",
        },
        Case::Samples {
            expression: "any(x0)",
            arguments: vec!["String"],
            result: "Nullable(String)",
            state: "Tuple(Nullable(String), Boolean)",
            samples: vec![
                Sample {
                    label: "string/false/empty",
                    inputs: vec![StringType::from_data(Vec::<&str>::new())],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/false/mixed",
                    inputs: vec![StringType::from_data(vec!["-220", "", "110", "500", "900"])],
                    state: tuple(vec![string("-220"), Scalar::Boolean(true)]),
                    result: string("-220"),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "any(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "Nullable(Decimal(15, 2))",
            state: "Tuple(Nullable(Decimal(18, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal64/false/empty",
                    inputs: vec![Decimal64Type::from_data_with_size(
                        Vec::<i64>::new(),
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal64/false/mixed",
                    inputs: vec![Decimal64Type::from_data_with_size(
                        vec![-220, 0, 110, 500, 900],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![decimal64(-220, 18, 0), Scalar::Boolean(true)]),
                    result: decimal64(-220, 15, 2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "any(x0)",
            arguments: vec!["Nullable(String)"],
            result: "Nullable(String)",
            state: "Tuple(Nullable(String), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "string/true/empty",
                    inputs: vec![StringType::from_opt_data(Vec::<Option<&str>>::new())],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/true/all_null",
                    inputs: vec![StringType::from_opt_data(vec![None::<&str>; 2])],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/true/mixed",
                    inputs: vec![StringType::from_opt_data(vec![
                        Some("-220"),
                        None,
                        Some("110"),
                        Some("500"),
                        Some("900"),
                    ])],
                    state: tuple(vec![
                        string("-220"),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: string("-220"),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "any(x0)",
            arguments: vec!["Nullable(Decimal(15, 2))"],
            result: "Nullable(Decimal(15, 2))",
            state: "Tuple(Nullable(Decimal(18, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal64/true/empty",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        Vec::<Option<i64>>::new(),
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal64/true/all_null",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        vec![None::<i64>; 2],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal64/true/mixed",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        vec![Some(-220), None, Some(110), Some(500), Some(900)],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal64(-220, 18, 0),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal64(-220, 15, 2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "max(x0)",
            arguments: vec!["String"],
            result: "Nullable(String)",
            state: "Tuple(Nullable(String), Boolean)",
            samples: vec![
                Sample {
                    label: "string/false/empty",
                    inputs: vec![StringType::from_data(Vec::<&str>::new())],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/false/mixed",
                    inputs: vec![StringType::from_data(vec!["-220", "", "110", "500", "900"])],
                    state: tuple(vec![string("900"), Scalar::Boolean(true)]),
                    result: string("900"),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "max(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "Nullable(Decimal(15, 2))",
            state: "Tuple(Nullable(Decimal(18, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal64/false/empty",
                    inputs: vec![Decimal64Type::from_data_with_size(
                        Vec::<i64>::new(),
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal64/false/mixed",
                    inputs: vec![Decimal64Type::from_data_with_size(
                        vec![-220, 0, 110, 500, 900],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![decimal64(900, 18, 0), Scalar::Boolean(true)]),
                    result: decimal64(900, 15, 2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "max(x0)",
            arguments: vec!["Nullable(String)"],
            result: "Nullable(String)",
            state: "Tuple(Nullable(String), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "string/true/empty",
                    inputs: vec![StringType::from_opt_data(Vec::<Option<&str>>::new())],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/true/all_null",
                    inputs: vec![StringType::from_opt_data(vec![None::<&str>; 2])],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/true/mixed",
                    inputs: vec![StringType::from_opt_data(vec![
                        Some("-220"),
                        None,
                        Some("110"),
                        Some("500"),
                        Some("900"),
                    ])],
                    state: tuple(vec![
                        string("900"),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: string("900"),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "max(x0)",
            arguments: vec!["Nullable(Decimal(15, 2))"],
            result: "Nullable(Decimal(15, 2))",
            state: "Tuple(Nullable(Decimal(18, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal64/true/empty",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        Vec::<Option<i64>>::new(),
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal64/true/all_null",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        vec![None::<i64>; 2],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal64/true/mixed",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        vec![Some(-220), None, Some(110), Some(500), Some(900)],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal64(900, 18, 0),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal64(900, 15, 2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "min(x0)",
            arguments: vec!["String"],
            result: "Nullable(String)",
            state: "Tuple(Nullable(String), Boolean)",
            samples: vec![
                Sample {
                    label: "string/false/empty",
                    inputs: vec![StringType::from_data(Vec::<&str>::new())],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/false/mixed",
                    inputs: vec![StringType::from_data(vec!["-220", "", "110", "500", "900"])],
                    state: tuple(vec![string(""), Scalar::Boolean(true)]),
                    result: string(""),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "min(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "Nullable(Decimal(15, 2))",
            state: "Tuple(Nullable(Decimal(18, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal64/false/empty",
                    inputs: vec![Decimal64Type::from_data_with_size(
                        Vec::<i64>::new(),
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal64/false/mixed",
                    inputs: vec![Decimal64Type::from_data_with_size(
                        vec![-220, 0, 110, 500, 900],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![decimal64(-220, 18, 0), Scalar::Boolean(true)]),
                    result: decimal64(-220, 15, 2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "min(x0)",
            arguments: vec!["Nullable(String)"],
            result: "Nullable(String)",
            state: "Tuple(Nullable(String), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "string/true/empty",
                    inputs: vec![StringType::from_opt_data(Vec::<Option<&str>>::new())],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/true/all_null",
                    inputs: vec![StringType::from_opt_data(vec![None::<&str>; 2])],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/true/mixed",
                    inputs: vec![StringType::from_opt_data(vec![
                        Some("-220"),
                        None,
                        Some("110"),
                        Some("500"),
                        Some("900"),
                    ])],
                    state: tuple(vec![
                        string("-220"),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: string("-220"),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "min(x0)",
            arguments: vec!["Nullable(Decimal(15, 2))"],
            result: "Nullable(Decimal(15, 2))",
            state: "Tuple(Nullable(Decimal(18, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal64/true/empty",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        Vec::<Option<i64>>::new(),
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal64/true/all_null",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        vec![None::<i64>; 2],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal64/true/mixed",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        vec![Some(-220), None, Some(110), Some(500), Some(900)],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal64(-220, 18, 0),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal64(-220, 15, 2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "min(x0)",
            arguments: vec!["Decimal(30, 5)"],
            result: "Nullable(Decimal(30, 5))",
            state: "Tuple(Nullable(Decimal(38, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal128/false/empty",
                    inputs: vec![Decimal128Type::from_data_with_size(
                        Vec::<i128>::new(),
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal128/false/mixed",
                    inputs: vec![Decimal128Type::from_data_with_size(
                        vec![-220, 0, 110, 500, 900],
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![decimal128(-220, 38, 0), Scalar::Boolean(true)]),
                    result: decimal128(-220, 30, 5),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "min(x0)",
            arguments: vec!["Nullable(Decimal(30, 5))"],
            result: "Nullable(Decimal(30, 5))",
            state: "Tuple(Nullable(Decimal(38, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal128/true/empty",
                    inputs: vec![Decimal128Type::from_opt_data_with_size(
                        Vec::<Option<i128>>::new(),
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal128/true/all_null",
                    inputs: vec![Decimal128Type::from_opt_data_with_size(
                        vec![None::<i128>; 2],
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal128/true/mixed",
                    inputs: vec![Decimal128Type::from_opt_data_with_size(
                        vec![Some(-220), None, Some(110), Some(500), Some(900)],
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal128(-220, 38, 0),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal128(-220, 30, 5),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "max(x0)",
            arguments: vec!["Decimal(30, 5)"],
            result: "Nullable(Decimal(30, 5))",
            state: "Tuple(Nullable(Decimal(38, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal128/false/empty",
                    inputs: vec![Decimal128Type::from_data_with_size(
                        Vec::<i128>::new(),
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal128/false/mixed",
                    inputs: vec![Decimal128Type::from_data_with_size(
                        vec![-220, 0, 110, 500, 900],
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![decimal128(900, 38, 0), Scalar::Boolean(true)]),
                    result: decimal128(900, 30, 5),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "max(x0)",
            arguments: vec!["Nullable(Decimal(30, 5))"],
            result: "Nullable(Decimal(30, 5))",
            state: "Tuple(Nullable(Decimal(38, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal128/true/empty",
                    inputs: vec![Decimal128Type::from_opt_data_with_size(
                        Vec::<Option<i128>>::new(),
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal128/true/all_null",
                    inputs: vec![Decimal128Type::from_opt_data_with_size(
                        vec![None::<i128>; 2],
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal128/true/mixed",
                    inputs: vec![Decimal128Type::from_opt_data_with_size(
                        vec![Some(-220), None, Some(110), Some(500), Some(900)],
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal128(900, 38, 0),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal128(900, 30, 5),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "any(x0)",
            arguments: vec!["Decimal(30, 5)"],
            result: "Nullable(Decimal(30, 5))",
            state: "Tuple(Nullable(Decimal(38, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal128/false/empty",
                    inputs: vec![Decimal128Type::from_data_with_size(
                        Vec::<i128>::new(),
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal128/false/mixed",
                    inputs: vec![Decimal128Type::from_data_with_size(
                        vec![-220, 0, 110, 500, 900],
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![decimal128(-220, 38, 0), Scalar::Boolean(true)]),
                    result: decimal128(-220, 30, 5),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "any(x0)",
            arguments: vec!["Nullable(Decimal(30, 5))"],
            result: "Nullable(Decimal(30, 5))",
            state: "Tuple(Nullable(Decimal(38, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal128/true/empty",
                    inputs: vec![Decimal128Type::from_opt_data_with_size(
                        Vec::<Option<i128>>::new(),
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal128/true/all_null",
                    inputs: vec![Decimal128Type::from_opt_data_with_size(
                        vec![None::<i128>; 2],
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal128/true/mixed",
                    inputs: vec![Decimal128Type::from_opt_data_with_size(
                        vec![Some(-220), None, Some(110), Some(500), Some(900)],
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal128(-220, 38, 0),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal128(-220, 30, 5),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "min(x0)",
            arguments: vec!["Decimal(50, 8)"],
            result: "Nullable(Decimal(50, 8))",
            state: "Tuple(Nullable(Decimal(76, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal256/false/empty",
                    inputs: vec![Decimal256Type::from_data_with_size(
                        Vec::<i256>::new(),
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal256/false/mixed",
                    inputs: vec![Decimal256Type::from_data_with_size(
                        vec![
                            i256::from(-220i128),
                            i256::from(0i128),
                            i256::from(110i128),
                            i256::from(500i128),
                            i256::from(900i128),
                        ],
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal256(i256::from(-220i128), 76, 0),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal256(i256::from(-220i128), 50, 8),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "min(x0)",
            arguments: vec!["Nullable(Decimal(50, 8))"],
            result: "Nullable(Decimal(50, 8))",
            state: "Tuple(Nullable(Decimal(76, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal256/true/empty",
                    inputs: vec![Decimal256Type::from_opt_data_with_size(
                        Vec::<Option<i256>>::new(),
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal256/true/all_null",
                    inputs: vec![Decimal256Type::from_opt_data_with_size(
                        vec![None::<i256>; 2],
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal256/true/mixed",
                    inputs: vec![Decimal256Type::from_opt_data_with_size(
                        vec![
                            Some(i256::from(-220i128)),
                            None,
                            Some(i256::from(110i128)),
                            Some(i256::from(500i128)),
                            Some(i256::from(900i128)),
                        ],
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal256(i256::from(-220i128), 76, 0),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal256(i256::from(-220i128), 50, 8),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "max(x0)",
            arguments: vec!["Decimal(50, 8)"],
            result: "Nullable(Decimal(50, 8))",
            state: "Tuple(Nullable(Decimal(76, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal256/false/empty",
                    inputs: vec![Decimal256Type::from_data_with_size(
                        Vec::<i256>::new(),
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal256/false/mixed",
                    inputs: vec![Decimal256Type::from_data_with_size(
                        vec![
                            i256::from(-220i128),
                            i256::from(0i128),
                            i256::from(110i128),
                            i256::from(500i128),
                            i256::from(900i128),
                        ],
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal256(i256::from(900i128), 76, 0),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal256(i256::from(900i128), 50, 8),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "max(x0)",
            arguments: vec!["Nullable(Decimal(50, 8))"],
            result: "Nullable(Decimal(50, 8))",
            state: "Tuple(Nullable(Decimal(76, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal256/true/empty",
                    inputs: vec![Decimal256Type::from_opt_data_with_size(
                        Vec::<Option<i256>>::new(),
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal256/true/all_null",
                    inputs: vec![Decimal256Type::from_opt_data_with_size(
                        vec![None::<i256>; 2],
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal256/true/mixed",
                    inputs: vec![Decimal256Type::from_opt_data_with_size(
                        vec![
                            Some(i256::from(-220i128)),
                            None,
                            Some(i256::from(110i128)),
                            Some(i256::from(500i128)),
                            Some(i256::from(900i128)),
                        ],
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal256(i256::from(900i128), 76, 0),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal256(i256::from(900i128), 50, 8),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "any(x0)",
            arguments: vec!["Decimal(50, 8)"],
            result: "Nullable(Decimal(50, 8))",
            state: "Tuple(Nullable(Decimal(76, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal256/false/empty",
                    inputs: vec![Decimal256Type::from_data_with_size(
                        Vec::<i256>::new(),
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![Scalar::Null, Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal256/false/mixed",
                    inputs: vec![Decimal256Type::from_data_with_size(
                        vec![
                            i256::from(-220i128),
                            i256::from(0i128),
                            i256::from(110i128),
                            i256::from(500i128),
                            i256::from(900i128),
                        ],
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal256(i256::from(-220i128), 76, 0),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal256(i256::from(-220i128), 50, 8),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "any(x0)",
            arguments: vec!["Nullable(Decimal(50, 8))"],
            result: "Nullable(Decimal(50, 8))",
            state: "Tuple(Nullable(Decimal(76, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal256/true/empty",
                    inputs: vec![Decimal256Type::from_opt_data_with_size(
                        Vec::<Option<i256>>::new(),
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal256/true/all_null",
                    inputs: vec![Decimal256Type::from_opt_data_with_size(
                        vec![None::<i256>; 2],
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Null,
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal256/true/mixed",
                    inputs: vec![Decimal256Type::from_opt_data_with_size(
                        vec![
                            Some(i256::from(-220i128)),
                            None,
                            Some(i256::from(110i128)),
                            Some(i256::from(500i128)),
                            Some(i256::from(900i128)),
                        ],
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        decimal256(i256::from(-220i128), 76, 0),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal256(i256::from(-220i128), 50, 8),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
    ]);
}
