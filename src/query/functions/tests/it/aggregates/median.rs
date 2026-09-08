use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::DecimalSize;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_median_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
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
            "x_all_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![false, false, false, false],
            )
            .into(),
        ),
        (
            "d",
            Decimal64Type::from_data_with_size(
                vec![400_i64, 300, 200, 100],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "median(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "median(d)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "median(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "median(x_all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "median(x_null)", columns, simulator, vec![]);
}

#[test]
fn test_median() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("median.txt").unwrap();
    run_median_cases(file, eval_aggregate);
}

#[test]
fn test_median_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("median_group_by.txt").unwrap();
    run_median_cases(file, simulate_two_groups_group_by);
}

// quantile_cont.rs: Float64 sample storage versus three decimal storage widths.
// Historical samples additionally exercise precision/scale and nullable boundaries.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::Decimal64Type;
    use databend_common_expression::types::Decimal128Type;
    use databend_common_expression::types::Decimal256Type;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::Float64Type;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::i256;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::decimal64;
    use super::support::decimal128;
    use super::support::decimal256;
    use super::support::float64;
    use super::support::tuple;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "median(x0)",
            arguments: vec!["Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Array(Float64), Boolean)",
        },
        Case::Metadata {
            expression: "median(x0)",
            arguments: vec!["Decimal(38, 6)"],
            result: "Nullable(Decimal(38, 6))",
            state: "Tuple(Array(Decimal(38, 0)), Boolean)",
        },
        Case::Metadata {
            expression: "median(x0)",
            arguments: vec!["Decimal(76, 12)"],
            result: "Nullable(Decimal(76, 12))",
            state: "Tuple(Array(Decimal(76, 0)), Boolean)",
        },
        Case::Metadata {
            expression: "median(x0)",
            arguments: vec!["Nullable(Float64)"],
            result: "Nullable(Float64)",
            state: "Tuple(Array(Float64), Boolean, Boolean)",
        },
        Case::Samples {
            expression: "median(x0)",
            arguments: vec!["Int64"],
            result: "Nullable(Float64)",
            state: "Tuple(Array(Float64), Boolean)",
            samples: vec![
                Sample {
                    label: "number/false/empty",
                    inputs: vec![Int64Type::from_data(Vec::<i64>::new())],
                    state: tuple(vec![
                        Scalar::Array(Float64Type::from_data(Vec::<f64>::new())),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "number/false/mixed",
                    inputs: vec![Int64Type::from_data(vec![-220, 0, 110, 500, 900])],
                    state: tuple(vec![
                        Scalar::Array(Float64Type::from_data(vec![
                            -220.0, 0.0, 110.0, 500.0, 900.0,
                        ])),
                        Scalar::Boolean(true),
                    ]),
                    result: float64(110.0),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "median(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "Nullable(Decimal(15, 2))",
            state: "Tuple(Array(Decimal(18, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal64/false/empty",
                    inputs: vec![Decimal64Type::from_data_with_size(
                        Vec::<i64>::new(),
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Array(Decimal64Type::from_data_with_size(
                            Vec::<i64>::new(),
                            Some(DecimalSize::new(18, 0).unwrap()),
                        )),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal64/false/mixed",
                    inputs: vec![Decimal64Type::from_data_with_size(
                        vec![-220, 0, 110, 500, 900],
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Array(Decimal64Type::from_data_with_size(
                            vec![-220, 0, 110, 500, 900],
                            Some(DecimalSize::new(18, 0).unwrap()),
                        )),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal64(110, 15, 2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "median(x0)",
            arguments: vec!["Nullable(Int64)"],
            result: "Nullable(Float64)",
            state: "Tuple(Array(Float64), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "number/true/empty",
                    inputs: vec![Int64Type::from_opt_data(Vec::<Option<i64>>::new())],
                    state: tuple(vec![
                        Scalar::Array(Float64Type::from_data(Vec::<f64>::new())),
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "number/true/all_null",
                    inputs: vec![Int64Type::from_opt_data(vec![None::<i64>; 2])],
                    state: tuple(vec![
                        Scalar::Array(Float64Type::from_data(Vec::<f64>::new())),
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "number/true/mixed",
                    inputs: vec![Int64Type::from_opt_data(vec![
                        Some(-220),
                        None,
                        Some(110),
                        Some(500),
                        Some(900),
                    ])],
                    state: tuple(vec![
                        Scalar::Array(Float64Type::from_data(vec![-220.0, 110.0, 500.0, 900.0])),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: float64(305.0),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "median(x0)",
            arguments: vec!["Nullable(Decimal(15, 2))"],
            result: "Nullable(Decimal(15, 2))",
            state: "Tuple(Array(Decimal(18, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal64/true/empty",
                    inputs: vec![Decimal64Type::from_opt_data_with_size(
                        Vec::<Option<i64>>::new(),
                        Some(DecimalSize::new(15, 2).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Array(Decimal64Type::from_data_with_size(
                            Vec::<i64>::new(),
                            Some(DecimalSize::new(18, 0).unwrap()),
                        )),
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
                        Scalar::Array(Decimal64Type::from_data_with_size(
                            Vec::<i64>::new(),
                            Some(DecimalSize::new(18, 0).unwrap()),
                        )),
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
                        Scalar::Array(Decimal64Type::from_data_with_size(
                            vec![-220, 110, 500, 900],
                            Some(DecimalSize::new(18, 0).unwrap()),
                        )),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal64(110, 15, 2),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "median(x0)",
            arguments: vec!["Decimal(30, 5)"],
            result: "Nullable(Decimal(30, 5))",
            state: "Tuple(Array(Decimal(38, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal128/false/empty",
                    inputs: vec![Decimal128Type::from_data_with_size(
                        Vec::<i128>::new(),
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Array(Decimal128Type::from_data_with_size(
                            Vec::<i128>::new(),
                            Some(DecimalSize::new(38, 0).unwrap()),
                        )),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "decimal128/false/mixed",
                    inputs: vec![Decimal128Type::from_data_with_size(
                        vec![-220, 0, 110, 500, 900],
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Array(Decimal128Type::from_data_with_size(
                            vec![-220, 0, 110, 500, 900],
                            Some(DecimalSize::new(38, 0).unwrap()),
                        )),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal128(110, 30, 5),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "median(x0)",
            arguments: vec!["Nullable(Decimal(30, 5))"],
            result: "Nullable(Decimal(30, 5))",
            state: "Tuple(Array(Decimal(38, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal128/true/empty",
                    inputs: vec![Decimal128Type::from_opt_data_with_size(
                        Vec::<Option<i128>>::new(),
                        Some(DecimalSize::new(30, 5).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Array(Decimal128Type::from_data_with_size(
                            Vec::<i128>::new(),
                            Some(DecimalSize::new(38, 0).unwrap()),
                        )),
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
                        Scalar::Array(Decimal128Type::from_data_with_size(
                            Vec::<i128>::new(),
                            Some(DecimalSize::new(38, 0).unwrap()),
                        )),
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
                        Scalar::Array(Decimal128Type::from_data_with_size(
                            vec![-220, 110, 500, 900],
                            Some(DecimalSize::new(38, 0).unwrap()),
                        )),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal128(110, 30, 5),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "median(x0)",
            arguments: vec!["Decimal(50, 8)"],
            result: "Nullable(Decimal(50, 8))",
            state: "Tuple(Array(Decimal(76, 0)), Boolean)",
            samples: vec![
                Sample {
                    label: "decimal256/false/empty",
                    inputs: vec![Decimal256Type::from_data_with_size(
                        Vec::<i256>::new(),
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Array(Decimal256Type::from_data_with_size(
                            Vec::<i256>::new(),
                            Some(DecimalSize::new(76, 0).unwrap()),
                        )),
                        Scalar::Boolean(false),
                    ]),
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
                        Scalar::Array(Decimal256Type::from_data_with_size(
                            vec![
                                i256::from(-220i128),
                                i256::from(0i128),
                                i256::from(110i128),
                                i256::from(500i128),
                                i256::from(900i128),
                            ],
                            Some(DecimalSize::new(76, 0).unwrap()),
                        )),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal256(i256::from(110i128), 50, 8),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "median(x0)",
            arguments: vec!["Nullable(Decimal(50, 8))"],
            result: "Nullable(Decimal(50, 8))",
            state: "Tuple(Array(Decimal(76, 0)), Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "decimal256/true/empty",
                    inputs: vec![Decimal256Type::from_opt_data_with_size(
                        Vec::<Option<i256>>::new(),
                        Some(DecimalSize::new(50, 8).unwrap()),
                    )],
                    state: tuple(vec![
                        Scalar::Array(Decimal256Type::from_data_with_size(
                            Vec::<i256>::new(),
                            Some(DecimalSize::new(76, 0).unwrap()),
                        )),
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
                        Scalar::Array(Decimal256Type::from_data_with_size(
                            Vec::<i256>::new(),
                            Some(DecimalSize::new(76, 0).unwrap()),
                        )),
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
                        Scalar::Array(Decimal256Type::from_data_with_size(
                            vec![
                                i256::from(-220i128),
                                i256::from(110i128),
                                i256::from(500i128),
                                i256::from(900i128),
                            ],
                            Some(DecimalSize::new(76, 0).unwrap()),
                        )),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: decimal256(i256::from(110i128), 50, 8),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
    ]);
}
