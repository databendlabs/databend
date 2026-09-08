use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_json_object_agg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
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
            "s",
            databend_common_expression::types::StringType::from_data(vec![
                "abc", "def", "opq", "xyz",
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
            "s_null",
            databend_common_expression::types::StringType::from_data_with_validity(
                vec!["a", "", "c", "d"],
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
    let columns = columns.as_slice();

    write_aggregate_expr_case(
        file,
        "json_object_agg('k', 'a')",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "json_object_agg(s, a)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "json_object_agg(s_null, b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "json_object_agg(a, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "json_object_agg(s, dec)", columns, simulator, vec![]);
}

#[test]
fn test_json_object_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("json_object_agg.txt").unwrap();
    run_json_object_agg_cases(file, eval_aggregate);
}

#[test]
fn test_json_object_agg_group_by_golden_preserves_single_group_order() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("json_object_agg_group_by.txt").unwrap();
    run_json_object_agg_cases(file, eval_aggregate);
}

#[test]
fn test_json_object_agg_two_groups() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("json_object_agg_two_groups.txt")
        .unwrap();
    run_json_object_agg_cases(file, simulate_two_groups_group_by);
}

// json_object_agg.rs: String keys and AnyType values share one state. Samples
// distinguish NULL keys/values, JSON null, empty objects and valid pairs.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::VariantType;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::binary;
    use super::support::bytes;
    use super::support::tuple;
    use super::support::variant;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "json_object_agg(x0, x1)",
            arguments: vec!["String", "Int64"],
            result: "Variant",
            state: "Tuple(Binary)",
        },
        Case::Metadata {
            expression: "json_object_agg(x0, x1)",
            arguments: vec!["String", "Variant"],
            result: "Variant",
            state: "Tuple(Binary)",
        },
        Case::Metadata {
            expression: "json_object_agg(x0, x1)",
            arguments: vec!["String", "Array(Int64)"],
            result: "Variant",
            state: "Tuple(Binary)",
        },
        Case::Metadata {
            expression: "json_object_agg(x0, x1)",
            arguments: vec!["Nullable(String)", "Int64"],
            result: "Variant",
            state: "Tuple(Binary)",
        },
        Case::Samples {
            expression: "json_object_agg(x0, x1)",
            arguments: vec!["Nullable(String)", "Variant"],
            result: "Variant",
            state: "Tuple(Binary)",
            samples: vec![Sample {
                label: "json_null",
                inputs: vec![
                    StringType::from_opt_data(vec![Some("k")]),
                    VariantType::from_data(vec![bytes("IAAAAAAAAAA=")]),
                ],
                state: tuple(vec![binary("AQAAAAEAAABrEAgAAAAgAAAAAAAAAA==")]),
                result: variant("QAAAARAAAAEAAAAAaw=="),
                merge_result: MergeResult::Skip,
            }],
        },
        Case::Samples {
            expression: "json_object_agg(x0, x1)",
            arguments: vec!["Nullable(String)", "Nullable(Int64)"],
            result: "Variant",
            state: "Tuple(Binary)",
            samples: vec![
                Sample {
                    label: "empty",
                    inputs: vec![
                        StringType::from_opt_data(Vec::<Option<&str>>::new()),
                        Int64Type::from_opt_data(Vec::<Option<i64>>::new()),
                    ],
                    state: tuple(vec![binary("AAAAAA==")]),
                    result: variant("QAAAAA=="),
                    merge_result: MergeResult::Skip,
                },
                Sample {
                    label: "null_keys",
                    inputs: vec![
                        StringType::from_opt_data(vec![None::<&str>; 2]),
                        Int64Type::from_opt_data(vec![Some(1), Some(2)]),
                    ],
                    state: tuple(vec![binary("AAAAAA==")]),
                    result: variant("QAAAAA=="),
                    merge_result: MergeResult::Skip,
                },
                Sample {
                    label: "null_values",
                    inputs: vec![
                        StringType::from_opt_data(vec![Some("a"), Some("b")]),
                        Int64Type::from_opt_data(vec![None::<i64>; 2]),
                    ],
                    state: tuple(vec![binary("AAAAAA==")]),
                    result: variant("QAAAAA=="),
                    merge_result: MergeResult::Skip,
                },
                Sample {
                    label: "mixed",
                    inputs: vec![
                        StringType::from_opt_data(vec![Some("a"), None, Some("c")]),
                        Int64Type::from_opt_data(vec![Some(1), Some(2), None]),
                    ],
                    state: tuple(vec![binary("AQAAAAEAAABhAwcBAAAAAAAAAA==")]),
                    result: variant("QAAAARAAAAEgAAACYUAB"),
                    merge_result: MergeResult::Skip,
                },
                Sample {
                    label: "values",
                    inputs: vec![
                        StringType::from_opt_data(vec![Some("a"), Some("b")]),
                        Int64Type::from_opt_data(vec![Some(1), Some(2)]),
                    ],
                    state: tuple(vec![binary(
                        "AgAAAAEAAABhAwcBAAAAAAAAAAEAAABiAwcCAAAAAAAAAA==",
                    )]),
                    result: variant("QAAAAhAAAAEQAAABIAAAAiAAAAJhYkABQAI="),
                    merge_result: MergeResult::Skip,
                },
            ],
        },
    ]);
}
