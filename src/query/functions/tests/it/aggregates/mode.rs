use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_mode_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        ("d", UInt64Type::from_data(vec![1u64, 1, 1, 1]).into()),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![110, 110, 110, 110],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        (
            "s",
            StringType::from_data(vec!["abc", "abc", "abc", "abc"]).into(),
        ),
        (
            "json",
            StringType::from_data(vec![r#"{"k":1}"#, r#"{"k":1}"#, r#"{"k":1}"#, r#"{"k":1}"#])
                .into(),
        ),
        (
            "x_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![2u64, 2, 2, 3],
                vec![true, false, true, true],
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

    write_aggregate_expr_case(file, "mode(1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(d)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(parse_json(json))", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "mode(all_null)", columns, simulator, vec![]);
}

#[test]
fn test_mode() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("mode.txt").unwrap();
    run_mode_cases(file, eval_aggregate);
}

#[test]
fn test_mode_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("mode_group_by.txt").unwrap();
    run_mode_cases(file, simulate_two_groups_group_by);
}

// mode.rs: NumberType, three DecimalType widths, and AnyType state paths.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::Int64Type;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::binary;
    use super::support::int64;
    use super::support::tuple;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "mode(x0)",
            arguments: vec!["Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "mode(x0)",
            arguments: vec!["Int64"],
            result: "Nullable(Int64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "mode(x0)",
            arguments: vec!["String"],
            result: "Nullable(String)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "mode(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "Nullable(Decimal(15, 2))",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "mode(x0)",
            arguments: vec!["Decimal(38, 6)"],
            result: "Nullable(Decimal(38, 6))",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "mode(x0)",
            arguments: vec!["Decimal(76, 12)"],
            result: "Nullable(Decimal(76, 12))",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Samples {
            expression: "mode(x0)",
            arguments: vec!["Nullable(Int64)"],
            result: "Nullable(Int64)",
            state: "Tuple(Binary, Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "empty",
                    inputs: vec![Int64Type::from_opt_data(Vec::<Option<i64>>::new())],
                    state: tuple(vec![
                        binary("AAAAAA=="),
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::Skip,
                },
                Sample {
                    label: "all_null",
                    inputs: vec![Int64Type::from_opt_data(vec![None::<i64>; 2])],
                    state: tuple(vec![
                        binary("AAAAAA=="),
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::Skip,
                },
                Sample {
                    label: "mixed",
                    inputs: vec![Int64Type::from_opt_data(vec![
                        Some(2),
                        None,
                        Some(2),
                        Some(5),
                        Some(9),
                    ])],
                    state: tuple(vec![
                        binary(
                            "AwAAAAIAAAAAAAAAAgAAAAAAAAAFAAAAAAAAAAEAAAAAAAAACQAAAAAAAAABAAAAAAAAAA==",
                        ),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: int64(2),
                    merge_result: MergeResult::Skip,
                },
            ],
        },
    ]);
}
