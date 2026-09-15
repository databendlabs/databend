use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_histogram_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        ("b", UInt64Type::from_data(vec![1u64, 2, 3, 4]).into()),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![400, 300, 200, 100],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        ("s", StringType::from_data(vec!["d", "c", "b", "a"]).into()),
        ("dt", TimestampType::from_data(vec![40, 30, 20, 10]).into()),
        ("date_col", DateType::from_data(vec![4, 3, 2, 1]).into()),
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

    write_aggregate_expr_case(file, "histogram(all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(x_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(a, 1)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(dec, 2)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(s, 2)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(dt, 2)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(date_col, 2)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(x_null, 2)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "histogram(all_null, 2)", columns, simulator, vec![]);
}

#[test]
fn test_histogram() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("histogram.txt").unwrap();
    run_histogram_cases(file, eval_aggregate);
}

#[test]
fn test_histogram_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("histogram_group_by.txt").unwrap();
    run_histogram_cases(file, simulate_two_groups_group_by);
}

// histogram.rs: numeric, decimal-width, string, date and timestamp dispatch.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::Int64Type;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::binary;
    use super::support::string;
    use super::support::tuple;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "histogram(x0)",
            arguments: vec!["Date"],
            result: "Nullable(String)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "histogram(x0)",
            arguments: vec!["Float64"],
            result: "Nullable(String)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "histogram(x0)",
            arguments: vec!["Int64"],
            result: "Nullable(String)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "histogram(x0)",
            arguments: vec!["String"],
            result: "Nullable(String)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "histogram(x0)",
            arguments: vec!["Timestamp"],
            result: "Nullable(String)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "histogram(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "Nullable(String)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "histogram(x0)",
            arguments: vec!["Decimal(38, 6)"],
            result: "Nullable(String)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "histogram(x0)",
            arguments: vec!["Decimal(76, 12)"],
            result: "Nullable(String)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Samples {
            expression: "histogram(x0)",
            arguments: vec!["Nullable(Int64)"],
            result: "Nullable(String)",
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
                    result: string(
                        "[{\"lower\":\"2\",\"upper\":\"2\",\"ndv\":1,\"count\":2,\"pre_sum\":0},{\"lower\":\"5\",\"upper\":\"5\",\"ndv\":1,\"count\":1,\"pre_sum\":2},{\"lower\":\"9\",\"upper\":\"9\",\"ndv\":1,\"count\":1,\"pre_sum\":3}]",
                    ),
                    merge_result: MergeResult::Skip,
                },
            ],
        },
    ]);
}
