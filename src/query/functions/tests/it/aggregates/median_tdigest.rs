use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_median_tdigest_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
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
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "median_tdigest(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "median_tdigest(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "median_tdigest(x_null)", columns, simulator, vec![]);
}

#[test]
fn test_median_tdigest() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("median_tdigest.txt").unwrap();
    run_median_tdigest_cases(file, eval_aggregate);
}

#[test]
fn test_median_tdigest_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("median_tdigest_group_by.txt").unwrap();
    run_median_tdigest_cases(file, simulate_two_groups_group_by);
}

// quantile_tdigest.rs: numeric inputs share the digest; retain three decimal
// conversion widths and the nullable adaptor. Explicit levels are out of scope.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::Int64Type;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::binary;
    use super::support::float64;
    use super::support::tuple;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "median_tdigest(x0)",
            arguments: vec!["Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "median_tdigest(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "median_tdigest(x0)",
            arguments: vec!["Decimal(38, 6)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "median_tdigest(x0)",
            arguments: vec!["Decimal(76, 12)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "median_tdigest(x0)",
            arguments: vec!["Nullable(Float64)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean, Boolean)",
        },
        Case::Samples {
            expression: "median_tdigest(x0)",
            arguments: vec!["Nullable(Int64)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "empty",
                    inputs: vec![Int64Type::from_opt_data(Vec::<Option<i64>>::new())],
                    state: tuple(vec![
                        binary("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPB/AAAAAAAA8P8="),
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
                        binary("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPB/AAAAAAAA8P8="),
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
                            "AAAAAAAAAAAAAAAAAAAAAAAAABBABAAAAAAAAAAAAABAAAAAAAAA8D8AAAAAAAAAQAAAAAAAAPA/AAAAAAAAFEAAAAAAAADwPwAAAAAAACJAAAAAAAAA8D8AAAAAAADwfwAAAAAAAPD/",
                        ),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: float64(5.0),
                    merge_result: MergeResult::Skip,
                },
            ],
        },
    ]);
}
