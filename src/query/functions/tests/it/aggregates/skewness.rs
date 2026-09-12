use std::io::Write;

use databend_common_exception::Result;
use databend_common_expression::FromData;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::UInt64Type;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::assert_single_float_close;
use super::support::eval_aggregate;
use super::support::eval_v2_aggr;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_skewness_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        ("flat", UInt64Type::from_data(vec![7, 7, 7, 7]).into()),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![110_i64, 220, 330, 440],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
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

    write_aggregate_expr_case(file, "skewness(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "skewness(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "skewness(flat)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "skewness(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "skewness(x_null)", columns, simulator, vec![]);
}

#[test]
fn test_skewness() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("skewness.txt").unwrap();
    run_skewness_cases(file, eval_aggregate);
}

#[test]
fn test_skewness_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("skewness_group_by.txt").unwrap();
    run_skewness_cases(file, simulate_two_groups_group_by);
}

#[test]
fn test_v2_skewness_matches_expected_formula() -> Result<()> {
    let entries = [UInt64Type::from_data(vec![1, 2, 3, 10]).into()];
    let direct_v2 = eval_v2_aggr("skewness", &entries, 4, false)?;
    let serialized_v2 = eval_v2_aggr("skewness", &entries, 4, true)?;

    assert_single_float_close(&direct_v2, 1.763632614803888);
    assert_eq!(serialized_v2, direct_v2);
    Ok(())
}

// moments.rs: each moment kernel shares Float64 state across numeric inputs;
// keep the decimal conversion branches and one nullable numeric input.
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
            expression: "skewness(x0)",
            arguments: vec!["Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "skewness(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "skewness(x0)",
            arguments: vec!["Decimal(38, 6)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "skewness(x0)",
            arguments: vec!["Decimal(76, 12)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "skewness(x0)",
            arguments: vec!["Nullable(Float64)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean, Boolean)",
        },
        Case::Samples {
            expression: "skewness(x0)",
            arguments: vec!["Nullable(Int64)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "empty",
                    inputs: vec![Int64Type::from_opt_data(Vec::<Option<i64>>::new())],
                    state: tuple(vec![
                        binary("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
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
                        binary("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
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
                        binary("BAAAAAAAAAAAAAAAAAAyQAAAAAAAgFxAAAAAAAAwi0A="),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: float64(1.0964048893736857),
                    merge_result: MergeResult::Skip,
                },
            ],
        },
    ]);
}
