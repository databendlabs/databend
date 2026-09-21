use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_arg_max_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "a",
            databend_common_expression::types::number::Int64Type::from_data(vec![4i64, 3, 2, 1])
                .into(),
        ),
        ("b", UInt64Type::from_data(vec![1u64, 2, 3, 4]).into()),
        (
            "s",
            StringType::from_data(vec!["abc", "def", "opq", "xyz"]).into(),
        ),
        (
            "event",
            BooleanType::from_data(vec![true, false, true, false]).into(),
        ),
        ("date_col", DateType::from_data(vec![1, 2, 3, 4]).into()),
        ("ts", TimestampType::from_data(vec![10, 20, 30, 40]).into()),
        (
            "json",
            StringType::from_data(vec![r#"{"k":1}"#, r#"{"k":2}"#, r#"{"k":3}"#, r#"{"k":4}"#])
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
            "y_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![false, false, true, true],
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

    write_aggregate_expr_case(file, "arg_max(a, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max_distinct(a, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(b, a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(a, s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(a, event)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(a, date_col)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(a, ts)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(a, [])", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(a, {})", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "arg_max(a, parse_json(json))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "arg_max(NULL, a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(a, NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(const_int, a)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "arg_max(const_int_null, a)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "arg_max(a, const_int)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "arg_max(a, const_int_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "arg_max(y_null, a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(a, y_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(all_null, a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_max(a, all_null)", columns, simulator, vec![]);
}

#[test]
fn test_arg_max() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("arg_max.txt").unwrap();
    run_arg_max_cases(file, eval_aggregate);
}

#[test]
fn test_arg_max_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("arg_max_group_by.txt").unwrap();
    run_arg_max_cases(file, simulate_two_groups_group_by);
}

// arg_min_max.rs: both argument and comparison dispatch have String, Boolean,
// Date, Timestamp, AnyNumberType and AnyType branches. Diagonal representatives
// plus heterogeneous calls cover them without a type cross product.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::Int64Type;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::int64;
    use super::support::tuple;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "arg_max(x0, x1)",
            arguments: vec!["Boolean", "Boolean"],
            result: "Nullable(Boolean)",
            state: "Tuple(Boolean, Boolean, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "arg_max(x0, x1)",
            arguments: vec!["Date", "Date"],
            result: "Nullable(Date)",
            state: "Tuple(Boolean, Date, Date, Boolean)",
        },
        Case::Metadata {
            expression: "arg_max(x0, x1)",
            arguments: vec!["Int64", "Int64"],
            result: "Nullable(Int64)",
            state: "Tuple(Boolean, Int64, Int64, Boolean)",
        },
        Case::Metadata {
            expression: "arg_max(x0, x1)",
            arguments: vec!["Int64", "String"],
            result: "Nullable(Int64)",
            state: "Tuple(Boolean, String, Int64, Boolean)",
        },
        Case::Metadata {
            expression: "arg_max(x0, x1)",
            arguments: vec!["String", "String"],
            result: "Nullable(String)",
            state: "Tuple(Boolean, String, String, Boolean)",
        },
        Case::Metadata {
            expression: "arg_max(x0, x1)",
            arguments: vec!["Timestamp", "Timestamp"],
            result: "Nullable(Timestamp)",
            state: "Tuple(Boolean, Timestamp, Timestamp, Boolean)",
        },
        Case::Metadata {
            expression: "arg_max(x0, x1)",
            arguments: vec!["Nullable(Int64)", "Int64"],
            result: "Nullable(Int64)",
            state: "Tuple(Boolean, Int64, Int64, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "arg_max(x0, x1)",
            arguments: vec!["Tuple(String, Int64)", "Tuple(String, Int64)"],
            result: "Nullable(Tuple(String, Int64))",
            state: "Tuple(Boolean, Tuple(String, Int64), Tuple(String, Int64), Boolean)",
        },
        Case::Samples {
            expression: "arg_max(x0, x1)",
            arguments: vec!["Nullable(Int64)", "Nullable(Int64)"],
            result: "Nullable(Int64)",
            state: "Tuple(Boolean, Int64, Int64, Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "empty",
                    inputs: vec![
                        Int64Type::from_opt_data(Vec::<Option<i64>>::new()),
                        Int64Type::from_opt_data(Vec::<Option<i64>>::new()),
                    ],
                    state: tuple(vec![
                        Scalar::Boolean(false),
                        int64(0),
                        int64(0),
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::Skip,
                },
                Sample {
                    label: "all_null",
                    inputs: vec![
                        Int64Type::from_opt_data(vec![None::<i64>; 2]),
                        Int64Type::from_opt_data(vec![None::<i64>; 2]),
                    ],
                    state: tuple(vec![
                        Scalar::Boolean(false),
                        int64(0),
                        int64(0),
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::Skip,
                },
                Sample {
                    label: "mixed",
                    inputs: vec![
                        Int64Type::from_opt_data(vec![Some(2), None, Some(2), Some(5), Some(9)]),
                        Int64Type::from_opt_data(vec![Some(2), None, Some(2), Some(5), Some(9)]),
                    ],
                    state: tuple(vec![
                        Scalar::Boolean(true),
                        int64(9),
                        int64(9),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: int64(9),
                    merge_result: MergeResult::Skip,
                },
            ],
        },
    ]);
}
