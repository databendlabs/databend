use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

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
    run_arg_max_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_arg_max_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("arg_max_group_by.txt").unwrap();
    run_arg_max_cases(file, simulate_two_groups_group_by);
}
