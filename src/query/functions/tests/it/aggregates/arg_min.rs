use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_arg_min_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
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
            StringType::from_data(vec![r#"{"k":4}"#, r#"{"k":3}"#, r#"{"k":2}"#, r#"{"k":1}"#])
                .into(),
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

    write_aggregate_expr_case(file, "arg_min(a, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min_distinct(a, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min(b, a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min(s, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min(event, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min(date_col, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min(ts, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min([], b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min({}, b)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "arg_min(parse_json(json), b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "arg_min(NULL, a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min(a, NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min(y_null, a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min(a, y_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min(all_null, a)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "arg_min(a, all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "arg_min(to_nullable(NULL), a)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "arg_min_if(to_nullable(NULL), a, b > 1)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "arg_min_state(to_nullable(NULL), a)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "arg_min_distinct(to_nullable(NULL), a)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_arg_min() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("arg_min.txt").unwrap();
    run_arg_min_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_arg_min_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("arg_min_group_by.txt").unwrap();
    run_arg_min_cases(file, simulate_two_groups_group_by);
}
