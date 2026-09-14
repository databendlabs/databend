use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_approx_count_distinct_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        ("a", Int64Type::from_data(vec![4i64, 3, 2, 1]).into()),
        ("b", UInt64Type::from_data(vec![1u64, 2, 3, 4]).into()),
        (
            "x_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 2, 3], vec![true, false, true, true])
                .into(),
        ),
        ("s", StringType::from_data(vec!["a", "b", "a", "c"]).into()),
        ("date_col", DateType::from_data(vec![1, 2, 1, 3]).into()),
        ("ts", TimestampType::from_data(vec![10, 20, 10, 30]).into()),
        (
            "json",
            StringType::from_data(vec![r#"{"k":1}"#, r#"{"k":2}"#, r#"{"k":1}"#, r#"null"#]).into(),
        ),
        (
            "all_null",
            UInt64Type::from_data_with_validity(vec![1u64, 2, 3, 4], vec![
                false, false, false, false,
            ])
            .into(),
        ),
    ];
    let columns = columns.as_slice();
    write_aggregate_expr_case(file, "approx_count_distinct(a)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "approx_count_distinct(to_float64(0.5))(a)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "approx_count_distinct(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "approx_count_distinct(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "approx_count_distinct(date_col)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "approx_count_distinct(ts)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "approx_count_distinct(parse_json(json))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "approx_count_distinct(null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "approx_count_distinct(x_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "approx_count_distinct(all_null)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_approx_count_distinct() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("approx_count_distinct.txt").unwrap();
    run_approx_count_distinct_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_approx_count_distinct_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("approx_count_distinct_group_by.txt")
        .unwrap();
    run_approx_count_distinct_cases(file, simulate_two_groups_group_by);
}
