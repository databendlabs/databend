use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

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
    run_json_object_agg_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_json_object_agg_group_by_golden_preserves_single_group_order() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("json_object_agg_group_by.txt").unwrap();
    run_json_object_agg_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_json_object_agg_two_groups() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("json_object_agg_two_groups.txt")
        .unwrap();
    run_json_object_agg_cases(file, simulate_two_groups_group_by);
}
