use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::SymbolOrOffset;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_functions::aggregates::AggregateFunctionSortDesc;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_string_agg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "s",
            StringType::from_data(vec!["abc", "def", "opq", "xyz"]).into(),
        ),
        ("a", Int64Type::from_data(vec![4i64, 3, 2, 1]).into()),
        (
            "event",
            BooleanType::from_data(vec![true, false, true, false]).into(),
        ),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![400_i64, 300, 200, 100],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        ("date_col", DateType::from_data(vec![1, 2, 1, 3]).into()),
        ("ts", TimestampType::from_data(vec![10, 20, 10, 30]).into()),
        (
            "json",
            StringType::from_data(vec![r#"{"k":1}"#, r#"{"k":2}"#, r#"{"k":1}"#, r#"null"#]).into(),
        ),
        (
            "s_all_null",
            StringType::from_data_with_validity(vec!["a", "b", "c", "d"], vec![
                false, false, false, false,
            ])
            .into(),
        ),
        (
            "s_null",
            StringType::from_data_with_validity(vec!["a", "", "c", "d"], vec![
                true, false, true, true,
            ])
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "string_agg(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(s)", columns, simulator, vec![
        AggregateFunctionSortDesc {
            index: SymbolOrOffset::Offset(0),
            is_reuse_index: true,
            data_type: columns[0].1.data_type(),
            nulls_first: false,
            asc: false,
        },
    ]);
    write_aggregate_expr_case(file, "string_agg(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(s_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(s_all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(s, '|')", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(a, '|')", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(event)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(date_col)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(ts)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "string_agg(parse_json(json))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "string_agg(NULL, '|')", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(s_null, '-')", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "group_concat(s)", columns, simulator, vec![
        AggregateFunctionSortDesc {
            index: SymbolOrOffset::Offset(0),
            is_reuse_index: true,
            data_type: columns[0].1.data_type(),
            nulls_first: false,
            asc: false,
        },
    ]);
    write_aggregate_expr_case(
        file,
        "string_agg(s_all_null, '|')",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_string_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("string_agg.txt").unwrap();
    run_string_agg_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_string_agg_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("string_agg_group_by.txt").unwrap();
    run_string_agg_cases(file, simulate_two_groups_group_by);
}
