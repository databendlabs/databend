use std::io::Write;

use databend_common_expression::FromData;
use databend_common_expression::SymbolOrOffset;
use databend_common_functions::aggregates::AggregateFunctionSortDesc;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_list_agg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "s",
            databend_common_expression::types::StringType::from_data(vec![
                "abc", "def", "opq", "xyz",
            ])
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
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "listagg(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "listagg(s)", columns, simulator, vec![
        AggregateFunctionSortDesc {
            index: SymbolOrOffset::Offset(0),
            is_reuse_index: true,
            data_type: columns[0].1.data_type(),
            nulls_first: false,
            asc: false,
        },
    ]);
    write_aggregate_expr_case(file, "listagg(s_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "listagg(s, '|')", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "listagg(s_null, '-')", columns, simulator, vec![]);
}

#[test]
fn test_list_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("list_agg.txt").unwrap();
    run_list_agg_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_list_agg_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("list_agg_group_by.txt").unwrap();
    run_list_agg_cases(file, simulate_two_groups_group_by);
}
