use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_aggregate_if_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
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
            "x_null",
            databend_common_expression::types::number::UInt64Type::from_data_with_validity(
                vec![1u64, 2, 3, 4],
                vec![true, true, false, false],
            )
            .into(),
        ),
        (
            "cond_nullable",
            databend_common_expression::types::BooleanType::from_data_with_validity(
                vec![true, true, false, true],
                vec![true, false, true, true],
            )
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(
        file,
        "count_if(1, x_null is null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "count_if(1, false)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "sum_if(a, x_null is null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "sum_if(b, x_null is null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "count_if(cond_nullable)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "count_if(x_null, cond_nullable)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "sum_if(b, cond_nullable)", columns, simulator, vec![]);
    // `sum_if(b, false)` and `sum_if(b, NULL)` are intentionally not covered
    // by this legacy-backed golden yet: old batch evaluation returns NULL, but
    // old per-row evaluation returns nullable 0 for the all-rejected case.
}

#[test]
fn test_aggregate_if() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("aggregate_if.txt").unwrap();
    run_aggregate_if_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_aggregate_if_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("aggregate_if_group_by.txt").unwrap();
    run_aggregate_if_cases(file, simulate_two_groups_group_by);
}
