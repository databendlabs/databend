use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::aggregate_case_fixtures as fixtures;
use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_bitmap_count_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "b",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 3, 4])
                .into(),
        ),
        ("bm", fixtures::bitmap_column().into()),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "bitmap_and_count(bm)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "bitmap_and_count_distinct(bm)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "bitmap_or_count(bm)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "bitmap_or_count_distinct(bm)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "bitmap_xor_count(bm)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "bitmap_not_count(bm)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "intersect_count(1, 2, 3, 4)(bm, b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "intersect_count(1, 2)(bm, b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "intersect_count_distinct(1, 2)(bm, b)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_bitmap_count() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("bitmap_count.txt").unwrap();
    run_bitmap_count_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_bitmap_count_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("bitmap_count_group_by.txt").unwrap();
    run_bitmap_count_cases(file, simulate_two_groups_group_by);
}
