use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::aggregate_case_fixtures as fixtures;
use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_bitmap_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "b",
            databend_common_expression::types::number::UInt64Type::from_data(vec![1u64, 2, 2, 3])
                .into(),
        ),
        ("bm", fixtures::bitmap_column().into()),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "bitmap_union(bm)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "bitmap_union_distinct(bm)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "bitmap_intersect(bm)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "bitmap_intersect_distinct(bm)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "bitmap_xor_agg(bm)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "bitmap_construct_agg(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "bitmap_construct_agg_distinct(b)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "group_bitmap(b)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "group_bitmap_distinct(b)", columns, simulator, vec![]);
}

#[test]
fn test_bitmap() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("bitmap.txt").unwrap();
    run_bitmap_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_bitmap_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("bitmap_group_by.txt").unwrap();
    run_bitmap_cases(file, simulate_two_groups_group_by);
}
