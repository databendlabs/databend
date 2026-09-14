use std::io::Write;

use goldenfile::Mint;

use super::aggregate_case_fixtures as fixtures;
use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_st_intersection_agg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    write_aggregate_expr_case(
        file,
        "st_intersection_agg(to_geometry(polygon_overlap))",
        fixtures::overlapping_geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_intersection_agg(NULL)",
        fixtures::overlapping_geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_intersection_agg(to_geometry(polygon_overlap_null))",
        fixtures::overlapping_geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_intersection_agg(to_geometry(polygon_overlap_all_null))",
        fixtures::overlapping_geometry_columns().as_slice(),
        simulator,
        vec![],
    );
}

fn run_st_intersection_agg_distinct_cases(
    file: &mut impl Write,
    simulator: impl AggregationSimulator,
) {
    write_aggregate_expr_case(
        file,
        "st_intersection_agg_distinct(to_geometry(polygon_overlap))",
        fixtures::overlapping_geometry_columns().as_slice(),
        simulator,
        vec![],
    );
}

#[test]
fn test_st_intersection_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("st_intersection_agg.txt").unwrap();
    run_st_intersection_agg_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_st_intersection_agg_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint
        .new_goldenfile("st_intersection_agg_group_by.txt")
        .unwrap();
    run_st_intersection_agg_cases(file, simulate_two_groups_group_by);
    run_st_intersection_agg_distinct_cases(file, simulate_two_groups_group_by);
}
