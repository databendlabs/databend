use std::io::Write;

use goldenfile::Mint;

use super::aggregate_case_fixtures as fixtures;
use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_st_envelope_agg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    write_aggregate_expr_case(
        file,
        "st_envelope_agg(to_geometry(point))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_envelope_agg_distinct(to_geometry(point))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_envelope_agg(to_geometry(line_string))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_envelope_agg(to_geometry(point_null))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_envelope_agg(to_geometry(point_all_null))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_envelope_agg(NULL)",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_envelope_agg(to_geometry(mixed_srid))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
}

#[test]
fn test_st_envelope_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("st_envelope_agg.txt").unwrap();
    run_st_envelope_agg_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_st_envelope_agg_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("st_envelope_agg_group_by.txt").unwrap();
    run_st_envelope_agg_cases(file, simulate_two_groups_group_by);
}
