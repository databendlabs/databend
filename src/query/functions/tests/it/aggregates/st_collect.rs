use std::io::Write;

use goldenfile::Mint;

use super::aggregate_case_fixtures as fixtures;
use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::write_aggregate_expr_case;

fn run_st_collect_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry('point(10 20)'))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry('srid=4326;linestring(10 20, 40 50)'))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(NULL)",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );

    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(point))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(point_null))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(point_all_null))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(line_string))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(line_string_null))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(polygon))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(mixed_geom))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(mixed_geom_null))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(point_4326))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(line_string_4326))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(polygon_4326))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(mixed_3857))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(mixed_srid))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(mixed_srid_null))",
        fixtures::geometry_columns().as_slice(),
        simulator,
        vec![],
    );
}

#[test]
fn test_st_collect() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("st_collect.txt").unwrap();
    run_st_collect_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_st_collect_group_by_golden_preserves_single_group_order() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("st_collect_group_by.txt").unwrap();
    run_st_collect_cases(file, eval_legacy_aggregate);
}
