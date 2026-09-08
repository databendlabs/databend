use std::io::Write;

use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::overlapping_geometry_columns;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_st_intersection_agg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    write_aggregate_expr_case(
        file,
        "st_intersection_agg(to_geometry(polygon_overlap))",
        overlapping_geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_intersection_agg(NULL)",
        overlapping_geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_intersection_agg(to_geometry(polygon_overlap_null))",
        overlapping_geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_intersection_agg(to_geometry(polygon_overlap_all_null))",
        overlapping_geometry_columns().as_slice(),
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
        overlapping_geometry_columns().as_slice(),
        simulator,
        vec![],
    );
}

#[test]
fn test_st_intersection_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("st_intersection_agg.txt").unwrap();
    run_st_intersection_agg_cases(file, eval_aggregate);
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

// geographic.rs: all four geometry operations have distinct computation;
// preserve both nullable forms and all captured EWKB/collection samples.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::GeometryType;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::binary;
    use super::support::bytes;
    use super::support::geometry;
    use super::support::tuple;

    super::support::check_state_baselines(vec![
        Case::Samples {
            expression: "st_intersection_agg(x0)",
            arguments: vec!["Geometry"],
            result: "Nullable(Geometry)",
            state: "Tuple(Binary, Boolean)",
            samples: vec![
                Sample {
                    label: "geometry/false/empty",
                    inputs: vec![GeometryType::from_data(Vec::<Vec<u8>>::new())],
                    state: tuple(vec![binary(""), Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "geometry/false/mixed",
                    inputs: vec![GeometryType::from_data(vec![
                        bytes(
                            "AQMAACDmEAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                        ),
                        bytes(
                            "AQMAACDmEAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAA==",
                        ),
                        bytes(
                            "AQMAACDmEAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                        ),
                    ])],
                    state: tuple(vec![
                        binary(
                            "AQMAACDmEAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAABAAAAAAAAA8D8AAAAAAAAAQA==",
                        ),
                        Scalar::Boolean(true),
                    ]),
                    result: geometry(
                        "AQMAACDmEAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAABAAAAAAAAA8D8AAAAAAAAAQA==",
                    ),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
        Case::Samples {
            expression: "st_intersection_agg(x0)",
            arguments: vec!["Nullable(Geometry)"],
            result: "Nullable(Geometry)",
            state: "Tuple(Binary, Boolean)",
            samples: vec![
                Sample {
                    label: "geometry/true/empty",
                    inputs: vec![GeometryType::from_opt_data(Vec::<Option<Vec<u8>>>::new())],
                    state: tuple(vec![binary(""), Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "geometry/true/nulls",
                    inputs: vec![GeometryType::from_opt_data(vec![None::<Vec<u8>>; 2])],
                    state: tuple(vec![binary(""), Scalar::Boolean(true)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "geometry/true/mixed",
                    inputs: vec![GeometryType::from_opt_data(vec![
                        Some(bytes(
                            "AQMAACDmEAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                        )),
                        None,
                        Some(bytes(
                            "AQMAACDmEAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAA==",
                        )),
                        Some(bytes(
                            "AQMAACDmEAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                        )),
                    ])],
                    state: tuple(vec![
                        binary(
                            "AQMAACDmEAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAABAAAAAAAAA8D8AAAAAAAAAQA==",
                        ),
                        Scalar::Boolean(true),
                    ]),
                    result: geometry(
                        "AQMAACDmEAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAABAAAAAAAAA8D8AAAAAAAAAQA==",
                    ),
                    merge_result: MergeResult::SameAsResult,
                },
            ],
        },
    ]);
}
