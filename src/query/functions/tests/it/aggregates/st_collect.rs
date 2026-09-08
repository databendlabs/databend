use std::io::Write;

use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::geometry_columns;
use super::support::write_aggregate_expr_case;

fn run_st_collect_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry('point(10 20)'))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry('srid=4326;linestring(10 20, 40 50)'))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(NULL)",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );

    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(point))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(point_null))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(point_all_null))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(line_string))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(line_string_null))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(polygon))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(mixed_geom))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(mixed_geom_null))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(point_4326))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(line_string_4326))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(polygon_4326))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(mixed_3857))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(mixed_srid))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "st_collect(to_geometry(mixed_srid_null))",
        geometry_columns().as_slice(),
        simulator,
        vec![],
    );
}

#[test]
fn test_st_collect() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("st_collect.txt").unwrap();
    run_st_collect_cases(file, eval_aggregate);
}

#[test]
fn test_st_collect_group_by_golden_preserves_single_group_order() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("st_collect_group_by.txt").unwrap();
    run_st_collect_cases(file, eval_aggregate);
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
    use super::support::bytes;
    use super::support::geometry;
    use super::support::tuple;

    super::support::check_state_baselines(vec![
        Case::Samples {
            expression: "st_collect(x0)",
            arguments: vec!["Geometry"],
            result: "Nullable(Geometry)",
            state: "Tuple(Array(Geometry), Boolean)",
            samples: vec![
                Sample {
                    label: "geometry/false/empty",
                    inputs: vec![GeometryType::from_data(Vec::<Vec<u8>>::new())],
                    state: tuple(vec![
                        Scalar::Array(GeometryType::from_data(Vec::<Vec<u8>>::new())),
                        Scalar::Boolean(false),
                    ]),
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
                        Scalar::Array(GeometryType::from_data(vec![
                            bytes(
                                "AQMAACDmEAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                            ),
                            bytes(
                                "AQMAACDmEAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAA==",
                            ),
                            bytes(
                                "AQMAACDmEAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                            ),
                        ])),
                        Scalar::Boolean(true),
                    ]),
                    result: geometry(
                        "AQYAACDmEAAAAwAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                    ),
                    merge_result: MergeResult::Value(geometry(
                        "AQYAACDmEAAABgAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                    )),
                },
            ],
        },
        Case::Samples {
            expression: "st_collect(x0)",
            arguments: vec!["Nullable(Geometry)"],
            result: "Nullable(Geometry)",
            state: "Tuple(Array(Geometry), Boolean)",
            samples: vec![
                Sample {
                    label: "geometry/true/empty",
                    inputs: vec![GeometryType::from_opt_data(Vec::<Option<Vec<u8>>>::new())],
                    state: tuple(vec![
                        Scalar::Array(GeometryType::from_data(Vec::<Vec<u8>>::new())),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "geometry/true/nulls",
                    inputs: vec![GeometryType::from_opt_data(vec![None::<Vec<u8>>; 2])],
                    state: tuple(vec![
                        Scalar::Array(GeometryType::from_data(Vec::<Vec<u8>>::new())),
                        Scalar::Boolean(true),
                    ]),
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
                        Scalar::Array(GeometryType::from_data(vec![
                            bytes(
                                "AQMAACDmEAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                            ),
                            bytes(
                                "AQMAACDmEAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAA==",
                            ),
                            bytes(
                                "AQMAACDmEAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                            ),
                        ])),
                        Scalar::Boolean(true),
                    ]),
                    result: geometry(
                        "AQYAACDmEAAAAwAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                    ),
                    merge_result: MergeResult::Value(geometry(
                        "AQYAACDmEAAABgAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAADwPwAAAAAAAAAAAAAAAAAACEAAAAAAAAAAAAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPwAAAAAAAABAAAAAAAAA8D8AAAAAAAAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAA==",
                    )),
                },
            ],
        },
    ]);
}
