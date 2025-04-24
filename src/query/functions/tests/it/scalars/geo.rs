// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;

use databend_common_expression::types::*;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_geo() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("geo.txt").unwrap();

    test_geo_to_h3(file);
    test_great_circle_distance(file);
    test_geo_distance(file);
    test_great_circle_angle(file);
    test_point_in_ellipses(file);
    test_point_in_polygon(file);
    test_geohash_encode(file);
    test_geohash_decode(file);
}

fn test_geo_to_h3(file: &mut impl Write) {
    run_ast(file, "geo_to_h3(37.79506683, 55.71290588, 16)", &[]);
    run_ast(file, "geo_to_h3(37.79506683, 55.71290588, 15)", &[]);
    run_ast(file, "geo_to_h3(lon, lat, 15)", &[
        (
            "lon",
            Float64Type::from_data(vec![37.63098076, 37.66018300, 37.59813500]),
        ),
        (
            "lat",
            Float64Type::from_data(vec![55.77922738, 55.76324100, 55.72076200]),
        ),
    ]);
}

fn test_great_circle_distance(file: &mut impl Write) {
    run_ast(
        file,
        "great_circle_distance(55.755831, 37.617673, -55.755831, -37.617673)",
        &[],
    );
    let table = [
        (
            "lon1",
            Float64Type::from_data(vec![55.755831, 56.755831, 57.755831]),
        ),
        (
            "lat1",
            Float64Type::from_data(vec![37.617673, 38.617673, 39.617673]),
        ),
        (
            "lon2",
            Float64Type::from_data(vec![-55.755831, -56.755831, -57.755831]),
        ),
        (
            "lat2",
            Float64Type::from_data(vec![-37.617673, -38.617673, -39.617673]),
        ),
    ];
    run_ast(
        file,
        "great_circle_distance(lon1, lat1, lon2, lat2)",
        &table,
    );
}

fn test_geo_distance(file: &mut impl Write) {
    run_ast(
        file,
        "geo_distance(55.755831, 37.617673, -55.755831, -37.617673)",
        &[],
    );
    let table = [
        (
            "lon1",
            Float64Type::from_data(vec![55.755831, 56.755831, 57.755831]),
        ),
        (
            "lat1",
            Float64Type::from_data(vec![37.617673, 38.617673, 39.617673]),
        ),
        (
            "lon2",
            Float64Type::from_data(vec![-55.755831, -56.755831, -57.755831]),
        ),
        (
            "lat2",
            Float64Type::from_data(vec![-37.617673, -38.617673, -39.617673]),
        ),
    ];
    run_ast(file, "geo_distance(lon1, lat1, lon2, lat2)", &table);
}

fn test_great_circle_angle(file: &mut impl Write) {
    run_ast(file, "great_circle_angle(0, 0, 45, 0)", &[]);
    run_ast(file, "great_circle_angle(0, 0, a, 0)", &[(
        "a",
        Float64Type::from_data(vec![45.0, 46.0, 47.0]),
    )]);
}

fn test_point_in_ellipses(file: &mut impl Write) {
    run_ast(
        file,
        "point_in_ellipses(10., 10., 10., 9.1, 1., 0.9999)",
        &[],
    );
    run_ast(file, "point_in_ellipses(10., 10., 10., 9.1, a, b)", &[
        ("a", Float64Type::from_data(vec![1.0, 1.1, 1.2])),
        ("b", Float64Type::from_data(vec![0.9999, 0.9998, 0.9997])),
    ]);
}

fn test_point_in_polygon(file: &mut impl Write) {
    // form 1: ((x, y), [(x1, y1), (x2, y2), ...])
    run_ast(
        file,
        "point_in_polygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)])",
        &[],
    );
    run_ast(
        file,
        "point_in_polygon((a, b), [(6, 0), (8, 4), (5, 8), (0, 2)])",
        &[
            ("a", Float64Type::from_data(vec![3.0, 3.1, 3.2])),
            ("b", Float64Type::from_data(vec![3.0, 3.1, 3.2])),
        ],
    );

    // form 2: ((x, y), [[(x1, y1), (x2, y2), ...], [(x21, y21), (x22, y22), ...], ...])
    run_ast(
        file,
        "point_in_polygon((1., 1.), [[(4., 0.), (8., 4.), (4., 8.), (0., 4.)], [(3., 3.), (3., 5.), (5., 5.), (5., 3.)]])",
        &[],
    );

    run_ast(
        file,
        "point_in_polygon((2.5, 2.5), [[(4., 0.), (8., 4.), (4., 8.), (0., 4.)], [(3., 3.), (3., 5.), (5., 5.), (5., 3.)]])",
        &[],
    );

    run_ast(
        file,
        "point_in_polygon((2.5, 2.5), [[(4., 0.), (8., 4.), (4., 8.), (0., 4.)], [(3., 3.), (a, b), (5., 5.), (5., 3.)]])",
        &[
            ("a", Float64Type::from_data(vec![3.0, 3.1])),
            ("b", Float64Type::from_data(vec![5.0, 5.0])),
        ],
    );

    // form 3: ((x, y), [(x1, y1), (x2, y2), ...], [(x21, y21), (x22, y22), ...], ...)
    run_ast(
        file,
        "point_in_polygon((2.5, 2.5), [(4., 0.), (8., 4.), (4., 8.), (0., 4.)], [(3., 3.), (3., 5.), (5., 5.), (5., 3.)])",
        &[],
    );

    run_ast(
        file,
        "point_in_polygon((2.5, 2.5), [(4., 0.), (8., 4.), (4., 8.), (0., 4.)], [(3., 3.), (a, b), (5., 5.), (5., 3.)])",
        &[
            ("a", Float64Type::from_data(vec![3.0, 3.0])),
            ("b", Float64Type::from_data(vec![5.0, 5.0])),
        ],
    );
}

fn test_geohash_encode(file: &mut impl Write) {
    run_ast(file, "geohash_encode(-5.60302734375, 42.593994140625)", &[]);
    run_ast(
        file,
        "geohash_encode(-5.60302734375, 42.593994140625, 11)",
        &[],
    );
    run_ast(
        file,
        "geohash_encode(-5.60302734375, 42.593994140625, 0)",
        &[],
    );
}

fn test_geohash_decode(file: &mut impl Write) {
    run_ast(file, "geohash_decode('ezs42')", &[]);
}
