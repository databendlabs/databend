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

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use crate::scalars::run_ast;

#[test]
fn test_geography() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("geography.txt").unwrap();
    test_st_makepoint(file);
    test_st_geographyfromewkt(file);
    test_st_asgeojson(file);
    test_st_asewkb(file);
    test_st_aswkb(file);
    test_st_asewkt(file);
    test_st_aswkt(file);
    test_st_distance(file);
    test_st_length(file);
    test_st_area(file);
    test_st_endpoint(file);
    test_st_startpoint(file);
    test_st_pointn(file);
    test_st_dimension(file);
    test_st_centroid(file);
    test_st_union(file);
    test_st_intersection(file);
    test_st_difference(file);
    test_st_symdifference(file);
    test_st_contains(file);
    test_st_disjoint(file);
    test_st_intersects(file);
    test_st_within(file);
    test_st_geogfromgeohash(file);
    test_st_geogpointfromgeohash(file);
    test_st_makepolygon(file);
    test_st_makeline(file);
    test_st_geohash(file);
    test_st_geographyfromwkb(file);
    test_st_geographyfromwkt(file);
    test_st_x(file);
    test_st_y(file);
    test_st_srid(file);
    test_st_xmax(file);
    test_st_xmin(file);
    test_st_ymax(file);
    test_st_ymin(file);
    test_st_npoints(file);
    test_to_string(file);
    test_to_geography(file);
    test_try_to_geography(file);
    test_st_hilbert(file);
}

fn test_st_makepoint(file: &mut impl Write) {
    run_ast(file, "st_makepoint(40.7127, -74.0059)", &[]);

    let columns = [
        ("lon", Float64Type::from_data(vec![12.57, 78.74, -48.5])),
        ("lat", Float64Type::from_data(vec![0.0, 90.0, -45.0])),
    ];
    run_ast(file, "st_makepoint(lon, lat)", &columns);
}

fn test_st_geographyfromewkt(file: &mut impl Write) {
    run_ast(file, "st_geographyfromewkt('POINT EMPTY')", &[]);
    run_ast(file, "st_geographyfromewkt('POINT(1 2)')", &[]);
    run_ast(
        file,
        "st_geographyfromewkt('SRID=4326;POINT(-122.35 37.55)')",
        &[],
    );
    run_ast(
        file,
        "st_geographyfromewkt('LINESTRING(-124.2 42,-120.01 41.99)')",
        &[],
    );
}

fn test_st_asgeojson(file: &mut impl Write) {
    run_ast(
        file,
        "st_asgeojson(st_geographyfromewkt('POINT(-122.35 37.55)'))",
        &[],
    );
    run_ast(
        file,
        "st_asgeojson(st_geographyfromewkt('LINESTRING(-124.2 42,-120.01 41.99)'))",
        &[],
    );
}

fn test_st_asewkb(file: &mut impl Write) {
    run_ast(
        file,
        "st_asewkb(st_geographyfromewkt('SRID=4326;POINT(-122.35 37.55)'))",
        &[],
    );
}

fn test_st_aswkb(file: &mut impl Write) {
    run_ast(
        file,
        "st_aswkb(st_geographyfromewkt('POINT(-122.35 37.55)'))",
        &[],
    );
}

fn test_st_asewkt(file: &mut impl Write) {
    run_ast(
        file,
        "st_asewkt(st_geographyfromewkt('SRID=4326;POINT(-122.35 37.55)'))",
        &[],
    );
}

fn test_st_aswkt(file: &mut impl Write) {
    run_ast(
        file,
        "st_aswkt(st_geographyfromewkt('POINT(-122.35 37.55)'))",
        &[],
    );
}

fn test_st_distance(file: &mut impl Write) {
    run_ast(
        file,
        "st_distance(st_geographyfromewkt('POINT(-122.35 37.55)'), st_geographyfromewkt('POINT(51.30 -0.07)'))",
        &[],
    );
    run_ast(
        file,
        "st_distance(st_geographyfromewkt('POINT(40.42 -74.0)'), st_geographyfromewkt('POINT(51.30 -0.07)'))",
        &[],
    );
}

fn test_st_length(file: &mut impl Write) {
    run_ast(
        file,
        "st_length(st_geographyfromewkt('LINESTRING(0 0,0 1)'))",
        &[],
    );
}

fn test_st_area(file: &mut impl Write) {
    run_ast(
        file,
        "st_area(st_geographyfromewkt('POLYGON((0 0,0 1,1 1,1 0,0 0))'))",
        &[],
    );
}

fn test_st_endpoint(file: &mut impl Write) {
    run_ast(
        file,
        "st_endpoint(st_geographyfromewkt('LINESTRING(0 0,1 1,2 2)'))",
        &[],
    );
}

fn test_st_startpoint(file: &mut impl Write) {
    run_ast(
        file,
        "st_startpoint(st_geographyfromewkt('LINESTRING(0 0,1 1,2 2)'))",
        &[],
    );
}

fn test_st_pointn(file: &mut impl Write) {
    run_ast(
        file,
        "st_pointn(st_geographyfromewkt('LINESTRING(0 0,1 1,2 2)'), 2)",
        &[],
    );
}

fn test_st_dimension(file: &mut impl Write) {
    run_ast(
        file,
        "st_dimension(st_geographyfromewkt('POINT(-122.35 37.55)'))",
        &[],
    );
    run_ast(
        file,
        "st_dimension(st_geographyfromewkt('LINESTRING(0 0,1 1,2 2)'))",
        &[],
    );
    run_ast(
        file,
        "st_dimension(st_geographyfromewkt('POLYGON((0 0,0 1,1 1,1 0,0 0))'))",
        &[],
    );
}

fn test_st_centroid(file: &mut impl Write) {
    run_ast(
        file,
        "st_centroid(st_geographyfromewkt('POINT(-0.1278 51.5074)'))",
        &[],
    );
    run_ast(
        file,
        "st_centroid(st_geographyfromewkt('LINESTRING(-74.0060 40.7128, -73.9851 40.7580)'))",
        &[],
    );
    run_ast(
        file,
        "st_centroid(st_geographyfromewkt('POLYGON((2.33 48.85, 2.33 48.87, 2.36 48.87, 2.36 48.85, 2.33 48.85))'))",
        &[],
    );
    run_ast(
        file,
        "st_centroid(st_geographyfromewkt('MULTIPOINT((-74.0060 40.7128), (-73.9851 40.7580), (-73.9680 40.7851))'))",
        &[],
    );
}

fn test_st_union(file: &mut impl Write) {
    run_ast(
        file,
        "st_union(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POLYGON((-74.00 40.72, -74.00 40.76, -73.96 40.76, -73.96 40.72, -74.00 40.72))'))",
        &[],
    );
    run_ast(
        file,
        "st_union(st_geographyfromewkt('POLYGON((-0.15 51.50, -0.15 51.52, -0.10 51.52, -0.10 51.50, -0.15 51.50))'), st_geographyfromewkt('POLYGON((2.33 48.85, 2.33 48.87, 2.36 48.87, 2.36 48.85, 2.33 48.85))'))",
        &[],
    );
    run_ast(
        file,
        "st_union(st_geographyfromewkt('POINT(-0.1278 51.5074)'), st_geographyfromewkt('POINT(2.3522 48.8566)'))",
        &[],
    );
}

fn test_st_intersection(file: &mut impl Write) {
    run_ast(
        file,
        "st_intersection(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POLYGON((-74.00 40.72, -74.00 40.76, -73.96 40.76, -73.96 40.72, -74.00 40.72))'))",
        &[],
    );
    run_ast(
        file,
        "st_intersection(st_geographyfromewkt('POLYGON((-0.15 51.50, -0.15 51.52, -0.10 51.52, -0.10 51.50, -0.15 51.50))'), st_geographyfromewkt('POLYGON((139.68 35.68, 139.68 35.70, 139.70 35.70, 139.70 35.68, 139.68 35.68))'))",
        &[],
    );
    run_ast(
        file,
        "st_intersection(st_geographyfromewkt('POLYGON((-0.20 51.48, -0.20 51.54, -0.05 51.54, -0.05 51.48, -0.20 51.48))'), st_geographyfromewkt('POINT(-0.1278 51.5074)'))",
        &[],
    );
}

fn test_st_difference(file: &mut impl Write) {
    run_ast(
        file,
        "st_difference(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POLYGON((-74.00 40.72, -74.00 40.76, -73.96 40.76, -73.96 40.72, -74.00 40.72))'))",
        &[],
    );
    run_ast(
        file,
        "st_difference(st_geographyfromewkt('POLYGON((-0.15 51.50, -0.15 51.52, -0.10 51.52, -0.10 51.50, -0.15 51.50))'), st_geographyfromewkt('POLYGON((139.68 35.68, 139.68 35.70, 139.70 35.70, 139.70 35.68, 139.68 35.68))'))",
        &[],
    );
    run_ast(
        file,
        "st_difference(st_geographyfromewkt('POLYGON((-0.20 51.48, -0.20 51.54, -0.05 51.54, -0.05 51.48, -0.20 51.48))'), st_geographyfromewkt('POLYGON((-0.20 51.48, -0.20 51.54, -0.05 51.54, -0.05 51.48, -0.20 51.48))'))",
        &[],
    );
}

fn test_st_symdifference(file: &mut impl Write) {
    run_ast(
        file,
        "st_symdifference(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POLYGON((-74.00 40.72, -74.00 40.76, -73.96 40.76, -73.96 40.72, -74.00 40.72))'))",
        &[],
    );
    run_ast(
        file,
        "st_symdifference(st_geographyfromewkt('POLYGON((2.33 48.85, 2.33 48.87, 2.36 48.87, 2.36 48.85, 2.33 48.85))'), st_geographyfromewkt('POLYGON((2.33 48.85, 2.33 48.87, 2.36 48.87, 2.36 48.85, 2.33 48.85))'))",
        &[],
    );
    run_ast(
        file,
        "st_symdifference(st_geographyfromewkt('POINT(139.6917 35.6895)'), st_geographyfromewkt('POINT(-0.1278 51.5074)'))",
        &[],
    );
}

fn test_st_contains(file: &mut impl Write) {
    run_ast(
        file,
        "st_contains(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POINT(-74.0060 40.7128)'))",
        &[],
    );
    run_ast(
        file,
        "st_contains(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POINT(139.6917 35.6895)'))",
        &[],
    );
    run_ast(
        file,
        "st_contains(st_geographyfromewkt('POLYGON((-0.20 51.48, -0.20 51.54, -0.05 51.54, -0.05 51.48, -0.20 51.48))'), st_geographyfromewkt('POLYGON((-0.15 51.50, -0.15 51.52, -0.10 51.52, -0.10 51.50, -0.15 51.50))'))",
        &[],
    );
    run_ast(
        file,
        "st_contains(st_geographyfromewkt('POLYGON((-0.20 51.48, -0.20 51.54, -0.05 51.54, -0.05 51.48, -0.20 51.48))'), st_geographyfromewkt('POINT(-0.1278 51.5074)'))",
        &[],
    );
    run_ast(
        file,
        "st_contains(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POINT(-74.0199 40.7001)'))",
        &[],
    );
    run_ast(
        file,
        "st_contains(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POINT(-74.0201 40.6999)'))",
        &[],
    );
    run_ast(
        file,
        "st_contains(st_geographyfromewkt('POLYGON((2.33 48.85, 2.33 48.87, 2.36 48.87, 2.36 48.85, 2.33 48.85))'), st_geographyfromewkt('POINT(2.345 48.860)'))",
        &[],
    );
    run_ast(
        file,
        "st_contains(st_geographyfromewkt('POLYGON((2.33 48.85, 2.33 48.87, 2.36 48.87, 2.36 48.85, 2.33 48.85))'), st_geographyfromewkt('POINT(2.3299 48.8499)'))",
        &[],
    );
}

fn test_st_disjoint(file: &mut impl Write) {
    run_ast(
        file,
        "st_disjoint(st_geographyfromewkt('POLYGON((-0.15 51.50, -0.15 51.52, -0.10 51.52, -0.10 51.50, -0.15 51.50))'), st_geographyfromewkt('POLYGON((139.68 35.68, 139.68 35.70, 139.70 35.70, 139.70 35.68, 139.68 35.68))'))",
        &[],
    );
    run_ast(
        file,
        "st_disjoint(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POLYGON((-74.00 40.72, -74.00 40.76, -73.96 40.76, -73.96 40.72, -74.00 40.72))'))",
        &[],
    );
    run_ast(
        file,
        "st_disjoint(st_geographyfromewkt('POINT(139.6917 35.6895)'), st_geographyfromewkt('POLYGON((-0.20 51.48, -0.20 51.54, -0.05 51.54, -0.05 51.48, -0.20 51.48))'))",
        &[],
    );
    run_ast(
        file,
        "st_disjoint(st_geographyfromewkt('POINT(-74.0201 40.6999)'), st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'))",
        &[],
    );
    run_ast(
        file,
        "st_disjoint(st_geographyfromewkt('POINT(-0.2002 51.4798)'), st_geographyfromewkt('POLYGON((-0.20 51.48, -0.20 51.54, -0.05 51.54, -0.05 51.48, -0.20 51.48))'))",
        &[],
    );
    run_ast(
        file,
        "st_disjoint(st_geographyfromewkt('POLYGON((-74.0206 40.7396, -74.0206 40.7398, -74.0204 40.7398, -74.0204 40.7396, -74.0206 40.7396))'), st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'))",
        &[],
    );
}

fn test_st_intersects(file: &mut impl Write) {
    run_ast(
        file,
        "st_intersects(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POLYGON((-74.00 40.72, -74.00 40.76, -73.96 40.76, -73.96 40.72, -74.00 40.72))'))",
        &[],
    );
    run_ast(
        file,
        "st_intersects(st_geographyfromewkt('POLYGON((-0.15 51.50, -0.15 51.52, -0.10 51.52, -0.10 51.50, -0.15 51.50))'), st_geographyfromewkt('POLYGON((139.68 35.68, 139.68 35.70, 139.70 35.70, 139.70 35.68, 139.68 35.68))'))",
        &[],
    );
    run_ast(
        file,
        "st_intersects(st_geographyfromewkt('POLYGON((-0.15 51.50, -0.15 51.52, -0.10 51.52, -0.10 51.50, -0.15 51.50))'), st_geographyfromewkt('POINT(-0.1278 51.5074)'))",
        &[],
    );
    run_ast(
        file,
        "st_intersects(st_geographyfromewkt('POINT(-74.0060 40.7128)'), st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'))",
        &[],
    );
    run_ast(
        file,
        "st_intersects(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POLYGON((-74.0205 40.6995, -74.0205 40.7005, -74.0195 40.7005, -74.0195 40.6995, -74.0205 40.6995))'))",
        &[],
    );
    run_ast(
        file,
        "st_intersects(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POLYGON((-74.0206 40.6994, -74.0206 40.6996, -74.0204 40.6996, -74.0204 40.6994, -74.0206 40.6994))'))",
        &[],
    );
    run_ast(
        file,
        "st_intersects(st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'), st_geographyfromewkt('POLYGON((-74.0201 40.7399, -74.0201 40.7402, -74.0198 40.7402, -74.0198 40.7399, -74.0201 40.7399))'))",
        &[],
    );
    run_ast(
        file,
        "st_intersects(st_geographyfromewkt('POINT(2.3301 48.8501)'), st_geographyfromewkt('POLYGON((2.33 48.85, 2.33 48.87, 2.36 48.87, 2.36 48.85, 2.33 48.85))'))",
        &[],
    );
}

fn test_st_within(file: &mut impl Write) {
    run_ast(
        file,
        "st_within(st_geographyfromewkt('POINT(-74.0060 40.7128)'), st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'))",
        &[],
    );
    run_ast(
        file,
        "st_within(st_geographyfromewkt('POLYGON((-0.15 51.50, -0.15 51.52, -0.10 51.52, -0.10 51.50, -0.15 51.50))'), st_geographyfromewkt('POLYGON((-0.20 51.48, -0.20 51.54, -0.05 51.54, -0.05 51.48, -0.20 51.48))'))",
        &[],
    );
    run_ast(
        file,
        "st_within(st_geographyfromewkt('POINT(139.6917 35.6895)'), st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'))",
        &[],
    );
    run_ast(
        file,
        "st_within(st_geographyfromewkt('POINT(-0.1278 51.5074)'), st_geographyfromewkt('POLYGON((-0.20 51.48, -0.20 51.54, -0.05 51.54, -0.05 51.48, -0.20 51.48))'))",
        &[],
    );
    run_ast(
        file,
        "st_within(st_geographyfromewkt('POINT(-74.0199 40.7001)'), st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'))",
        &[],
    );
    run_ast(
        file,
        "st_within(st_geographyfromewkt('POLYGON((-74.0198 40.7002, -74.0198 40.7008, -74.0192 40.7008, -74.0192 40.7002, -74.0198 40.7002))'), st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'))",
        &[],
    );
    run_ast(
        file,
        "st_within(st_geographyfromewkt('POINT(-74.0201 40.6999)'), st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'))",
        &[],
    );
    run_ast(
        file,
        "st_within(st_geographyfromewkt('POLYGON((-74.0198 40.7398, -74.0198 40.7406, -74.0192 40.7406, -74.0192 40.7398, -74.0198 40.7398))'), st_geographyfromewkt('POLYGON((-74.02 40.70, -74.02 40.74, -73.98 40.74, -73.98 40.70, -74.02 40.70))'))",
        &[],
    );
}

fn test_st_geogfromgeohash(file: &mut impl Write) {
    run_ast(
        file,
        "st_geogfromgeohash(st_geohash(st_makepoint(-122.3061, 37.554162)))",
        &[],
    );
}

fn test_st_geogpointfromgeohash(file: &mut impl Write) {
    run_ast(
        file,
        "st_geogpointfromgeohash(st_geohash(st_makepoint(-122.3061, 37.554162)))",
        &[],
    );
}

fn test_st_makepolygon(file: &mut impl Write) {
    run_ast(
        file,
        "st_makepolygon(st_geographyfromewkt('LINESTRING(0 0,0 1,1 1,1 0,0 0)'))",
        &[],
    );
}

fn test_st_makeline(file: &mut impl Write) {
    run_ast(
        file,
        "st_makeline(st_makepoint(0, 0), st_makepoint(1, 1))",
        &[],
    );
}

fn test_st_geohash(file: &mut impl Write) {
    run_ast(file, "st_geohash(st_makepoint(-122.3061, 37.554162))", &[]);
    run_ast(
        file,
        "st_geohash(st_makepoint(-122.3061, 37.554162), 8)",
        &[],
    );
}

fn test_st_geographyfromwkb(file: &mut impl Write) {
    run_ast(
        file,
        "st_geographyfromwkb(to_hex(st_aswkb(st_geographyfromewkt('POINT(-122.35 37.55)'))))",
        &[],
    );
    run_ast(
        file,
        "st_geographyfromwkb(st_aswkb(st_geographyfromewkt('POINT(-122.35 37.55)')))",
        &[],
    );
    run_ast(
        file,
        "st_geographyfromwkb(to_hex(st_aswkb(st_geographyfromewkt('POINT(-122.35 37.55)'))))",
        &[],
    );
    run_ast(
        file,
        "st_geographyfromwkb(st_aswkb(st_geographyfromewkt('POINT(-122.35 37.55)')))",
        &[],
    );
}

fn test_st_geographyfromwkt(file: &mut impl Write) {
    run_ast(file, "st_geographyfromwkt('POINT(-122.35 37.55)')", &[]);
    run_ast(file, "st_geographyfromwkt('POINT(-122.35 37.55)')", &[]);
}

fn test_st_x(file: &mut impl Write) {
    run_ast(
        file,
        "st_x(st_geographyfromewkt('POINT(-122.35 37.55)'))",
        &[],
    );
}

fn test_st_y(file: &mut impl Write) {
    run_ast(
        file,
        "st_y(st_geographyfromewkt('POINT(-122.35 37.55)'))",
        &[],
    );
}

fn test_st_srid(file: &mut impl Write) {
    run_ast(
        file,
        "st_srid(st_geographyfromewkt('POINT(-122.35 37.55)'))",
        &[],
    );
    run_ast(
        file,
        "st_srid(st_geographyfromewkt('SRID=4326;POINT(-122.35 37.55)'))",
        &[],
    );
}

fn test_st_xmax(file: &mut impl Write) {
    run_ast(
        file,
        "st_xmax(st_geographyfromewkt('POLYGON((0 0,0 1,1 1,1 0,0 0))'))",
        &[],
    );
}

fn test_st_xmin(file: &mut impl Write) {
    run_ast(
        file,
        "st_xmin(st_geographyfromewkt('POLYGON((0 0,0 1,1 1,1 0,0 0))'))",
        &[],
    );
}

fn test_st_ymax(file: &mut impl Write) {
    run_ast(
        file,
        "st_ymax(st_geographyfromewkt('POLYGON((0 0,0 1,1 1,1 0,0 0))'))",
        &[],
    );
}

fn test_st_ymin(file: &mut impl Write) {
    run_ast(
        file,
        "st_ymin(st_geographyfromewkt('POLYGON((0 0,0 1,1 1,1 0,0 0))'))",
        &[],
    );
}

fn test_st_npoints(file: &mut impl Write) {
    run_ast(
        file,
        "st_npoints(st_geographyfromewkt('LINESTRING(0 0,1 1,2 2)'))",
        &[],
    );
    run_ast(
        file,
        "st_npoints(st_geographyfromewkt('POLYGON((0 0,0 1,1 1,1 0,0 0))'))",
        &[],
    );
}

fn test_to_string(file: &mut impl Write) {
    run_ast(
        file,
        "to_string(st_geographyfromewkt('POINT(-122.35 37.55)'))",
        &[],
    );
}

fn test_to_geography(file: &mut impl Write) {
    run_ast(file, "to_geography('POINT(-122.35 37.55)')", &[]);
    run_ast(
        file,
        "to_geography(st_aswkb(st_geographyfromewkt('POINT(-122.35 37.55)')))",
        &[],
    );
    run_ast(
        file,
        r#"to_geography(parse_json('{"type":"Point","coordinates":[1,2]}'))"#,
        &[],
    );
}

fn test_try_to_geography(file: &mut impl Write) {
    run_ast(file, "try_to_geography('POINT(-122.35 37.55)')", &[]);
    run_ast(file, "try_to_geography('INVALID')", &[]);
    run_ast(
        file,
        r#"try_to_geography(parse_json('{"type":"Point","coordinates":[1,2]}'))"#,
        &[],
    );
    run_ast(
        file,
        "try_to_geography(st_aswkb(st_geographyfromewkt('POINT(-122.35 37.55)')))",
        &[],
    );
}

fn test_st_hilbert(file: &mut impl Write) {
    run_ast(file, "ST_HILBERT(TO_GEOGRAPHY('POINT(113.15 23.06)'))", &[]);
    run_ast(file, "ST_HILBERT(TO_GEOGRAPHY('POINT(116.25 39.54)'))", &[]);
    run_ast(file, "ST_HILBERT(TO_GEOGRAPHY('POINT(107.40 33.42)'))", &[]);
    run_ast(
        file,
        "ST_HILBERT(TO_GEOGRAPHY('POINT(113.15 23.06)'), [73, 4, 135, 53])",
        &[],
    );
    run_ast(
        file,
        "ST_HILBERT(TO_GEOGRAPHY('POINT(116.25 39.54)'), [73, 4, 135, 53])",
        &[],
    );
    run_ast(
        file,
        "ST_HILBERT(TO_GEOGRAPHY('POINT(107.40 33.42)'), [73, 4, 135, 53])",
        &[],
    );
}
