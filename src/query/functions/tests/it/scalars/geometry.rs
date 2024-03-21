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

use goldenfile::Mint;

use databend_common_expression::FromData;
use databend_common_expression::types::{BinaryType, Float64Type};
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;

use crate::scalars::run_ast;

#[test]
fn test_geometry() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("geometry.txt").unwrap();
    test_st_makeline(file);
    test_st_makepoint(file);
    test_to_string(file);
    test_st_geometryfromwkt(file);
    // test_st_transform(file);
}

fn test_st_makeline(file: &mut impl Write) {
    run_ast(file, "st_makeline(
                            to_geometry('POINT(1.0 2.0)'),
                            to_geometry('POINT(3.5 4.5)'))", &[]);
    run_ast(file, "st_makeline(
                            to_geometry('POINT(1.0 2.0)'),
                            to_geometry('LINESTRING(1.0 2.0, 10.1 5.5)'))", &[]);
    run_ast(file, "st_makeline(
                            to_geometry('LINESTRING(1.0 2.0, 10.1 5.5)'),
                            to_geometry('MULTIPOINT(3.5 4.5, 6.1 7.9)'))", &[]);
    run_ast(file, "st_makeline(a, b)", &[
        ("a", BinaryType::from_data(vec!["LINESTRING(1.0 2.0, 10.1 5.5)".as_bytes()])),
        ("b", BinaryType::from_data(vec!["MULTIPOINT(3.5 4.5, 6.1 7.9)".as_bytes()])),
    ]);
}

fn test_st_makepoint(file: &mut impl Write) {
    run_ast(file, "st_makegeompoint(7.0, 8.0)", &[]);
    run_ast(file, "st_makegeompoint(7.0, -8.0)", &[]);
    run_ast(file, "st_makegeompoint(a, b)", &[
        ("a", Float64Type::from_data(vec![1.0, 2.0, 3.0])),
        ("b", Float64Type::from_data(vec![1.0, 2.0, 3.0])),
    ]);
}

fn test_to_string(file: &mut impl Write) {
    run_ast(file, "to_string(st_makegeompoint(7.0, -8.0))", &[]);
    run_ast(file, "to_string(st_makegeompoint(a, b))", &[
        ("a", Float64Type::from_data(vec![1.0, 2.0, 3.0])),
        ("b", Float64Type::from_data(vec![1.0, 2.0, 3.0])),
    ]);
}

fn test_st_geometryfromwkt(file: &mut impl Write) {
    // without srid
    run_ast(
        file,
        "st_geometryfromwkt('POINT(389866.35 5819003.03)')",
        &[],
    );

    run_ast(file, "st_geometryfromwkt(a)", &[(
        "a",
        StringType::from_data(vec![
            "POINT(389866.35 5819003.03)",
            "POINT(389866.35 5819003.03)",
            "POINT(389866.35 5819003.03)",
        ]),
    )]);

    // with srid
    run_ast(
        file,
        "st_geometryfromwkt('POINT(389866.35 5819003.03)', 32633)",
        &[],
    );

    run_ast(file, "st_geometryfromwkt(a, b)", &[
        (
            "a",
            StringType::from_data(vec![
                "POINT(389866.35 5819003.03)",
                "POINT(389866.35 5819003.03)",
                "POINT(389866.35 5819003.03)",
            ]),
        ),
        ("b", Int32Type::from_data(vec![32633, 4326, 3857])),
    ]);
}

// fn test_st_transform(file: &mut impl Write) {
//     // just to_srid
//     run_ast(
//         file,
//         "st_transform(st_geomfromwkt('POINT(389866.35 5819003.03)', 32633), 3857)",
//         &[],
//     );
//
//     run_ast(file, "st_transform(st_geomfromwkt(a, b), c)", &[
//         (
//             "a",
//             StringType::from_data(vec!["POINT(389866.35 5819003.03)"]),
//         ),
//         ("b", Int32Type::from_data(vec![32633])),
//         ("c", Int32Type::from_data(vec![3857])),
//     ]);
//
//     // from_srid and to_srid
//     run_ast(
//         file,
//         "st_transform(st_geomfromwkt('POINT(4.500212 52.161170)'), 4326, 28992)",
//         &[],
//     );
//
//     run_ast(file, "st_transform(st_geomfromwkt(a), b, c)", &[
//         (
//             "a",
//             StringType::from_data(vec!["POINT(4.500212 52.161170)"]),
//         ),
//         ("b", Int32Type::from_data(vec![4326])),
//         ("c", Int32Type::from_data(vec![28992])),
//     ]);
// }
