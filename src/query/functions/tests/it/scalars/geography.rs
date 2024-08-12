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

use crate::scalars::run_ast;

#[test]
fn test_geography() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("geography.txt").unwrap();
    test_st_makepoint(file);
    test_st_geographyfromwkt(file);
}

fn test_st_makepoint(file: &mut impl Write) {
    run_ast(file, "st_makepoint(40.7127, -74.0059)", &[]);

    let columns = [
        ("lon", Float64Type::from_data(vec![12.57, 78.74, -48.5])),
        ("lat", Float64Type::from_data(vec![0.0, 90.0, -45.0])),
    ];
    run_ast(file, "st_makepoint(lon, lat)", &columns);
}

fn test_st_geographyfromwkt(file: &mut impl Write) {
    run_ast(
        file,
        "st_geographyfromwkt('SRID=4326;POINT(-71.0325022849392 42.3793285830812)')",
        &[],
    );
    run_ast(
        file,
        "st_geographyfromwkt('SRID=4326;POLYGON((-71.0325022849392 42.3793285830812,-71.0325745928559 42.3793012556699,-71.0326708728343 42.3794450989722,-71.0326045866257 42.3794706688942,-71.0325022849392 42.3793285830812))')",
        &[],
    );
}
