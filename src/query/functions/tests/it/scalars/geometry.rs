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

use databend_common_expression::types::Float64Type;
use databend_common_expression::FromData;
use goldenfile::Mint;

use crate::scalars::run_ast;

#[test]
fn test_geometry() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("geometry.txt").unwrap();

    test_st_makepoint(file);
    test_to_string(file);
}

fn test_st_makepoint(file: &mut impl Write) {
    run_ast(file, "st_makepoint(7.0, 8.0)", &[]);
    run_ast(file, "st_makepoint(7.0, -8.0)", &[]);
    run_ast(file, "st_makepoint(a, b)", &[
        ("a", Float64Type::from_data(vec![1.0, 2.0, 3.0])),
        ("b", Float64Type::from_data(vec![1.0, 2.0, 3.0])),
    ]);
}

fn test_to_string(file: &mut impl Write) {
    run_ast(file, "to_string(st_makepoint(7.0, -8.0))", &[]);
    run_ast(file, "to_string(st_makepoint(a, b))", &[
        ("a", Float64Type::from_data(vec![1.0, 2.0, 3.0])),
        ("b", Float64Type::from_data(vec![1.0, 2.0, 3.0])),
    ]);
}
