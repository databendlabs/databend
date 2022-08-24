// Copyright 2021 Datafuse Labs.
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

use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ColumnFrom;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_math() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("math.txt").unwrap();

    test_abs(file);
    test_sign(file);
    test_trigonometric(file);
}

fn test_abs(file: &mut impl Write) {
    run_ast(file, "abs(1)", &[]);
    run_ast(file, "abs(-1)", &[]);
    run_ast(file, "abs(null)", &[]);
    run_ast(file, "abs(a)", &[(
        "a",
        DataType::Int64,
        Column::from_data(vec![1i64, -30, 1024]),
    )]);
}

fn test_sign(file: &mut impl Write) {
    run_ast(file, "sign(1)", &[]);
    run_ast(file, "sign(-1)", &[]);
    run_ast(file, "sign(null)", &[]);
    run_ast(file, "sign(a)", &[(
        "a",
        DataType::Int64,
        Column::from_data(vec![1i64, -30, 1024]),
    )]);
}

fn test_trigonometric(file: &mut impl Write) {
    run_ast(file, "sin(1)", &[]);
    run_ast(file, "cos(1)", &[]);
    run_ast(file, "tan(1)", &[]);
    run_ast(file, "atan(0.5)", &[]);
    run_ast(file, "cot(-1.0)", &[]);
    run_ast(file, "asin(1)", &[]);
    run_ast(file, "acos(0)", &[]);
    run_ast(file, "atan(null)", &[]);
    run_ast(file, "atan2(a, 4)", &[(
        "a",
        DataType::Int64,
        Column::from_data(vec![1i64, -1, 1024]),
    )]);
}
