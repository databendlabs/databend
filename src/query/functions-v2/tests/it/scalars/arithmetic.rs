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
fn test_arithmetic() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("arithmetic.txt").unwrap();

    let columns = &[
        ("a", DataType::Int8, Column::from_data(vec![1i8, 2, 3])),
        (
            "a2",
            DataType::Nullable(Box::new(DataType::UInt8)),
            Column::from_data_with_validity(vec![1u8, 2, 3], vec![true, true, false]),
        ),
        ("b", DataType::Int16, Column::from_data(vec![2i16, 4, 6])),
        (
            "c",
            DataType::UInt32,
            Column::from_data(vec![10u32, 20, 30]),
        ),
        (
            "d",
            DataType::Float64,
            Column::from_data(vec![10f64, -20f64, 30f64]),
        ),
    ];
    test_add(file, columns);
    test_minus(file, columns);
    test_mul(file, columns);
    test_div(file, columns);
    test_intdiv(file, columns);
    test_modulo(file, columns);
}

fn test_add(file: &mut impl Write, columns: &[(&str, DataType, Column)]) {
    run_ast(file, "a + b", columns);
    run_ast(file, "a2 + 10", columns);
    run_ast(file, "a2 + c", columns);
    run_ast(file, "c + b", columns);
    run_ast(file, "c + d", columns);
    run_ast(file, "+d", columns);
}

fn test_minus(file: &mut impl Write, columns: &[(&str, DataType, Column)]) {
    run_ast(file, "a - b", columns);
    run_ast(file, "a2 - 10", columns);
    run_ast(file, "a2 - c", columns);
    run_ast(file, "c - b", columns);
    run_ast(file, "c - d", columns);
    run_ast(file, "-c", columns);
}

fn test_mul(file: &mut impl Write, columns: &[(&str, DataType, Column)]) {
    run_ast(file, "a  * b", columns);
    run_ast(file, "a2 * 10", columns);
    run_ast(file, "a2 * c", columns);
    run_ast(file, "c * b", columns);
    run_ast(file, "c * d", columns);
}

fn test_div(file: &mut impl Write, columns: &[(&str, DataType, Column)]) {
    run_ast(file, "a / b", columns);
    run_ast(file, "a2 / 10", columns);
    run_ast(file, "a2 / c", columns);
    run_ast(file, "divide(c, b)", columns);
    run_ast(file, "c / d", columns);
}

fn test_intdiv(file: &mut impl Write, columns: &[(&str, DataType, Column)]) {
    run_ast(file, "a  div b", columns);
    run_ast(file, "a2 div 10", columns);
    run_ast(file, "a2 div c", columns);
    run_ast(file, "c div b", columns);
    run_ast(file, "c div d", columns);
}

fn test_modulo(file: &mut impl Write, columns: &[(&str, DataType, Column)]) {
    run_ast(file, "(a + 3)  % b", columns);
    run_ast(file, "a2 % 4", columns);
    run_ast(file, "(a2 + 4) % c", columns);
    run_ast(file, "c % (b + 3)", columns);
    run_ast(file, "c % (d - 3)", columns);
}
