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

use databend_common_expression::types::decimal::DecimalColumn;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::number::*;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use ethnum::i256;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_arithmetic() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("arithmetic.txt").unwrap();

    let columns = &[
        ("a", Int8Type::from_data(vec![1i8, 2, 3])),
        (
            "a2",
            UInt8Type::from_data_with_validity(vec![1u8, 2, 3], vec![true, true, false]),
        ),
        ("b", Int16Type::from_data(vec![2i16, 4, 6])),
        ("c", UInt32Type::from_data(vec![10u32, 20, 30])),
        ("d", Float64Type::from_data(vec![10f64, -20f64, 30f64])),
        (
            "d2",
            UInt8Type::from_data_with_validity(vec![1u8, 0, 3], vec![true, false, true]),
        ),
        (
            "e",
            Column::Decimal(DecimalColumn::Decimal128(
                vec![31, 335, 1888].into(),
                DecimalSize {
                    precision: 10,
                    scale: 1,
                },
            )),
        ),
        (
            "f",
            Column::Decimal(DecimalColumn::Decimal256(
                vec![i256::from(50), i256::from(92), i256::from(1234)].into(),
                DecimalSize {
                    precision: 76,
                    scale: 2,
                },
            )),
        ),
        ("g", Int64Type::from_data(vec![i64::MAX, i64::MIN, 0])),
    ];
    test_add(file, columns);
    test_minus(file, columns);
    test_unary_minus(file, columns);
    test_mul(file, columns);
    test_div(file, columns);
    test_intdiv(file, columns);
    test_modulo(file, columns);
    test_to_string(file, columns);
    test_carte(file, columns);
    test_square(file, columns);
    test_cube(file, columns);
    test_abs(file, columns);
    test_factorial(file, columns);
    test_bitwise_xor(file, columns);
    test_bitwise_and(file, columns);
    test_bitwise_or(file, columns);
    test_bitwise_not(file, columns);
    test_bitwise_shift_left(file, columns);
    test_bitwise_shift_right(file, columns);
}

fn test_add(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a + b", columns);
    run_ast(file, "a2 + 10", columns);
    run_ast(file, "a2 + c", columns);
    run_ast(file, "c + 0.5", columns);
    run_ast(file, "c + b", columns);
    run_ast(file, "c + d", columns);
    run_ast(file, "c + e", columns);
    run_ast(file, "d + e", columns);
    run_ast(file, "d2 + e", columns);
    run_ast(file, "d2 + f", columns);
    run_ast(file, "e + f", columns);
}

fn test_minus(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a - b", columns);
    run_ast(file, "a2 - 10", columns);
    run_ast(file, "a2 - c", columns);
    run_ast(file, "c - 0.5", columns);
    run_ast(file, "c - b", columns);
    run_ast(file, "c - d", columns);
    run_ast(file, "c - e", columns);
    run_ast(file, "d - e", columns);
    run_ast(file, "d2 - e", columns);
    run_ast(file, "d2 - f", columns);
    run_ast(file, "e - f", columns);
}

fn test_unary_minus(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "-a", columns);
    run_ast(file, "-a2", columns);
    run_ast(file, "-b", columns);
    run_ast(file, "-c", columns);
    run_ast(file, "-d", columns);
    run_ast(file, "-d2", columns);
    run_ast(file, "-e", columns);
    run_ast(file, "-f", columns);
    run_ast(file, "-g", columns);
}

fn test_mul(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a  * b", columns);
    run_ast(file, "a2 * 10", columns);
    run_ast(file, "a2 * c", columns);
    run_ast(file, "c * 0.5", columns);
    run_ast(file, "c * b", columns);
    run_ast(file, "c * d", columns);
    run_ast(file, "c * e", columns);
    run_ast(file, "d * e", columns);
    run_ast(file, "d2 * e", columns);
    run_ast(file, "d2 * f", columns);
    run_ast(file, "e * f", columns);
    run_ast(file, "e * e", columns);
    run_ast(file, "f * f", columns);
    run_ast(file, "e * 0.5", columns);
}

fn test_div(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a / b", columns);
    run_ast(file, "a2 / 10", columns);
    run_ast(file, "a2 / c", columns);
    run_ast(file, "c / 0.5", columns);
    run_ast(file, "divide(c, b)", columns);
    run_ast(file, "c / d", columns);
    run_ast(file, "b / d2", columns);
    run_ast(file, "2.0 / 0", columns);
    run_ast(file, "c / e", columns);
    run_ast(file, "d / e", columns);
    run_ast(file, "d2 / e", columns);
    run_ast(file, "d2 / f", columns);
    run_ast(file, "e / f", columns);
}

fn test_intdiv(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a  div b", columns);
    run_ast(file, "a2 div 10", columns);
    run_ast(file, "a2 div c", columns);
    run_ast(file, "c div 0.5", columns);
    run_ast(file, "c div b", columns);
    run_ast(file, "c div d", columns);
    run_ast(file, "c div d2", columns);
    run_ast(file, "c div 0", columns);
}

fn test_modulo(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "(a + 3)  % b", columns);
    run_ast(file, "a2 % 4", columns);
    run_ast(file, "(a2 + 4) % c", columns);
    run_ast(file, "c % (b + 3)", columns);
    run_ast(file, "c % (d - 3)", columns);
    run_ast(file, "c % 0", columns);
    run_ast(file, "c % d2", columns);
}

fn test_to_string(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "to_string(a)", columns);
    run_ast(file, "to_string(a2)", columns);
    run_ast(file, "to_string(b)", columns);
    run_ast(file, "to_string(c)", columns);
    run_ast(file, "to_string(d)", columns);
    run_ast(file, "to_string(d2)", columns);
    run_ast(file, "to_string(e)", columns);
    run_ast(file, "to_string(f)", columns);
}

fn test_carte(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a ^ 2", columns);
    run_ast(file, "a ^ a", columns);
    run_ast(file, "a ^ a2", columns);
    run_ast(file, "c ^ 0", columns);
    run_ast(file, "c ^ d", columns);
}

fn test_square(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "|/a", columns);
    run_ast(file, "|/a2", columns);
    run_ast(file, "|/b", columns);
    run_ast(file, "|/c", columns);
    run_ast(file, "|/d", columns);
    run_ast(file, "|/d2", columns);
}

fn test_cube(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "||/a", columns);
    run_ast(file, "||/a2", columns);
    run_ast(file, "||/b", columns);
    run_ast(file, "||/c", columns);
    run_ast(file, "||/d", columns);
    run_ast(file, "||/d2", columns);
}

fn test_factorial(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a!", columns);
    run_ast(file, "b!", columns);
    run_ast(file, "12!", columns);
    run_ast(file, "30!", columns);
}

fn test_abs(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "@ a", columns);
    run_ast(file, "@ a2", columns);
    run_ast(file, "@ b", columns);
    run_ast(file, "@ c", columns);
    run_ast(file, "@ d", columns);
    run_ast(file, "@ d2", columns);
}

fn test_bitwise_and(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a & b", columns);
    run_ast(file, "a2 & 10", columns);
    run_ast(file, "a2 & c", columns);
    run_ast(file, "c & b", columns);
}

fn test_bitwise_or(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a | b", columns);
    run_ast(file, "a2 | 10", columns);
    run_ast(file, "a2 | c", columns);
    run_ast(file, "c | b", columns);
}

fn test_bitwise_xor(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a # b", columns);
    run_ast(file, "a2 # 10", columns);
    run_ast(file, "a2 # c", columns);
    run_ast(file, "c # b", columns);
}

fn test_bitwise_not(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "~a", columns);
    run_ast(file, "~a2", columns);
    run_ast(file, "~b", columns);
    run_ast(file, "~c", columns);
}

fn test_bitwise_shift_left(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a << 4", columns);
    run_ast(file, "a2 << 2", columns);
    run_ast(file, "a2 << 4", columns);
    run_ast(file, "c << 2", columns);
}

fn test_bitwise_shift_right(file: &mut impl Write, columns: &[(&str, Column)]) {
    run_ast(file, "a >> 1", columns);
    run_ast(file, "a2 >> 1", columns);
    run_ast(file, "a2 >> 2", columns);
    run_ast(file, "c >> 2", columns);
}
