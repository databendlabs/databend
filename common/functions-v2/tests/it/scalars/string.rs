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
use common_expression::ColumnBuilder;
use common_expression::ScalarRef;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_string() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("string.txt").unwrap();

    test_upper(file);
    test_bit_length(file);
    test_octet_length(file);
    test_char_length(file);
    test_to_base64(file);
    test_from_base64(file);
}

fn test_upper(file: &mut impl Write) {
    run_ast(file, "upper('Abc')", &[]);
    run_ast(file, "upper('Dobrý den')", &[]);
    run_ast(file, "upper('ß😀山')", &[]);
    run_ast(file, "upper(NULL)", &[]);
    run_ast(file, "ucase(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["Abc", "Dobrý den", "ß😀山"]),
    )]);
}

fn test_bit_length(file: &mut impl Write) {
    run_ast(file, "bit_length('latin')", &[]);
    run_ast(file, "bit_length(NULL)", &[]);
    run_ast(file, "bit_length(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["latin", "кириллица", "кириллица and latin"]),
    )]);
}

fn test_octet_length(file: &mut impl Write) {
    run_ast(file, "octet_length('latin')", &[]);
    run_ast(file, "octet_length(NULL)", &[]);
    run_ast(file, "length(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["latin", "кириллица", "кириллица and latin"]),
    )]);
}

fn test_char_length(file: &mut impl Write) {
    run_ast(file, "char_length('latin')", &[]);
    run_ast(file, "char_length(NULL)", &[]);
    run_ast(file, "character_length(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["latin", "кириллица", "кириллица and latin"]),
    )]);
}

fn test_to_base64(file: &mut impl Write) {
    run_ast(file, "to_base64('Abc')", &[]);
    run_ast(file, "to_base64('123')", &[]);
    run_ast(file, "to_base64(Null)", &[]);
    run_ast(file, "to_base64(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["Abc", "123"]),
    )]);
}

fn test_from_base64(file: &mut impl Write) {
    run_ast(file, "from_base64('QWJj')", &[]);
    run_ast(file, "from_base64('MTIz')", &[]);
    run_ast(file, "to_base64(Null)", &[]);
    run_ast(file, "to_base64(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["QWJj", "MTIz"]),
    )])
}

fn build_string_column(strings: &[&str]) -> Column {
    let mut builder = ColumnBuilder::with_capacity(&DataType::String, strings.len());
    for s in strings {
        builder.push(ScalarRef::String(s.as_bytes()));
    }
    builder.build()
}
