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

use databend_common_expression::types::BinaryType;
use databend_common_expression::types::StringType;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_binary() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("binary.txt").unwrap();

    test_length(file);
    test_to_base64(file);
    test_to_hex(file);

    for is_try in [false, true] {
        test_from_base64(file, is_try);
        test_from_unhex(file, is_try);
    }
}

fn test_length(file: &mut impl Write) {
    run_ast(file, "length(to_binary('latin'))", &[]);
    run_ast(file, "length(to_binary(NULL))", &[]);
    run_ast(file, "length(a)", &[(
        "a",
        BinaryType::from_data(vec![b"latin", "кириллица".as_bytes(), &[
            0xDE, 0xAD, 0xBE, 0xEF,
        ]]),
    )]);
}

fn test_to_hex(file: &mut impl Write) {
    run_ast(file, "to_hex('abc')", &[]);
    run_ast(file, "to_hex(a)", &[(
        "a",
        StringType::from_data(vec!["abc", "def", "databend"]),
    )]);
}

fn test_from_unhex(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(
        file,
        format!("{prefix}from_hex('6461746162656e64')::String"),
        &[],
    );
    run_ast(file, format!("{prefix}from_hex('6461746162656e6')"), &[]);
    run_ast(file, format!("{prefix}from_hex(s)::String"), &[(
        "s",
        StringType::from_data(vec!["616263", "646566", "6461746162656e64"]),
    )]);
}

fn test_to_base64(file: &mut impl Write) {
    run_ast(file, "to_base64('Abc')", &[]);
    run_ast(file, "to_base64('123')", &[]);
    run_ast(file, "to_base64(Null)", &[]);
    run_ast(file, "to_base64(a)", &[(
        "a",
        StringType::from_data(vec!["Abc", "123"]),
    )]);
}

fn test_from_base64(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}from_base64('QWJj')::String"), &[]);
    run_ast(file, format!("{prefix}from_base64('MTIz')::String"), &[]);
    run_ast(file, format!("{prefix}from_base64(a)::String"), &[(
        "a",
        StringType::from_data(vec!["QWJj", "MTIz"]),
    )]);
    run_ast(file, format!("{prefix}from_base64('!@#')"), &[]);
}
