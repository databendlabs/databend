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
    test_lower(file);
    test_bit_length(file);
    test_octet_length(file);
    test_char_length(file);
    test_to_base64(file);
    test_from_base64(file);
    test_quote(file);
    test_reverse(file);
    test_ascii(file);
    test_ltrim(file);
    test_rtrim(file);
    test_trim_leading(file);
    test_trim_trailing(file);
    test_trim_both(file);
    test_trim(file);
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

fn test_lower(file: &mut impl Write) {
    run_ast(file, "lower('Abc')", &[]);
    run_ast(file, "lower('DOBRÝ DEN')", &[]);
    run_ast(file, "lower('İ😀山')", &[]);
    run_ast(file, "lower(NULL)", &[]);
    run_ast(file, "lcase(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["Abc", "DOBRÝ DEN", "İ😀山"]),
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
    run_ast(file, "from_base64(Null)", &[]);
    run_ast(file, "from_base64(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["QWJj", "MTIz"]),
    )])
}

fn test_quote(file: &mut impl Write) {
    run_ast(file, r#"quote('a\0b')"#, &[]);
    run_ast(file, r#"quote('a\'b')"#, &[]);
    run_ast(file, r#"quote('a\"b')"#, &[]);
    run_ast(file, r#"quote('a\bb')"#, &[]);
    run_ast(file, r#"quote('a\nb')"#, &[]);
    run_ast(file, r#"quote('a\rb')"#, &[]);
    run_ast(file, r#"quote('a\tb')"#, &[]);
    run_ast(file, r#"quote('a\\b')"#, &[]);
    run_ast(file, "quote('你好')", &[]);
    run_ast(file, "quote('ß😀山')", &[]);
    run_ast(file, "quote('Dobrý den')", &[]);
    run_ast(file, "quote(Null)", &[]);
    run_ast(file, "quote(a)", &[(
        "a",
        DataType::String,
        build_string_column(&[r#"a\0b"#, r#"a\'b"#, r#"a\nb"#]),
    )])
}

fn test_reverse(file: &mut impl Write) {
    run_ast(file, "reverse('abc')", &[]);
    run_ast(file, "reverse('a')", &[]);
    run_ast(file, "reverse('')", &[]);
    run_ast(file, "reverse('你好')", &[]);
    run_ast(file, "reverse('ß😀山')", &[]);
    run_ast(file, "reverse('Dobrý den')", &[]);
    run_ast(file, "reverse(Null)", &[]);
    run_ast(file, "reverse(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["abc", "a", ""]),
    )])
}

fn test_ascii(file: &mut impl Write) {
    run_ast(file, "ascii('1')", &[]);
    run_ast(file, "ascii('123')", &[]);
    run_ast(file, "ascii('-1')", &[]);
    run_ast(file, "ascii('')", &[]);
    run_ast(file, "ascii('你好')", &[]);
    run_ast(file, "ascii('😀123')", &[]);
    run_ast(file, "ascii(Null)", &[]);
    run_ast(file, "ascii(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["1", "123", "-1", "你好"]),
    )]);
    run_ast(file, "ascii(b)", &[(
        "b",
        DataType::String,
        build_string_column(&[""]),
    )]);
}

fn test_ltrim(file: &mut impl Write) {
    run_ast(file, "ltrim('   abc   ')", &[]);
    run_ast(file, "ltrim('  ')", &[]);
    run_ast(file, "ltrim(NULL)", &[]);
    run_ast(file, "ltrim(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["abc", "   abc", "   abc   ", "abc   "]),
    )]);
}

fn test_rtrim(file: &mut impl Write) {
    run_ast(file, "rtrim('   abc   ')", &[]);
    run_ast(file, "rtrim('  ')", &[]);
    run_ast(file, "rtrim(NULL)", &[]);
    run_ast(file, "rtrim(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["abc", "   abc", "   abc   ", "abc   "]),
    )]);
}

fn test_trim_leading(file: &mut impl Write) {
    run_ast(file, "trim_leading('aaaaabbbbb', 'a')", &[]);
    run_ast(file, "trim_leading('aaaaabbbbb', 'aa')", &[]);
    run_ast(file, "trim_leading('aaaaaaaaaa', 'a')", &[]);
    run_ast(file, "trim_leading('aaaaabbbbb', 'b')", &[]);
    run_ast(file, "trim_leading(NULL, 'a')", &[]);
    run_ast(file, "trim_leading('aaaaabbbbb', NULL)", &[]);

    let table = [
        (
            "a",
            DataType::String,
            build_string_column(&["aaaabb", "bbbbcc", "ccccdd"]),
        ),
        ("b", DataType::String, build_string_column(&["a", "b", "c"])),
    ];

    run_ast(file, "trim_leading(a, 'a')", &table);
    run_ast(file, "trim_leading(a, b)", &table);
    run_ast(file, "trim_leading('aaaaabbbbb', b)", &table);
}

fn test_trim_trailing(file: &mut impl Write) {
    run_ast(file, "trim_trailing('aaaaabbbbb', 'b')", &[]);
    run_ast(file, "trim_trailing('aaaaabbbbb', 'bb')", &[]);
    run_ast(file, "trim_trailing('aaaaaaaaaa', 'a')", &[]);
    run_ast(file, "trim_trailing('aaaaabbbbb', 'a')", &[]);
    run_ast(file, "trim_trailing(NULL, 'a')", &[]);
    run_ast(file, "trim_trailing('aaaaabbbbb', NULL)", &[]);

    let table = [
        (
            "a",
            DataType::String,
            build_string_column(&["aaaabb", "bbbbcc", "ccccdd"]),
        ),
        ("b", DataType::String, build_string_column(&["b", "c", "d"])),
    ];

    run_ast(file, "trim_trailing(a, 'b')", &table);
    run_ast(file, "trim_trailing(a, b)", &table);
    run_ast(file, "trim_trailing('aaaaabbbbb', b)", &table);
}

fn test_trim_both(file: &mut impl Write) {
    run_ast(file, "trim_both('aaabbaaa', 'a')", &[]);
    run_ast(file, "trim_both('aaabbaaa', 'aa')", &[]);
    run_ast(file, "trim_both('aaaaaaaa', 'a')", &[]);
    run_ast(file, "trim_both('aaabbaaa', 'b')", &[]);
    run_ast(file, "trim_both(NULL, 'a')", &[]);
    run_ast(file, "trim_both('aaaaaaaa', NULL)", &[]);

    let table = [
        (
            "a",
            DataType::String,
            build_string_column(&["aabbaa", "bbccbb", "ccddcc"]),
        ),
        ("b", DataType::String, build_string_column(&["a", "b", "c"])),
    ];

    run_ast(file, "trim_both(a, 'a')", &table);
    run_ast(file, "trim_both(a, b)", &table);
    run_ast(file, "trim_both('aaabbaaa', b)", &table);
}

fn test_trim_with_from(file: &mut impl Write, trim_where: &str) {
    assert!(matches!(trim_where, "both" | "leading" | "trailing"));

    run_ast(
        file,
        format!("trim({} 'a' from 'aaabbaaa')", trim_where).as_str(),
        &[],
    );
    run_ast(
        file,
        format!("trim({} 'aa' from 'aaabbaaa')", trim_where).as_str(),
        &[],
    );
    run_ast(
        file,
        format!("trim({} 'a' from 'aaaaaaaa')", trim_where).as_str(),
        &[],
    );
    run_ast(
        file,
        format!("trim({} 'b' from 'aaabbaaa')", trim_where).as_str(),
        &[],
    );
    run_ast(
        file,
        format!("trim({} 'a' from NULL)", trim_where).as_str(),
        &[],
    );
    run_ast(
        file,
        format!("trim({} NULL from 'aaaaaaaa')", trim_where).as_str(),
        &[],
    );

    let table = [
        (
            "a",
            DataType::String,
            build_string_column(&["aabbaa", "bbccbb", "ccddcc"]),
        ),
        ("b", DataType::String, build_string_column(&["a", "b", "c"])),
    ];

    run_ast(
        file,
        format!("trim({} 'a' from a)", trim_where).as_str(),
        &table,
    );
    run_ast(
        file,
        format!("trim({} b from a)", trim_where).as_str(),
        &table,
    );
    run_ast(
        file,
        format!("trim({} a from a)", trim_where).as_str(),
        &table,
    );
    run_ast(
        file,
        format!("trim({} a from 'a')", trim_where).as_str(),
        &table,
    );
}

fn test_trim(file: &mut impl Write) {
    // TRIM(<expr>)
    run_ast(file, "trim('   abc   ')", &[]);
    run_ast(file, "trim('  ')", &[]);
    run_ast(file, "trim(NULL)", &[]);
    run_ast(file, "trim(a)", &[(
        "a",
        DataType::String,
        build_string_column(&["abc", "   abc", "   abc   ", "abc   "]),
    )]);

    // TRIM([[BOTH | LEADING | TRAILING] <expr> FROM] <expr>)
    test_trim_with_from(file, "both");
    test_trim_with_from(file, "leading");
    test_trim_with_from(file, "trailing");
}

fn build_string_column(strings: &[&str]) -> Column {
    let mut builder = ColumnBuilder::with_capacity(&DataType::String, strings.len());
    for s in strings {
        builder.push(ScalarRef::String(s.as_bytes()));
    }
    builder.build()
}
