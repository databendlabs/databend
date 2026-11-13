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
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::StringType;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_binary() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("binary.txt").unwrap();

    test_length(file);
    test_to_base64(file);
    test_to_hex(file);
    test_to_jsonb_binary(file);

    for is_try in [false, true] {
        test_from_base64(file, is_try);
        test_from_hex(file, is_try);
        test_to_binary(file, is_try);
        test_charset(file, is_try);
    }
}

fn test_charset(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    // GBK bytes for "你好" and "中文".
    let gbk_col = BinaryType::from_data(vec![
        vec![0xC4, 0xE3, 0xBA, 0xC3],
        vec![0xD6, 0xD0, 0xCE, 0xC4],
        b"ascii".to_vec(),
    ]);

    // GB18030 four-byte encoding (unlike GBK, it can represent emoji).
    run_ast(
        file,
        format!("{prefix}to_string(unhex('9439FC36'), 'gb18030')"),
        &[],
    );
    run_ast(
        file,
        format!("to_hex({prefix}to_binary('😀', 'gb18030'))"),
        &[],
    );

    // decode
    run_ast(
        file,
        format!("{prefix}to_string(unhex('C4E3BAC3'), 'gbk')"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}to_string(unhex('C4E3BAC3'), 'GB2312')"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}to_string(unhex('C4E3BAC3'), 'gb18030')"),
        &[],
    );
    run_ast(file, format!("{prefix}to_string(a, 'gbk')"), &[(
        "a",
        gbk_col.clone(),
    )]);
    run_ast(file, format!("{prefix}convert_from(a, 'gbk')"), &[(
        "a",
        gbk_col.clone(),
    )]);
    run_ast(
        file,
        format!("{prefix}to_string(unhex('A4A4'), 'big5')"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}to_string(unhex('E4BDA0'), 'utf-8')"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}to_string(unhex('604F7D59'), 'utf-16le')"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}to_string(unhex('616263'), 'hex')"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}to_string(unhex('616263'), 'base64')"),
        &[],
    );
    // invalid GBK sequence (truncated lead byte)
    run_ast(file, format!("{prefix}to_string(unhex('C4'), 'gbk')"), &[]);
    run_ast(file, format!("{prefix}to_string(a, 'gbk')"), &[(
        "a",
        BinaryType::from_data(vec![vec![0xC4, 0xE3], vec![0xC4], b"ok".to_vec()]),
    )]);
    run_ast(
        file,
        format!("{prefix}to_string(unhex('C4E3'), 'not-a-charset')"),
        &[],
    );
    run_ast(file, format!("{prefix}to_string(unhex('C4E3'), a)"), &[(
        "a",
        StringType::from_data(vec!["gbk"]),
    )]);

    // encode
    run_ast(
        file,
        format!("to_hex({prefix}to_binary('你好', 'gbk'))"),
        &[],
    );
    run_ast(
        file,
        format!("to_hex({prefix}convert_to('你好', 'gbk'))"),
        &[],
    );
    run_ast(
        file,
        format!("to_hex({prefix}to_binary('你好', 'utf-8'))"),
        &[],
    );
    run_ast(file, format!("to_hex({prefix}to_binary(a, 'gbk'))"), &[(
        "a",
        StringType::from_data(vec!["你好", "中文", "ascii"]),
    )]);
    // round trip
    run_ast(
        file,
        format!("{prefix}to_string({prefix}to_binary(a, 'gbk'), 'gbk')"),
        &[("a", StringType::from_data(vec!["你好", "中文", "ascii"]))],
    );
    // unmappable character (emoji is not representable in GBK)
    run_ast(file, format!("to_hex({prefix}to_binary('😀', 'gbk'))"), &[
    ]);
    run_ast(file, format!("to_hex({prefix}to_binary(a, 'gbk'))"), &[(
        "a",
        StringType::from_data(vec!["你好", "😀", "ok"]),
    )]);
    // Constant validation errors must not be hidden by a NULL first row.
    for format in ["not-a-charset", "utf-16le"] {
        for function in ["to_binary", "convert_to"] {
            run_ast(
                file,
                format!("to_hex({prefix}{function}(a, '{format}'))"),
                &[(
                    "a",
                    StringType::from_opt_data(vec![None, Some("abc"), None]),
                )],
            );
        }
    }
    for function in ["to_string", "convert_from"] {
        // A column is invalid even when every charset label in it is supported.
        run_ast(file, format!("{prefix}{function}(unhex('C4E3'), a)"), &[(
            "a",
            StringType::from_data(vec!["gbk", "gb18030"]),
        )]);
        run_ast(file, format!("{prefix}{function}(unhex('C4E3'), NULL)"), &[
        ]);
        run_ast(file, format!("{prefix}{function}(a, 'not-a-charset')"), &[
            (
                "a",
                BinaryType::from_opt_data(vec![None, Some(b"abc".as_slice()), None]),
            ),
        ]);
        run_ast(file, format!("{prefix}{function}(unhex('C4E3'), a)"), &[(
            "a",
            StringType::from_opt_data(vec![None, Some("gbk"), None]),
        )]);
    }
    for function in ["to_binary", "convert_to"] {
        run_ast(file, format!("{prefix}{function}('abc', a)"), &[(
            "a",
            StringType::from_data(vec!["gbk", "gb18030"]),
        )]);
        run_ast(file, format!("{prefix}{function}('abc', NULL)"), &[]);
        run_ast(file, format!("{prefix}{function}('abc', a)"), &[(
            "a",
            StringType::from_opt_data(vec![None, Some("gbk"), None]),
        )]);
    }

    // encoding_rs cannot encode to UTF-16
    run_ast(
        file,
        format!("to_hex({prefix}to_binary('a', 'utf-16le'))"),
        &[],
    );
    run_ast(
        file,
        format!("to_hex({prefix}to_binary('a', 'not-a-charset'))"),
        &[],
    );
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
    run_ast(file, "to_hex(to_binary('abc'))", &[]);
    run_ast(file, "hex(to_binary('abc'))", &[]);
    run_ast(file, "to_hex(to_binary(a))", &[(
        "a",
        StringType::from_data(vec!["abc", "def", "databend"]),
    )]);
}

fn test_from_hex(file: &mut impl Write, is_try: bool) {
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

fn test_to_jsonb_binary(file: &mut impl Write) {
    run_ast(
        file,
        format!("to_jsonb_binary(parse_json('{{\"k1\":\"val\",\"k2\":100}}'))"),
        &[],
    );
    run_ast(file, format!("to_jsonb_binary(parse_json('10'))"), &[]);
    run_ast(file, format!("to_jsonb_binary(parse_json('123456'))"), &[]);
    run_ast(file, format!("to_jsonb_binary(parse_json('\"abcd\"'))"), &[
    ]);
}

fn test_to_binary(file: &mut impl Write, is_try: bool) {
    let prefix = if is_try { "TRY_" } else { "" };

    run_ast(file, format!("{prefix}to_binary(to_bitmap('1,2,3'))"), &[]);
    run_ast(
        file,
        format!("{prefix}to_binary(to_bitmap('100,25,50,700'))"),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}to_binary(st_geometryfromwkt('SRID=4326;POINT(1.0 2.0)'))"),
        &[],
    );
    run_ast(
        file,
        format!(
            "{prefix}to_binary(st_geometryfromwkb(unhex('0101000020797f000066666666a9cb17411f85ebc19e325641')))"
        ),
        &[],
    );
    run_ast(
        file,
        format!("{prefix}to_binary(st_geographyfromewkt('SRID=4326;POINT(-122.35 37.55)'))"),
        &[],
    );
}
