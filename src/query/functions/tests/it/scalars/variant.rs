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

use common_expression::types::*;
use common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_variant() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("variant.txt").unwrap();

    test_parse_json(file);
    test_try_parse_json(file);
    test_check_json(file);
    test_length(file);
    test_json_object_keys(file);
    test_get(file);
    test_get_ignore_case(file);
    test_get_path(file);
    test_json_extract_path_text(file);
    test_as_type(file);
    test_to_type(file);
    test_try_to_type(file);
    test_json_object(file);
    test_json_object_keep_null(file);
    test_json_path_query_array(file);
    test_json_path_query_first(file);
}

fn test_parse_json(file: &mut impl Write) {
    run_ast(file, "parse_json(NULL)", &[]);
    run_ast(file, "parse_json('nuLL')", &[]);
    run_ast(file, "parse_json('null')", &[]);
    run_ast(file, "parse_json(' \t')", &[]);
    run_ast(file, "parse_json('true')", &[]);
    run_ast(file, "parse_json('false')", &[]);
    run_ast(file, "parse_json('\"测试\"')", &[]);
    run_ast(file, "parse_json('1234')", &[]);
    run_ast(file, "parse_json('[1,2,3,4]')", &[]);
    run_ast(file, "parse_json('{\"a\":\"b\",\"c\":\"d\"}')", &[]);

    run_ast(file, "parse_json(s)", &[(
        "s",
        StringType::from_data(vec![
            r#"null"#,
            r#"true"#,
            r#"9223372036854775807"#,
            r#"-32768"#,
            r#"1234.5678"#,
            r#"1.912e2"#,
            r#""\\\"abc\\\"""#,
            r#""databend""#,
            r#"{"k":"v","a":"b"}"#,
            r#"[1,2,3,["a","b","c"]]"#,
        ]),
    )]);

    run_ast(file, "parse_json(s)", &[(
        "s",
        StringType::from_data_with_validity(&["true", "false", "", "1234"], vec![
            true, true, false, true,
        ]),
    )]);
}

fn test_try_parse_json(file: &mut impl Write) {
    run_ast(file, "try_parse_json(NULL)", &[]);
    run_ast(file, "try_parse_json('nuLL')", &[]);
    run_ast(file, "try_parse_json('null')", &[]);
    run_ast(file, "try_parse_json('true')", &[]);
    run_ast(file, "try_parse_json('false')", &[]);
    run_ast(file, "try_parse_json('\"测试\"')", &[]);
    run_ast(file, "try_parse_json('1234')", &[]);
    run_ast(file, "try_parse_json('[1,2,3,4]')", &[]);
    run_ast(file, "try_parse_json('{\"a\":\"b\",\"c\":\"d\"}')", &[]);

    run_ast(file, "try_parse_json(s)", &[(
        "s",
        StringType::from_data(vec![
            r#"null"#,
            r#"true"#,
            r#"9223372036854775807"#,
            r#"-32768"#,
            r#"1234.5678"#,
            r#"1.912e2"#,
            r#""\\\"abc\\\"""#,
            r#""databend""#,
            r#"{"k":"v","a":"b"}"#,
            r#"[1,2,3,["a","b","c"]]"#,
        ]),
    )]);

    run_ast(file, "try_parse_json(s)", &[(
        "s",
        StringType::from_data_with_validity(&["true", "ttt", "", "1234"], vec![
            true, true, false, true,
        ]),
    )]);
}

fn test_check_json(file: &mut impl Write) {
    run_ast(file, "check_json(NULL)", &[]);
    run_ast(file, "check_json('true')", &[]);
    run_ast(file, "check_json('nuLL')", &[]);

    run_ast(file, "check_json(s)", &[(
        "s",
        StringType::from_data(vec![r#"null"#, r#"abc"#, r#"true"#]),
    )]);

    run_ast(file, "check_json(s)", &[(
        "s",
        StringType::from_data_with_validity(&["true", "ttt", "", "1234"], vec![
            true, true, false, true,
        ]),
    )]);
}

fn test_length(file: &mut impl Write) {
    run_ast(file, "length(parse_json('1234'))", &[]);
    run_ast(file, "length(parse_json('[1,2,3,4]'))", &[]);
    run_ast(file, "length(parse_json('{\"k\":\"v\"}'))", &[]);

    run_ast(file, "length(parse_json(s))", &[(
        "s",
        StringType::from_data(vec!["true", "[1,2,3,4]", "[\"a\",\"b\",\"c\"]"]),
    )]);

    run_ast(file, "length(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(
            &["true", "[1,2,3,4]", "", "[\"a\",\"b\",\"c\"]"],
            vec![true, true, false, true],
        ),
    )]);
}

fn test_json_object_keys(file: &mut impl Write) {
    run_ast(file, "json_object_keys(parse_json('[1,2,3,4]'))", &[]);
    run_ast(
        file,
        "json_object_keys(parse_json('{\"k1\":\"v1\",\"k2\":\"v2\"}'))",
        &[],
    );

    run_ast(file, "json_object_keys(parse_json(s))", &[(
        "s",
        StringType::from_data(vec![
            "[1,2,3,4]",
            "{\"a\":\"b\",\"c\":\"d\"}",
            "{\"k1\":\"v1\",\"k2\":\"v2\"}",
        ]),
    )]);

    run_ast(file, "json_object_keys(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(
            &[
                "[1,2,3,4]",
                "{\"a\":\"b\",\"c\":\"d\"}",
                "",
                "{\"k1\":\"v1\",\"k2\":\"v2\"}",
            ],
            vec![true, true, false, true],
        ),
    )]);
}

fn test_get(file: &mut impl Write) {
    run_ast(file, "parse_json('null')[1]", &[]);
    run_ast(file, "parse_json('null')['k']", &[]);
    run_ast(file, "parse_json('[1,2,3,4]')[1]", &[]);
    run_ast(file, "parse_json('[1,2,3,4]')[2+3]", &[]);
    run_ast(file, "parse_json('{\"k\":\"v\"}')['k']", &[]);
    run_ast(file, "parse_json('{\"k\":\"v\"}')['x']", &[]);
    run_ast(file, "CAST(('a', 'b') AS VARIANT)['2']", &[]);

    run_ast(file, "parse_json(s)[i]", &[
        (
            "s",
            StringType::from_data(vec!["true", "[1,2,3,4]", "[\"a\",\"b\",\"c\"]"]),
        ),
        ("i", UInt64Type::from_data(vec![0u64, 0, 1])),
    ]);

    run_ast(file, "parse_json(s)[i]", &[
        (
            "s",
            StringType::from_data_with_validity(
                &["true", "[1,2,3,4]", "", "[\"a\",\"b\",\"c\"]"],
                vec![true, true, false, true],
            ),
        ),
        (
            "i",
            UInt64Type::from_data_with_validity(vec![0u64, 2, 0, 1], vec![
                false, true, false, true,
            ]),
        ),
    ]);

    run_ast(file, "parse_json(s)[k]", &[
        (
            "s",
            StringType::from_data(vec!["true", "{\"k\":1}", "{\"a\":\"b\"}"]),
        ),
        ("k", StringType::from_data(vec!["k", "k", "x"])),
    ]);

    run_ast(file, "parse_json(s)[k]", &[
        (
            "s",
            StringType::from_data_with_validity(&["true", "{\"k\":1}", "", "{\"a\":\"b\"}"], vec![
                true, true, false, true,
            ]),
        ),
        ("k", StringType::from_data(vec!["", "k", "", "a"])),
    ]);
}

fn test_get_ignore_case(file: &mut impl Write) {
    run_ast(
        file,
        "get_ignore_case(parse_json('{\"Aa\":1, \"aA\":2, \"aa\":3}'), 'AA')",
        &[],
    );
    run_ast(
        file,
        "get_ignore_case(parse_json('{\"Aa\":1, \"aA\":2, \"aa\":3}'), 'aa')",
        &[],
    );
    run_ast(
        file,
        "get_ignore_case(parse_json('{\"Aa\":1, \"aA\":2, \"aa\":3}'), 'bb')",
        &[],
    );

    run_ast(file, "get_ignore_case(parse_json(s), k)", &[
        (
            "s",
            StringType::from_data(vec!["true", "{\"k\":1}", "{\"a\":\"b\"}"]),
        ),
        ("k", StringType::from_data(vec!["k", "K", "A"])),
    ]);
    run_ast(file, "get_ignore_case(parse_json(s), k)", &[
        (
            "s",
            StringType::from_data_with_validity(&["true", "{\"k\":1}", "", "{\"a\":\"b\"}"], vec![
                true, true, false, true,
            ]),
        ),
        ("k", StringType::from_data(vec!["", "K", "", "A"])),
    ]);
}

fn test_get_path(file: &mut impl Write) {
    run_ast(file, "get_path(parse_json('[[1,2],3]'), '[0]')", &[]);
    run_ast(file, "get_path(parse_json('[[1,2],3]'), '[0][1]')", &[]);
    run_ast(file, "get_path(parse_json('[1,2,3]'), '[0]')", &[]);
    run_ast(file, "get_path(parse_json('[1,2,3]'), 'k2:k3')", &[]);
    run_ast(
        file,
        "get_path(parse_json('{\"a\":{\"b\":2}}'), '[\"a\"][\"b\"]')",
        &[],
    );
    run_ast(file, "get_path(parse_json('{\"a\":{\"b\":2}}'), 'a:b')", &[
    ]);
    run_ast(
        file,
        "get_path(parse_json('{\"a\":{\"b\":2}}'), '[\"a\"]')",
        &[],
    );
    run_ast(file, "get_path(parse_json('{\"a\":{\"b\":2}}'), 'a')", &[]);

    run_ast(file, "get_path(parse_json(s), k)", &[
        (
            "s",
            StringType::from_data(vec!["true", "{\"k\":1}", "[\"a\",\"b\"]"]),
        ),
        ("k", StringType::from_data(vec!["k", "[\"k\"]", "[\"a\"]"])),
    ]);
    run_ast(file, "get_path(parse_json(s), k)", &[
        (
            "s",
            StringType::from_data_with_validity(&["true", "{\"k\":1}", "", "[\"a\",\"b\"]"], vec![
                true, true, false, true,
            ]),
        ),
        (
            "k",
            StringType::from_data(vec!["[0]", "[\"k\"]", "", "[0]"]),
        ),
    ]);
}

fn test_json_extract_path_text(file: &mut impl Write) {
    run_ast(file, "json_extract_path_text('[[1,2],3]', '[0]')", &[]);
    run_ast(file, "json_extract_path_text('[[1,2],3]', '[0][1]')", &[]);
    run_ast(file, "json_extract_path_text('[1,2,3]', '[0]')", &[]);
    run_ast(file, "json_extract_path_text('[1,2,3]', 'k2:k3')", &[]);
    run_ast(
        file,
        "json_extract_path_text('{\"a\":{\"b\":2}}', '[\"a\"][\"b\"]')",
        &[],
    );
    run_ast(
        file,
        "json_extract_path_text('{\"a\":{\"b\":2}}', 'a:b')",
        &[],
    );
    run_ast(
        file,
        "json_extract_path_text('{\"a\":{\"b\":2}}', '[\"a\"]')",
        &[],
    );
    run_ast(file, "json_extract_path_text('{\"a\":{\"b\":2}}', 'a')", &[
    ]);

    run_ast(file, "json_extract_path_text(s, k)", &[
        (
            "s",
            StringType::from_data(vec!["true", "{\"k\":1}", "[\"a\",\"b\"]"]),
        ),
        ("k", StringType::from_data(vec!["k", "[\"k\"]", "[\"a\"]"])),
    ]);
    run_ast(file, "json_extract_path_text(s, k)", &[
        (
            "s",
            StringType::from_data_with_validity(&["true", "{\"k\":1}", "", "[\"a\",\"b\"]"], vec![
                true, true, false, true,
            ]),
        ),
        (
            "k",
            StringType::from_data(vec!["[0]", "[\"k\"]", "", "[0]"]),
        ),
    ]);
}

fn test_as_type(file: &mut impl Write) {
    run_ast(file, "as_boolean(parse_json('true'))", &[]);
    run_ast(file, "as_boolean(parse_json('123'))", &[]);
    run_ast(file, "as_integer(parse_json('true'))", &[]);
    run_ast(file, "as_integer(parse_json('123'))", &[]);
    run_ast(file, "as_float(parse_json('\"ab\"'))", &[]);
    run_ast(file, "as_float(parse_json('12.34'))", &[]);
    run_ast(file, "as_string(parse_json('\"ab\"'))", &[]);
    run_ast(file, "as_string(parse_json('12.34'))", &[]);
    run_ast(file, "as_array(parse_json('[1,2,3]'))", &[]);
    run_ast(file, "as_array(parse_json('{\"a\":\"b\"}'))", &[]);
    run_ast(file, "as_object(parse_json('[1,2,3]'))", &[]);
    run_ast(file, "as_object(parse_json('{\"a\":\"b\"}'))", &[]);

    let columns = &[(
        "s",
        StringType::from_data(vec![
            "true",
            "123",
            "12.34",
            "\"ab\"",
            "[1,2,3]",
            "{\"a\":\"b\"}",
        ]),
    )];
    run_ast(file, "as_boolean(parse_json(s))", columns);
    run_ast(file, "as_boolean(try_parse_json(s))", columns);
    run_ast(file, "as_integer(parse_json(s))", columns);
    run_ast(file, "as_integer(try_parse_json(s))", columns);
    run_ast(file, "as_float(parse_json(s))", columns);
    run_ast(file, "as_float(try_parse_json(s))", columns);
    run_ast(file, "as_string(parse_json(s))", columns);
    run_ast(file, "as_string(try_parse_json(s))", columns);
    run_ast(file, "as_array(parse_json(s))", columns);
    run_ast(file, "as_array(try_parse_json(s))", columns);
    run_ast(file, "as_object(parse_json(s))", columns);
    run_ast(file, "as_object(try_parse_json(s))", columns);
}

fn test_to_type(file: &mut impl Write) {
    run_ast(file, "to_boolean(parse_json('true'))", &[]);
    run_ast(file, "to_boolean(parse_json('123'))", &[]);
    run_ast(file, "to_boolean(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_uint64(parse_json('123'))", &[]);
    run_ast(file, "to_uint64(parse_json('-123'))", &[]);
    run_ast(file, "to_uint64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_int64(parse_json('123'))", &[]);
    run_ast(file, "to_int64(parse_json('-123'))", &[]);
    run_ast(file, "to_int64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_float64(parse_json('12.34'))", &[]);
    run_ast(file, "to_float64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_date(parse_json('\"2023-01-01\"'))", &[]);
    run_ast(file, "to_date(parse_json('\"abc\"'))", &[]);
    run_ast(
        file,
        "to_timestamp(parse_json('\"2023-01-01 00:00:00\"'))",
        &[],
    );
    run_ast(file, "to_timestamp(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_string(parse_json('12.34'))", &[]);
    run_ast(file, "to_string(parse_json('\"abc\"'))", &[]);

    run_ast(file, "to_boolean(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(&["true", "", "true"], vec![true, false, true]),
    )]);
    run_ast(file, "to_int64(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(&["1", "", "-10"], vec![true, false, true]),
    )]);
    run_ast(file, "to_uint64(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(&["1", "", "20"], vec![true, false, true]),
    )]);
    run_ast(file, "to_float64(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(&["1.2", "", "100.2"], vec![true, false, true]),
    )]);
    run_ast(file, "to_date(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(&["\"2020-01-01\"", "", "\"2023-10-01\""], vec![
            true, false, true,
        ]),
    )]);
    run_ast(file, "to_timestamp(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(
            &["\"2020-01-01 00:00:00\"", "", "\"2023-10-01 10:11:12\""],
            vec![true, false, true],
        ),
    )]);
    run_ast(file, "to_string(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(&["\"abc\"", "", "123"], vec![true, false, true]),
    )]);
}

fn test_try_to_type(file: &mut impl Write) {
    run_ast(file, "try_to_boolean(parse_json('true'))", &[]);
    run_ast(file, "try_to_boolean(parse_json('123'))", &[]);
    run_ast(file, "try_to_boolean(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_uint64(parse_json('123'))", &[]);
    run_ast(file, "try_to_uint64(parse_json('-123'))", &[]);
    run_ast(file, "try_to_uint64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_int64(parse_json('123'))", &[]);
    run_ast(file, "try_to_int64(parse_json('-123'))", &[]);
    run_ast(file, "try_to_int64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_float64(parse_json('12.34'))", &[]);
    run_ast(file, "try_to_float64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_date(parse_json('\"2023-01-01\"'))", &[]);
    run_ast(file, "try_to_date(parse_json('\"abc\"'))", &[]);
    run_ast(
        file,
        "try_to_timestamp(parse_json('\"2023-01-01 00:00:00\"'))",
        &[],
    );
    run_ast(file, "try_to_timestamp(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_string(parse_json('12.34'))", &[]);
    run_ast(file, "try_to_string(parse_json('\"abc\"'))", &[]);

    let columns = &[(
        "s",
        StringType::from_data_with_validity(
            &[
                "true",
                "123",
                "-100",
                "12.34",
                "",
                "\"2020-01-01\"",
                "\"2021-01-01 20:00:00\"",
                "\"abc\"",
            ],
            vec![true, true, true, true, false, true, true, true],
        ),
    )];
    run_ast(file, "try_to_boolean(parse_json(s))", columns);
    run_ast(file, "try_to_int64(parse_json(s))", columns);
    run_ast(file, "try_to_uint64(parse_json(s))", columns);
    run_ast(file, "try_to_float64(parse_json(s))", columns);
    run_ast(file, "try_to_date(parse_json(s))", columns);
    run_ast(file, "try_to_timestamp(parse_json(s))", columns);
    run_ast(file, "try_to_string(parse_json(s))", columns);
}

fn test_json_object(file: &mut impl Write) {
    run_ast(file, "json_object()", &[]);
    run_ast(
        file,
        "json_object('a', true, 'b', 1, 'c', 'str', 'd', [1,2], 'e', {'k':'v'})",
        &[],
    );
    run_ast(
        file,
        "json_object('k1', 1, 'k2', null, 'k3', 2, null, 3)",
        &[],
    );
    run_ast(file, "json_object('k1', 1, 'k1')", &[]);
    run_ast(file, "json_object('k1', 1, 'k1', 2)", &[]);
    run_ast(file, "json_object(1, 'k1', 2, 'k2')", &[]);

    run_ast(file, "json_object(k1, v1, k2, v2)", &[
        (
            "k1",
            StringType::from_data_with_validity(&["a1", "b1", "", "d1"], vec![
                true, true, false, true,
            ]),
        ),
        (
            "v1",
            StringType::from_data_with_validity(&["j1", "k1", "l1", ""], vec![
                true, true, true, false,
            ]),
        ),
        (
            "k2",
            StringType::from_data_with_validity(&["a2", "", "c2", "d2"], vec![
                true, false, true, true,
            ]),
        ),
        (
            "v2",
            StringType::from_data_with_validity(&["j2", "k2", "l2", "m2"], vec![
                true, true, true, true,
            ]),
        ),
    ]);
}

fn test_json_object_keep_null(file: &mut impl Write) {
    run_ast(file, "json_object_keep_null()", &[]);
    run_ast(
        file,
        "json_object_keep_null('a', true, 'b', 1, 'c', 'str', 'd', [1,2], 'e', {'k':'v'})",
        &[],
    );
    run_ast(
        file,
        "json_object_keep_null('k1', 1, 'k2', null, 'k3', 2, null, 3)",
        &[],
    );
    run_ast(file, "json_object_keep_null('k1', 1, 'k1')", &[]);
    run_ast(file, "json_object_keep_null('k1', 1, 'k1', 2)", &[]);
    run_ast(file, "json_object_keep_null(1, 'k1', 2, 'k2')", &[]);

    run_ast(file, "json_object_keep_null(k1, v1, k2, v2)", &[
        (
            "k1",
            StringType::from_data_with_validity(&["a1", "b1", "", "d1"], vec![
                true, true, false, true,
            ]),
        ),
        (
            "v1",
            StringType::from_data_with_validity(&["j1", "k1", "l1", ""], vec![
                true, true, true, false,
            ]),
        ),
        (
            "k2",
            StringType::from_data_with_validity(&["a2", "", "c2", "d2"], vec![
                true, false, true, true,
            ]),
        ),
        (
            "v2",
            StringType::from_data_with_validity(&["j2", "k2", "l2", "m2"], vec![
                true, true, true, true,
            ]),
        ),
    ]);
}

fn test_json_path_query_array(file: &mut impl Write) {
    run_ast(
        file,
        "json_path_query_array(parse_json('[1, 2, 3, 4, 5, 6]'), '$[0, 2 to last, 4]')",
        &[],
    );
    run_ast(
        file,
        "json_path_query_array(parse_json('[1, 2, 3, 4, 5, 6]'), '$[100]')",
        &[],
    );
    run_ast(
        file,
        "json_path_query_array(parse_json('[{\"a\": 1}, {\"a\": 2}]'), '$[*].a')",
        &[],
    );
    run_ast(
        file,
        "json_path_query_array(parse_json('[{\"a\": 1}, {\"a\": 2}]'), '$[*].a ? (@ == 1)')",
        &[],
    );
    run_ast(
        file,
        "json_path_query_array(parse_json('[{\"a\": 1}, {\"a\": 2}]'), '$[*].a ? (@ > 10)')",
        &[],
    );
    run_ast(
        file,
        "json_path_query_array(parse_json('[{\"a\": {\"b\":10}}, {\"a\": 2}]'), '$[*].a.b')",
        &[],
    );

    run_ast(file, "json_path_query_array(parse_json(s), p)", &[
        (
            "s",
            StringType::from_data_with_validity(
                &["true", "[{\"k\":1},{\"k\":2}]", "", "[1,2,3,4]"],
                vec![true, true, false, true],
            ),
        ),
        (
            "p",
            StringType::from_data(vec!["$[0]", "$[*].k", "$.a", "$[0,2]"]),
        ),
    ]);
}

fn test_json_path_query_first(file: &mut impl Write) {
    run_ast(
        file,
        "json_path_query_first(parse_json('[1, 2, 3, 4, 5, 6]'), '$[0, 2 to last, 4]')",
        &[],
    );
    run_ast(
        file,
        "json_path_query_first(parse_json('[1, 2, 3, 4, 5, 6]'), '$[100]')",
        &[],
    );
    run_ast(
        file,
        "json_path_query_first(parse_json('[{\"a\": 1}, {\"a\": 2}]'), '$[*].a')",
        &[],
    );
    run_ast(
        file,
        "json_path_query_first(parse_json('[{\"a\": 1}, {\"a\": 2}]'), '$[*].a ? (@ == 1)')",
        &[],
    );
    run_ast(
        file,
        "json_path_query_first(parse_json('[{\"a\": 1}, {\"a\": 2}]'), '$[*].a ? (@ > 10)')",
        &[],
    );
    run_ast(
        file,
        "json_path_query_first(parse_json('[{\"a\": {\"b\":10}}, {\"a\": 2}]'), '$[*].a.b')",
        &[],
    );

    run_ast(file, "json_path_query_first(parse_json(s), p)", &[
        (
            "s",
            StringType::from_data_with_validity(
                &["true", "[{\"k\":1},{\"k\":2}]", "", "[1,2,3,4]"],
                vec![true, true, false, true],
            ),
        ),
        (
            "p",
            StringType::from_data(vec!["$[0]", "$[*].k", "$.a", "$[0,2]"]),
        ),
    ]);
}
