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
use databend_common_expression::types::*;
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
    test_object_keys(file);
    test_get(file);
    test_get_ignore_case(file);
    test_get_path(file);
    test_json_extract_path_text(file);
    test_as_type(file);
    test_is_type(file);
    test_to_type(file);
    test_try_to_type(file);
    test_object_construct(file);
    test_object_construct_keep_null(file);
    test_json_path_query_array(file);
    test_json_path_query_first(file);
    test_json_to_string(file);
    test_json_pretty(file);
    test_json_strip_nulls(file);
    test_json_typeof(file);
    test_array_construct(file);
    test_json_path_exists(file);
    test_get_arrow_op(file);
    test_get_string_arrow_op(file);
    test_get_by_keypath_op(file);
    test_get_by_keypath_string_op(file);
    test_exists_key_op(file);
    test_exists_any_keys_op(file);
    test_exists_all_keys_op(file);
    test_contains_in_left_op(file);
    test_contains_in_right_op(file);
    test_json_path_match(file);
    test_json_path_match_op(file);
    test_json_path_exists_op(file);
    test_concat_op(file);
    test_delete_by_name_op(file);
    test_delete_by_index_op(file);
    test_delete_by_keypath_op(file);
    test_array_insert(file);
    test_array_distinct(file);
    test_array_intersection(file);
    test_array_except(file);
    test_array_overlap(file);
    test_object_insert(file);
    test_object_delete(file);
    test_object_pick(file);
    test_strip_null_value(file);
    test_array_append(file);
    test_array_prepend(file);
    test_array_compact(file);
    test_array_flatten(file);
    test_array_indexof(file);
    test_array_remove(file);
    test_array_remove_first(file);
    test_array_remove_last(file);
    test_array_reverse(file);
    test_array_unique(file);
    test_array_contains(file);
    test_array_slice(file);
}

fn test_parse_json(file: &mut impl Write) {
    run_ast(file, "parse_json(NULL)", &[]);
    run_ast(file, "parse_json('null')", &[]);
    run_ast(file, "parse_json('true')", &[]);
    run_ast(file, "parse_json('false')", &[]);
    run_ast(file, "parse_json('\"测试\"')", &[]);
    run_ast(file, "parse_json('1234')", &[]);
    run_ast(file, "parse_json('[1,2,3,4]')", &[]);
    run_ast(file, "parse_json('{\"a\":\"b\",\"c\":\"d\"}')", &[]);
    run_ast(file, "parse_json('{\"k\":\"v\",\"k\":\"v2\"}')", &[]);

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
        StringType::from_data_with_validity(vec!["true", "false", "", "1234"], vec![
            true, true, false, true,
        ]),
    )]);

    // json extension syntax
    run_ast(file, "parse_json('  ')", &[]);
    run_ast(file, "parse_json('nuLL')", &[]);
    run_ast(file, "parse_json('+10')", &[]);
    run_ast(file, "parse_json('001')", &[]);
    run_ast(file, "parse_json('.12')", &[]);
    run_ast(file, "parse_json('12.')", &[]);
    run_ast(file, "parse_json('0xabc')", &[]);
    run_ast(file, "parse_json('0x12abc.def')", &[]);
    run_ast(
        file,
        "parse_json('99999999999999999999999999999999999999')",
        &[],
    );
    run_ast(file, r#"parse_json('\'single quoted string\'')"#, &[]);
    run_ast(file, "parse_json('[1,2,,4]')", &[]);
    run_ast(
        file,
        "parse_json('{ key :\"val\", key123_$测试 :\"val\" }')",
        &[],
    );
    run_ast(file, "parse_json('{ 123 :\"val\" }')", &[]);
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
    run_ast(file, "try_parse_json('{\"k\":\"v\",\"k\":\"v2\"}')", &[]);

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
        StringType::from_data_with_validity(vec!["true", "ttt", "", "1234"], vec![
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
        StringType::from_data_with_validity(vec!["true", "ttt", "", "1234"], vec![
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
            vec!["true", "[1,2,3,4]", "", "[\"a\",\"b\",\"c\"]"],
            vec![true, true, false, true],
        ),
    )]);
}

fn test_object_keys(file: &mut impl Write) {
    run_ast(file, "object_keys(parse_json('[1,2,3,4]'))", &[]);
    run_ast(
        file,
        "object_keys(parse_json('{\"k1\":\"v1\",\"k2\":\"v2\"}'))",
        &[],
    );

    run_ast(file, "object_keys(parse_json(s))", &[(
        "s",
        StringType::from_data(vec![
            "[1,2,3,4]",
            "{\"a\":\"b\",\"c\":\"d\"}",
            "{\"k1\":\"v1\",\"k2\":\"v2\"}",
        ]),
    )]);

    run_ast(file, "object_keys(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(
            vec![
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
                vec!["true", "[1,2,3,4]", "", "[\"a\",\"b\",\"c\"]"],
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
            StringType::from_data_with_validity(
                vec!["true", "{\"k\":1}", "", "{\"a\":\"b\"}"],
                vec![true, true, false, true],
            ),
        ),
        ("k", StringType::from_data(vec!["", "k", "", "a"])),
    ]);
}

fn test_get_arrow_op(file: &mut impl Write) {
    run_ast(file, "parse_json('null')->1", &[]);
    run_ast(file, "parse_json('null')->'k'", &[]);
    run_ast(file, "parse_json('[1,2,3,4]')->1", &[]);
    run_ast(file, "parse_json('[1,2,3,4]')->(2+3)", &[]);
    run_ast(file, "parse_json('{\"k\":\"v\"}')->'k'", &[]);
    run_ast(file, "parse_json('{\"k\":\"v\"}')->'x'", &[]);
    run_ast(file, "CAST(('a', 'b') AS VARIANT)->'2'", &[]);

    run_ast(file, "parse_json(s)->i", &[
        (
            "s",
            StringType::from_data(vec!["true", "[1,2,3,4]", "[\"a\",\"b\",\"c\"]"]),
        ),
        ("i", UInt64Type::from_data(vec![0u64, 0, 1])),
    ]);

    run_ast(file, "parse_json(s)->i", &[
        (
            "s",
            StringType::from_data_with_validity(
                vec!["true", "[1,2,3,4]", "", "[\"a\",\"b\",\"c\"]"],
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

    run_ast(file, "parse_json(s)->k", &[
        (
            "s",
            StringType::from_data(vec!["true", "{\"k\":1}", "{\"a\":\"b\"}"]),
        ),
        ("k", StringType::from_data(vec!["k", "k", "x"])),
    ]);

    run_ast(file, "parse_json(s)->k", &[
        (
            "s",
            StringType::from_data_with_validity(
                vec!["true", "{\"k\":1}", "", "{\"a\":\"b\"}"],
                vec![true, true, false, true],
            ),
        ),
        ("k", StringType::from_data(vec!["", "k", "", "a"])),
    ]);
}

fn test_get_string_arrow_op(file: &mut impl Write) {
    run_ast(file, "parse_json('null')->>1", &[]);
    run_ast(file, "parse_json('null')->>'k'", &[]);
    run_ast(file, "parse_json('[1,2,3,4]')->>1", &[]);
    run_ast(file, "parse_json('[1,2,3,4]')->>(2+3)", &[]);
    run_ast(file, "parse_json('{\"k\":\"v\"}')->>'k'", &[]);
    run_ast(file, "parse_json('{\"k\":\"v\"}')->>'x'", &[]);
    run_ast(file, "parse_json('{\"k\":null}')->>'k'", &[]);
    run_ast(file, "CAST(('a', 'b') AS VARIANT)->>'2'", &[]);

    run_ast(file, "parse_json(s)->>i", &[
        (
            "s",
            StringType::from_data(vec!["true", "[1,2,3,4]", "[\"a\",\"b\",\"c\"]"]),
        ),
        ("i", UInt64Type::from_data(vec![0u64, 0, 1])),
    ]);

    run_ast(file, "parse_json(s)->>i", &[
        (
            "s",
            StringType::from_data_with_validity(
                vec!["true", "[1,2,3,4]", "", "[\"a\",\"b\",\"c\"]"],
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

    run_ast(file, "parse_json(s)->>k", &[
        (
            "s",
            StringType::from_data(vec!["true", "{\"k\":1}", "{\"a\":\"b\"}"]),
        ),
        ("k", StringType::from_data(vec!["k", "k", "x"])),
    ]);

    run_ast(file, "parse_json(s)->>k", &[
        (
            "s",
            StringType::from_data_with_validity(
                vec!["true", "{\"k\":1}", "", "{\"a\":\"b\"}"],
                vec![true, true, false, true],
            ),
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
            StringType::from_data_with_validity(
                vec!["true", "{\"k\":1}", "", "{\"a\":\"b\"}"],
                vec![true, true, false, true],
            ),
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
            StringType::from_data_with_validity(
                vec!["true", "{\"k\":1}", "", "[\"a\",\"b\"]"],
                vec![true, true, false, true],
            ),
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
    run_ast(file, "json_extract_path_text('{\"a\":null}', 'a')", &[]);

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
            StringType::from_data_with_validity(
                vec!["true", "{\"k\":1}", "", "[\"a\",\"b\"]"],
                vec![true, true, false, true],
            ),
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
    run_ast(file, "as_decimal(parse_json('12.34'))", &[]);
    run_ast(file, "as_decimal(10, 2)(parse_json('12.34'))", &[]);
    run_ast(file, "as_string(parse_json('\"ab\"'))", &[]);
    run_ast(file, "as_string(parse_json('12.34'))", &[]);
    run_ast(file, "as_array(parse_json('[1,2,3]'))", &[]);
    run_ast(file, "as_array(parse_json('{\"a\":\"b\"}'))", &[]);
    run_ast(file, "as_object(parse_json('[1,2,3]'))", &[]);
    run_ast(file, "as_object(parse_json('{\"a\":\"b\"}'))", &[]);
    run_ast(file, "as_binary(to_binary('abcd')::variant)", &[]);
    run_ast(file, "as_date(to_date('2025-10-11')::variant)", &[]);
    run_ast(
        file,
        "as_timestamp(to_timestamp('2025-05-01 10:00:00')::variant)",
        &[],
    );
    run_ast(
        file,
        "as_interval(to_interval('1 year 2 month')::variant)",
        &[],
    );

    let columns = &[(
        "s",
        StringType::from_data(vec![
            "null",
            "true",
            "123",
            "12.34",
            "\"ab\"",
            "[1,2,3]",
            "{\"a\":\"b\"}",
        ]),
    )];
    run_ast(file, "as_boolean(parse_json(s))", columns);
    run_ast(file, "as_integer(parse_json(s))", columns);
    run_ast(file, "as_float(parse_json(s))", columns);
    run_ast(file, "as_string(parse_json(s))", columns);
    run_ast(file, "as_array(parse_json(s))", columns);
    run_ast(file, "as_object(parse_json(s))", columns);
}

fn test_is_type(file: &mut impl Write) {
    run_ast(file, "is_null_value(parse_json('null'))", &[]);
    run_ast(file, "is_null_value(parse_json('[1,2]'))", &[]);
    run_ast(file, "is_boolean(parse_json('true'))", &[]);
    run_ast(file, "is_boolean(parse_json('123'))", &[]);
    run_ast(file, "is_integer(parse_json('true'))", &[]);
    run_ast(file, "is_integer(parse_json('123'))", &[]);
    run_ast(file, "is_float(parse_json('\"ab\"'))", &[]);
    run_ast(file, "is_float(parse_json('12.34'))", &[]);
    run_ast(
        file,
        "is_decimal(parse_json('99999999999999999999999999999999999999'))",
        &[],
    );
    run_ast(
        file,
        "is_decimal(parse_json('99999999999999999999999999999999999999999999999999999999999999999999999999991'))",
        &[],
    );
    run_ast(file, "is_string(parse_json('\"ab\"'))", &[]);
    run_ast(file, "is_string(parse_json('12.34'))", &[]);
    run_ast(file, "is_array(parse_json('[1,2,3]'))", &[]);
    run_ast(file, "is_array(parse_json('{\"a\":\"b\"}'))", &[]);
    run_ast(file, "is_object(parse_json('[1,2,3]'))", &[]);
    run_ast(file, "is_object(parse_json('{\"a\":\"b\"}'))", &[]);
    run_ast(file, "is_binary(to_binary('abcd')::variant)", &[]);
    run_ast(file, "is_date(to_date('2025-10-11')::variant)", &[]);
    run_ast(
        file,
        "is_timestamp(to_timestamp('2025-05-01 10:00:00')::variant)",
        &[],
    );
    run_ast(
        file,
        "is_interval(to_interval('1 year 2 month')::variant)",
        &[],
    );

    let columns = &[(
        "s",
        StringType::from_data(vec![
            "null",
            "true",
            "123",
            "12.34",
            "\"ab\"",
            "[1,2,3]",
            "{\"a\":\"b\"}",
        ]),
    )];
    run_ast(file, "is_null_value(parse_json(s))", columns);
    run_ast(file, "is_boolean(parse_json(s))", columns);
    run_ast(file, "is_integer(parse_json(s))", columns);
    run_ast(file, "is_float(parse_json(s))", columns);
    run_ast(file, "is_string(parse_json(s))", columns);
    run_ast(file, "is_array(parse_json(s))", columns);
    run_ast(file, "is_object(parse_json(s))", columns);
}

fn test_to_type(file: &mut impl Write) {
    run_ast(file, "to_boolean(parse_json('null'))", &[]);
    run_ast(file, "to_boolean(parse_json('true'))", &[]);
    run_ast(file, "to_boolean(parse_json('123'))", &[]);
    run_ast(file, "to_boolean(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_uint64(parse_json('null'))", &[]);
    run_ast(file, "to_uint64(parse_json('123'))", &[]);
    run_ast(file, "to_uint64(parse_json('-123'))", &[]);
    run_ast(file, "to_uint64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_int64(parse_json('123'))", &[]);
    run_ast(file, "to_int64(parse_json('-123'))", &[]);
    run_ast(file, "to_int64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_float64(parse_json('12.34'))", &[]);
    run_ast(file, "to_float64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_date(parse_json('null'))", &[]);
    run_ast(file, "to_date(parse_json('\"2023-01-01\"'))", &[]);
    run_ast(file, "to_date(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_timestamp(parse_json('null'))", &[]);
    run_ast(
        file,
        "to_timestamp(parse_json('\"2023-01-01 00:00:00\"'))",
        &[],
    );
    run_ast(file, "to_timestamp(parse_json('\"abc\"'))", &[]);
    run_ast(file, "to_string(parse_json('null'))", &[]);
    run_ast(file, "to_string(parse_json('12.34'))", &[]);
    run_ast(file, "to_string(parse_json('\"abc\"'))", &[]);

    // 128 to 256/128
    run_ast(file, "to_decimal(3, 2)(to_variant(1.23))", &[]);
    run_ast(file, "to_decimal(2, 1)(to_variant(1.23))", &[]);
    run_ast(file, "to_decimal(1, 1)(to_variant(1.23))", &[]);
    run_ast(file, "to_decimal(76, 1)(to_variant(1.23))", &[]);
    run_ast(file, "to_decimal(4, 3)(to_variant(1.23))", &[]);
    run_ast(file, "to_decimal(3, 3)(to_variant(1.23))", &[]);
    run_ast(
        file,
        "to_decimal(38, 2)(to_variant(100000000000000000000000000000000000.01))",
        &[],
    );

    // 256 to 256/128
    run_ast(
        file,
        "to_decimal(39, 2)(to_variant(1000000000000000000000000000000000000.01))",
        &[],
    );
    run_ast(
        file,
        "to_decimal(38, 1)(to_variant(1000000000000000000000000000000000000.01))",
        &[],
    );
    run_ast(
        file,
        "to_decimal(38, 3)(to_variant(1000000000000000000000000000000000000.01))",
        &[],
    );
    run_ast(
        file,
        "to_decimal(39, 1)(to_variant(1000000000000000000000000000000000000.01))",
        &[],
    );
    run_ast(
        file,
        "to_decimal(39, 3)(to_variant(1000000000000000000000000000000000000.01))",
        &[],
    );

    run_ast(file, "to_decimal(3, 2)(parse_json('null'))", &[]);
    run_ast(file, "to_decimal(3, 2)(parse_json('\"3.14\"'))", &[]);
    run_ast(file, "to_decimal(2, 1)(parse_json('true'))", &[]);

    run_ast(file, "to_boolean(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(vec!["true", "", "true"], vec![true, false, true]),
    )]);
    run_ast(file, "to_int64(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(vec!["1", "", "-10"], vec![true, false, true]),
    )]);
    run_ast(file, "to_uint64(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(vec!["1", "", "20"], vec![true, false, true]),
    )]);
    run_ast(file, "to_float64(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(vec!["1.2", "", "100.2"], vec![true, false, true]),
    )]);
    run_ast(file, "to_date(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(vec!["\"2020-01-01\"", "", "\"2023-10-01\""], vec![
            true, false, true,
        ]),
    )]);
    run_ast(file, "to_timestamp(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(
            vec!["\"2020-01-01 00:00:00\"", "", "\"2023-10-01 10:11:12\""],
            vec![true, false, true],
        ),
    )]);
    run_ast(file, "to_string(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(vec!["\"abc\"", "", "123"], vec![true, false, true]),
    )]);
}

fn test_try_to_type(file: &mut impl Write) {
    run_ast(file, "try_to_boolean(parse_json('null'))", &[]);
    run_ast(file, "try_to_boolean(parse_json('true'))", &[]);
    run_ast(file, "try_to_boolean(parse_json('123'))", &[]);
    run_ast(file, "try_to_boolean(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_uint64(parse_json('null'))", &[]);
    run_ast(file, "try_to_uint64(parse_json('123'))", &[]);
    run_ast(file, "try_to_uint64(parse_json('-123'))", &[]);
    run_ast(file, "try_to_uint64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_int64(parse_json('123'))", &[]);
    run_ast(file, "try_to_int64(parse_json('-123'))", &[]);
    run_ast(file, "try_to_int64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_float64(parse_json('12.34'))", &[]);
    run_ast(file, "try_to_float64(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_date(parse_json('null'))", &[]);
    run_ast(file, "try_to_date(parse_json('\"2023-01-01\"'))", &[]);
    run_ast(file, "try_to_date(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_timestamp(parse_json('null'))", &[]);
    run_ast(
        file,
        "try_to_timestamp(parse_json('\"2023-01-01 00:00:00\"'))",
        &[],
    );
    run_ast(file, "try_to_timestamp(parse_json('\"abc\"'))", &[]);
    run_ast(file, "try_to_string(parse_json('null'))", &[]);
    run_ast(file, "try_to_string(parse_json('12.34'))", &[]);
    run_ast(file, "try_to_string(parse_json('\"abc\"'))", &[]);

    let columns = &[(
        "s",
        StringType::from_data_with_validity(
            vec![
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

fn test_object_construct(file: &mut impl Write) {
    run_ast(file, "object_construct()", &[]);
    run_ast(
        file,
        "object_construct('a', true, 'b', 1, 'c', 'str', 'd', [1,2], 'e', {'k':'v'})",
        &[],
    );
    run_ast(
        file,
        "object_construct('k1', 1, 'k2', null, 'k3', 2, null, 3)",
        &[],
    );
    run_ast(file, "object_construct('k1', 1, 'k1')", &[]);
    run_ast(file, "object_construct('k1', 1, 'k1', 2)", &[]);
    run_ast(file, "object_construct(1, 'k1', 2, 'k2')", &[]);

    run_ast(file, "object_construct(k1, v1, k2, v2)", &[
        (
            "k1",
            StringType::from_data_with_validity(vec!["a1", "b1", "", "d1"], vec![
                true, true, false, true,
            ]),
        ),
        (
            "v1",
            StringType::from_data_with_validity(vec!["j1", "k1", "l1", ""], vec![
                true, true, true, false,
            ]),
        ),
        (
            "k2",
            StringType::from_data_with_validity(vec!["a2", "", "c2", "d2"], vec![
                true, false, true, true,
            ]),
        ),
        (
            "v2",
            StringType::from_data_with_validity(vec!["j2", "k2", "l2", "m2"], vec![
                true, true, true, true,
            ]),
        ),
    ]);

    run_ast(file, "try_object_construct()", &[]);
    run_ast(
        file,
        "try_object_construct('a', true, 'b', 1, 'c', 'str', 'd', [1,2], 'e', {'k':'v'})",
        &[],
    );
    run_ast(
        file,
        "try_object_construct('k1', 1, 'k2', null, 'k3', 2, null, 3)",
        &[],
    );
    run_ast(file, "try_object_construct('k1', 1, 'k1')", &[]);
    run_ast(file, "try_object_construct('k1', 1, 'k1', 2)", &[]);
    run_ast(file, "try_object_construct(1, 'k1', 2, 'k2')", &[]);

    run_ast(file, "try_object_construct(k1, v1, k2, v2)", &[
        (
            "k1",
            StringType::from_data_with_validity(vec!["a1", "b1", "", "d1"], vec![
                true, true, false, true,
            ]),
        ),
        (
            "v1",
            StringType::from_data_with_validity(vec!["j1", "k1", "l1", ""], vec![
                true, true, true, false,
            ]),
        ),
        (
            "k2",
            StringType::from_data_with_validity(vec!["a2", "", "c2", "d2"], vec![
                true, false, true, true,
            ]),
        ),
        (
            "v2",
            StringType::from_data_with_validity(vec!["j2", "k2", "l2", "m2"], vec![
                true, true, true, true,
            ]),
        ),
    ]);
}

fn test_object_construct_keep_null(file: &mut impl Write) {
    run_ast(file, "object_construct_keep_null()", &[]);
    run_ast(
        file,
        "object_construct_keep_null('a', true, 'b', 1, 'c', 'str', 'd', [1,2], 'e', {'k':'v'})",
        &[],
    );
    run_ast(
        file,
        "object_construct_keep_null('k1', 1, 'k2', null, 'k3', 2, null, 3)",
        &[],
    );
    run_ast(file, "object_construct_keep_null('k1', 1, 'k1')", &[]);
    run_ast(file, "object_construct_keep_null('k1', 1, 'k1', 2)", &[]);
    run_ast(file, "object_construct_keep_null(1, 'k1', 2, 'k2')", &[]);

    run_ast(file, "object_construct_keep_null(k1, v1, k2, v2)", &[
        (
            "k1",
            StringType::from_data_with_validity(vec!["a1", "b1", "", "d1"], vec![
                true, true, false, true,
            ]),
        ),
        (
            "v1",
            StringType::from_data_with_validity(vec!["j1", "k1", "l1", ""], vec![
                true, true, true, false,
            ]),
        ),
        (
            "k2",
            StringType::from_data_with_validity(vec!["a2", "", "c2", "d2"], vec![
                true, false, true, true,
            ]),
        ),
        (
            "v2",
            StringType::from_data_with_validity(vec!["j2", "k2", "l2", "m2"], vec![
                true, true, true, true,
            ]),
        ),
    ]);

    run_ast(file, "try_object_construct_keep_null()", &[]);
    run_ast(
        file,
        "try_object_construct_keep_null('a', true, 'b', 1, 'c', 'str', 'd', [1,2], 'e', {'k':'v'})",
        &[],
    );
    run_ast(
        file,
        "try_object_construct_keep_null('k1', 1, 'k2', null, 'k3', 2, null, 3)",
        &[],
    );
    run_ast(file, "try_object_construct_keep_null('k1', 1, 'k1')", &[]);
    run_ast(file, "try_object_construct_keep_null('k1', 1, 'k1', 2)", &[
    ]);
    run_ast(file, "try_object_construct_keep_null(1, 'k1', 2, 'k2')", &[
    ]);

    run_ast(file, "try_object_construct_keep_null(k1, v1, k2, v2)", &[
        (
            "k1",
            StringType::from_data_with_validity(vec!["a1", "b1", "", "d1"], vec![
                true, true, false, true,
            ]),
        ),
        (
            "v1",
            StringType::from_data_with_validity(vec!["j1", "k1", "l1", ""], vec![
                true, true, true, false,
            ]),
        ),
        (
            "k2",
            StringType::from_data_with_validity(vec!["a2", "", "c2", "d2"], vec![
                true, false, true, true,
            ]),
        ),
        (
            "v2",
            StringType::from_data_with_validity(vec!["j2", "k2", "l2", "m2"], vec![
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
                vec!["true", "[{\"k\":1},{\"k\":2}]", "", "[1,2,3,4]"],
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
                vec!["true", "[{\"k\":1},{\"k\":2}]", "", "[1,2,3,4]"],
                vec![true, true, false, true],
            ),
        ),
        (
            "p",
            StringType::from_data(vec!["$[0]", "$[*].k", "$.a", "$[0,2]"]),
        ),
    ]);
}

fn test_json_to_string(file: &mut impl Write) {
    run_ast(file, "json_to_string(parse_json('true'))", &[]);
    run_ast(file, "json_to_string(parse_json('123456'))", &[]);
    run_ast(file, "json_to_string(parse_json('\"abcd\"'))", &[]);
    run_ast(file, "json_to_string(parse_json('[1, 2, 3, 4, 5, 6]'))", &[
    ]);
    run_ast(
        file,
        "json_to_string(parse_json('{\"k1\":123, \"k2\":\"abc\"}'))",
        &[],
    );
}

fn test_json_pretty(file: &mut impl Write) {
    run_ast(file, "json_pretty(parse_json('true'))", &[]);
    run_ast(file, "json_pretty(parse_json('123456'))", &[]);
    run_ast(file, "json_pretty(parse_json('\"abcd\"'))", &[]);
    run_ast(file, "json_pretty(parse_json('[1, 2, 3, 4, 5, 6]'))", &[]);
    run_ast(
        file,
        "json_pretty(parse_json('{\"k1\":123, \"k2\":\"abc\"}'))",
        &[],
    );
    run_ast(
        file,
        r#"json_pretty(parse_json('{"a":1,"b":true,"c":["1","2","3"],"d":{"a":1,"b":[1,2,3],"c":{"a":1,"b":2}}}'))"#,
        &[],
    );
}

fn test_json_strip_nulls(file: &mut impl Write) {
    run_ast(file, r#"json_strip_nulls(parse_json('true'))"#, &[]);
    run_ast(file, r#"json_strip_nulls(parse_json('null'))"#, &[]);
    run_ast(
        file,
        r#"json_strip_nulls(parse_json('[1, 2, 3, null]'))"#,
        &[],
    );
    run_ast(
        file,
        r#"json_strip_nulls(parse_json('{"a":null, "b": {"c": 1, "d": null}, "c": [{"a": 1, "b": null}]}'))"#,
        &[],
    );
}

fn test_json_typeof(file: &mut impl Write) {
    run_ast(file, r#"json_typeof(NULL)"#, &[]);
    run_ast(file, r#"json_typeof(parse_json('null'))"#, &[]);
    run_ast(file, r#"json_typeof(parse_json('true'))"#, &[]);
    run_ast(file, r#"json_typeof(parse_json('"test"'))"#, &[]);
    run_ast(file, r#"json_typeof(parse_json('123'))"#, &[]);
    run_ast(file, r#"json_typeof(parse_json('-1.12'))"#, &[]);
    run_ast(file, r#"json_typeof(parse_json('1.12e10'))"#, &[]);
    run_ast(file, r#"json_typeof(parse_json('[1,2,3]'))"#, &[]);
    run_ast(file, r#"json_typeof(parse_json('{"a":1,"b":2}'))"#, &[]);
}

fn test_array_construct(file: &mut impl Write) {
    run_ast(file, "array_construct()", &[]);
    run_ast(
        file,
        "array_construct(true, 1, 'str', [1,2], {'k':'v'}, null)",
        &[],
    );
    run_ast(file, "array_construct(v1, v2, v3)", &[
        (
            "v1",
            StringType::from_data_with_validity(vec!["a1", "b1", "", "d1"], vec![
                true, true, false, true,
            ]),
        ),
        (
            "v2",
            StringType::from_data_with_validity(vec!["j1", "k1", "l1", ""], vec![
                true, true, true, false,
            ]),
        ),
        (
            "v3",
            StringType::from_data_with_validity(vec!["a2", "", "c2", "d2"], vec![
                true, false, true, true,
            ]),
        ),
    ]);
}

fn test_json_path_exists(file: &mut impl Write) {
    run_ast(file, "json_path_exists(NULL, '$.a')", &[]);
    run_ast(
        file,
        r#"json_path_exists(parse_json('{"a": 1, "b": 2}'), NULL)"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_exists(parse_json('{"a": 1, "b": 2}'), '$.a')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_exists(parse_json('{"a": 1, "b": 2}'), '$.c')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_exists(parse_json('{"a": 1, "b": 2}'), '$.a ? (@ == 1)')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_exists(parse_json('{"a": 1, "b": 2}'), '$.a ? (@ > 1)')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_exists(parse_json('{"a": 1, "b": [1,2,3]}'), '$.b[0]')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_exists(parse_json('{"a": 1, "b": [1,2,3]}'), '$.b[3]')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_exists(parse_json('{"a": 1, "b": [1,2,3]}'), '$.b[1 to last] ? (@ >=2 && @ <=3)')"#,
        &[],
    );
}

fn test_json_path_exists_op(file: &mut impl Write) {
    run_ast(file, "NULL @? '$.a'", &[]);
    run_ast(file, r#"parse_json('{"a": 1, "b": 2}') @? NULL"#, &[]);
    run_ast(file, r#"parse_json('{"a": 1, "b": 2}') @? '$.a'"#, &[]);
    run_ast(file, r#"parse_json('{"a": 1, "b": 2}') @? '$.c'"#, &[]);
    run_ast(
        file,
        r#"parse_json('{"a": 1, "b": 2}') @? '$.a ? (@ == 1)'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a": 1, "b": 2}') @? '$.a ? (@ > 1)'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a": 1, "b": [1,2,3]}') @? '$.b[0]'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a": 1, "b": [1,2,3]}') @? '$.b[3]'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a": 1, "b": [1,2,3]}') @? '$.b[1 to last] ? (@ >=2 && @ <=3)'"#,
        &[],
    );
}

fn test_json_path_match(file: &mut impl Write) {
    run_ast(
        file,
        r#"json_path_match(parse_json('{"a":1,"b":2}'), '$.a == 1')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_match(parse_json('{"a":1,"b":2}'), '$.a > 1')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_match(parse_json('{"a":1,"b":2}'), '$.c > 0')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_match(parse_json('{"a":1,"b":2}'), '$.b < 2')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_match(parse_json('{"a":1,"b":[1,2,3]}'), '$.b[0] == 1')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_match(parse_json('{"a":1,"b":[1,2,3]}'), '$.b[0] > 1')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_match(parse_json('{"a":1,"b":[1,2,3]}'), '$.b[3] == 0')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_match(parse_json('{"a":1,"b":[1,2,3]}'), '$.b[1 to last] >= 2')"#,
        &[],
    );
    run_ast(
        file,
        r#"json_path_match(parse_json('{"a":1,"b":[1,2,3]}'), '$.b[1 to last] == 2 || $.b[1 to last] == 3')"#,
        &[],
    );
    run_ast(file, "json_path_match(parse_json(s), p)", &[
        (
            "s",
            StringType::from_data_with_validity(
                vec!["true", "[{\"k\":1},{\"k\":2}]", "", "[1,2,3,4]"],
                vec![true, true, false, true],
            ),
        ),
        (
            "p",
            StringType::from_data(vec!["$.a > 0", "$[*].k == 1", "$[*] > 1", "$[*] > 2"]),
        ),
    ]);
}

fn test_json_path_match_op(file: &mut impl Write) {
    run_ast(file, r#"parse_json('{"a":1,"b":2}') @@ '$.a == 1'"#, &[]);
    run_ast(file, r#"parse_json('{"a":1,"b":2}') @@ '$.a > 1'"#, &[]);
    run_ast(file, r#"parse_json('{"a":1,"b":2}') @@ '$.c > 0'"#, &[]);
    run_ast(file, r#"parse_json('{"a":1,"b":2}') @@ '$.b < 2'"#, &[]);
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":[1,2,3]}') @@ '$.b[0] == 1'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":[1,2,3]}') @@ '$.b[0] > 1'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":[1,2,3]}') @@ '$.b[3] == 0'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":[1,2,3]}') @@ '$.b[1 to last] >= 2'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":[1,2,3]}') @@ '$.b[1 to last] == 2 || $.b[1 to last] == 3'"#,
        &[],
    );
    run_ast(file, "parse_json(s) @@ p", &[
        (
            "s",
            StringType::from_data_with_validity(
                vec!["true", "[{\"k\":1},{\"k\":2}]", "", "[1,2,3,4]"],
                vec![true, true, false, true],
            ),
        ),
        (
            "p",
            StringType::from_data(vec!["$.a > 0", "$[*].k == 1", "$[*] > 1", "$[*] > 2"]),
        ),
    ]);
}

fn test_get_by_keypath_op(file: &mut impl Write) {
    run_ast(file, r#"parse_json('[10, 20, 30]') #> '1'"#, &[]);
    run_ast(file, "NULL #> NULL", &[]);
    run_ast(file, "NULL #> '{0}'", &[]);
    run_ast(file, r#"parse_json('"string"') #> '{0}'"#, &[]);
    run_ast(file, r#"parse_json('1') #> '{0}'"#, &[]);
    run_ast(file, r#"parse_json('[10, 20, 30]') #> '{1}'"#, &[]);
    run_ast(file, r#"parse_json('[10, 20, 30]') #> '{3}'"#, &[]);
    run_ast(file, r#"parse_json('[10, 20, 30]') #> '{a}'"#, &[]);
    run_ast(file, r#"parse_json('{"k": null}') #> '{k}'"#, &[]);
    run_ast(file, r#"parse_json('[10, 20, null]') #> '{2}'"#, &[]);
    run_ast(
        file,
        r#"parse_json('[10, {"a":{"k1":[1,2,3], "k2":2}}, 30]') #> '{1, a, k1}'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('[10, {"a":{"k1":[1,2,3], "k2":2}}, 30]') #> '{1, a, k1, 0}'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('[10, {"a":{"k1":[1,2,3], "k2":2}}, 30]') #> '{1, a, k1, 0, 10}'"#,
        &[],
    );
    run_ast(file, "parse_json(s) #> '{0}'", &[(
        "s",
        StringType::from_data_with_validity(
            vec!["[1,2,3]", "{\"k\":1}", "", "{\"a\":\"b\"}"],
            vec![true, true, false, true],
        ),
    )]);
    run_ast(file, "parse_json(s) #> k", &[
        (
            "s",
            StringType::from_data_with_validity(
                vec!["true", "{\"k\":1}", "", "{\"a\":\"b\"}"],
                vec![true, true, false, true],
            ),
        ),
        (
            "k",
            StringType::from_data_with_validity(vec!["{1}", "{k}", "", "{a}"], vec![
                true, true, false, true,
            ]),
        ),
    ]);
}

fn test_get_by_keypath_string_op(file: &mut impl Write) {
    run_ast(file, "NULL #>> '{0}'", &[]);
    run_ast(file, r#"parse_json('"string"') #>> '{0}'"#, &[]);
    run_ast(file, r#"parse_json('1') #>> '{0}'"#, &[]);
    run_ast(file, r#"parse_json('[10, 20, 30]') #>> '{1}'"#, &[]);
    run_ast(file, r#"parse_json('[10, 20, 30]') #>> '{3}'"#, &[]);
    run_ast(file, r#"parse_json('[10, 20, 30]') #>> '{a}'"#, &[]);
    run_ast(file, r#"parse_json('{"k": null}') #>> '{k}'"#, &[]);
    run_ast(file, r#"parse_json('[10, 20, null]') #>> '{2}'"#, &[]);
    run_ast(
        file,
        r#"parse_json('[10, {"a":{"k1":[1,2,3], "k2":2}}, 30]') #>> '{1, a, k1}'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('[10, {"a":{"k1":[1,2,3], "k2":2}}, 30]') #>> '{1, a, k1, 0}'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('[10, {"a":{"k1":[1,2,3], "k2":2}}, 30]') #>> '{1, a, k1, 0, 10}'"#,
        &[],
    );
    run_ast(file, "parse_json(s) #>> '{0}'", &[(
        "s",
        StringType::from_data_with_validity(
            vec!["[1,2,3]", "{\"k\":1}", "", "{\"a\":\"b\"}"],
            vec![true, true, false, true],
        ),
    )]);
    run_ast(file, "parse_json(s) #>> k", &[
        (
            "s",
            StringType::from_data_with_validity(
                vec!["true", "{\"k\":1}", "", "{\"a\":\"b\"}"],
                vec![true, true, false, true],
            ),
        ),
        (
            "k",
            StringType::from_data_with_validity(vec!["{1}", "{k}", "", "{a}"], vec![
                true, true, false, true,
            ]),
        ),
    ]);
}

fn test_exists_key_op(file: &mut impl Write) {
    run_ast(file, r#"parse_json('["1","2","3"]') ? NULL"#, &[]);
    run_ast(file, r#"parse_json('true') ? '1'"#, &[]);
    run_ast(file, r#"parse_json('["1","2","3"]') ? '1'"#, &[]);
    run_ast(file, r#"parse_json('["1","2","3"]') ? '4'"#, &[]);
    run_ast(file, r#"parse_json('{"a":1,"b":2,"c":3}') ? 'a'"#, &[]);
    run_ast(file, r#"parse_json('{"a":1,"b":2,"c":3}') ? 'd'"#, &[]);
    run_ast(file, "parse_json(s) ? 'a'", &[(
        "s",
        StringType::from_data_with_validity(vec!["[1,2,3]", r#"{"a":1}"#, "", r#"{"b":1}"#], vec![
            true, true, false, true,
        ]),
    )]);
}

fn test_exists_any_keys_op(file: &mut impl Write) {
    run_ast(file, r#"parse_json('["1","2","3"]') ?| NULL"#, &[]);
    run_ast(file, r#"parse_json('true') ?| ['1','2']"#, &[]);
    run_ast(file, r#"parse_json('["1","2","3"]') ?| ['1','2']"#, &[]);
    run_ast(file, r#"parse_json('["1","2","3"]') ?| ['4','5']"#, &[]);
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":2,"c":3}') ?| ['a','b']"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":2,"c":3}') ?| ['d','e']"#,
        &[],
    );
    run_ast(file, "parse_json(s) ?| ['a','b']", &[(
        "s",
        StringType::from_data_with_validity(
            vec![r#"["a","e","d"]"#, r#"{"a":1,"b":2}"#, "", r#"{"c":1}"#],
            vec![true, true, false, true],
        ),
    )]);
}

fn test_exists_all_keys_op(file: &mut impl Write) {
    run_ast(file, r#"parse_json('["1","2","3"]') ?& NULL"#, &[]);
    run_ast(file, r#"parse_json('true') ?& ['1','2']"#, &[]);
    run_ast(file, r#"parse_json('["1","2","3"]') ?& ['1','2']"#, &[]);
    run_ast(file, r#"parse_json('["1","2","3"]') ?& ['3','5']"#, &[]);
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":2,"c":3}') ?& ['a','b']"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":2,"c":3}') ?& ['c','e']"#,
        &[],
    );
    run_ast(file, "parse_json(s) ?& ['a','b']", &[(
        "s",
        StringType::from_data_with_validity(
            vec![
                r#"["a","e","b"]"#,
                r#"{"a":1,"b":2}"#,
                "",
                r#"{"a":0,"c":1}"#,
            ],
            vec![true, true, false, true],
        ),
    )]);
}

fn test_contains_in_left_op(file: &mut impl Write) {
    run_ast(file, "NULL @> NULL", &[]);
    run_ast(file, "parse_json('[1,2,3]') @> NULL", &[]);
    run_ast(file, "NULL @> parse_json('1')", &[]);
    run_ast(file, "parse_json('true') @> parse_json('true')", &[]);
    run_ast(file, "parse_json('true') @> parse_json('false')", &[]);
    run_ast(file, r#"parse_json('"asd"') @> parse_json('"asd"')"#, &[]);
    run_ast(file, r#"parse_json('"asd"') @> parse_json('"asdd"')"#, &[]);
    run_ast(file, "parse_json('[1,2,3]') @> parse_json('1')", &[]);
    run_ast(file, "parse_json('[1,2,3]') @> parse_json('4')", &[]);
    run_ast(file, "parse_json('[1,2,3,4]') @> parse_json('[2,1,3]')", &[
    ]);
    run_ast(file, "parse_json('[1,2,3,4]') @> parse_json('[2,1,1]')", &[
    ]);
    run_ast(file, "parse_json('[1,2,[1,3]]') @> parse_json('[1,3]')", &[
    ]);
    run_ast(
        file,
        "parse_json('[1,2,[1,3]]') @> parse_json('[[1,3]]')",
        &[],
    );
    run_ast(
        file,
        "parse_json('[1,2,[1,3]]') @> parse_json('[[[1,3]]]')",
        &[],
    );
    run_ast(
        file,
        r#"parse_json('[{"a":1}]') @> parse_json('{"a":1}')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('[{"a":1},{"b":2}]') @> parse_json('[{"a":1}]')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":2}') @> parse_json('{"a":1}')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":2}') @> parse_json('{"a":2}')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"z":2,"b":{"a":1}}') @> parse_json('{"a":1}')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":{"c":100,"d":200},"b":2}') @> parse_json('{"a":{}}')"#,
        &[],
    );
}

fn test_contains_in_right_op(file: &mut impl Write) {
    run_ast(file, "NULL <@ NULL", &[]);
    run_ast(file, "parse_json('[1,2,3]') <@ NULL", &[]);
    run_ast(file, "NULL <@ parse_json('1')", &[]);
    run_ast(file, "parse_json('true') <@ parse_json('true')", &[]);
    run_ast(file, "parse_json('true') <@ parse_json('false')", &[]);
    run_ast(file, r#"parse_json('"asd"') <@ parse_json('"asd"')"#, &[]);
    run_ast(file, r#"parse_json('"asd"') <@ parse_json('"asdd"')"#, &[]);
    run_ast(file, "parse_json('1') <@ parse_json('[1,2,3]')", &[]);
    run_ast(file, "parse_json('4') <@ parse_json('[1,2,3]')", &[]);
    run_ast(file, "parse_json('[2,1,3]') <@ parse_json('[1,2,3,4]')", &[
    ]);
    run_ast(file, "parse_json('[2,1,1]') <@ parse_json('[1,2,3,4]')", &[
    ]);
    run_ast(file, "parse_json('[1,3]') <@ parse_json('[1,2,[1,3]]')", &[
    ]);
    run_ast(
        file,
        "parse_json('[[1,3]]') <@ parse_json('[1,2,[1,3]]')",
        &[],
    );
    run_ast(
        file,
        "parse_json('[[[1,3]]]') <@ parse_json('[1,2,[1,3]]')",
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1}') <@ parse_json('[{"a":1}]')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('[{"a":1}]') <@ parse_json('[{"a":1},{"b":2}]')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1}') <@ parse_json('{"a":1,"b":2}')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":2}') <@ parse_json('{"a":1,"b":2}')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1}') <@ parse_json('{"z":2,"b":{"a":1}}')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":{}}') <@ parse_json('{"a":{"c":100,"d":200},"b":2}')"#,
        &[],
    );
}

fn test_concat_op(file: &mut impl Write) {
    run_ast(file, "parse_json('[1,2,3]') || NULL", &[]);
    run_ast(file, "parse_json('[1,2,3]') || parse_json('10')", &[]);
    run_ast(file, r#"parse_json('"asd"') || parse_json('[1,2,3]')"#, &[]);
    run_ast(
        file,
        r#"parse_json('[1,{"a":1,"b":2,"c":[1,2,3]},3]') || parse_json('"asd"')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('[1,{"a":1,"b":2,"c":[1,2,3]},3]') || parse_json('[10,20,30]')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('[1,[1,2,3],3]') || parse_json('[[10,20,30]]')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":2}') || parse_json('true')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('[1,2,3]') || parse_json('{"a":1,"b":2}')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":2}') || parse_json('[1,2,3]')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":2}') || parse_json('{"c":3,"d":4}')"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":2,"d":10}') || parse_json('{"a":3,"b":4}')"#,
        &[],
    );
}

fn test_delete_by_name_op(file: &mut impl Write) {
    run_ast(file, "parse_json('true') - '1'", &[]);
    run_ast(file, "parse_json('[1,2,3]') - '1'", &[]);
    run_ast(file, r#"parse_json('["1","2","3"]') - '0'"#, &[]);
    run_ast(file, r#"parse_json('["1","2","3"]') - '1'"#, &[]);
    run_ast(
        file,
        r#"parse_json('["1","2","3",{"a":1,"b":2}]') - '1'"#,
        &[],
    );
    run_ast(file, r#"parse_json('{"a":1,"b":2}') - 'c'"#, &[]);
    run_ast(file, r#"parse_json('{"a":1,"b":2}') - 'a'"#, &[]);
    run_ast(file, r#"parse_json('{"b":2}') - 'b'"#, &[]);
}

fn test_delete_by_index_op(file: &mut impl Write) {
    run_ast(file, "parse_json('true') - 1", &[]);
    run_ast(file, "parse_json('[1,2,3]') - 0", &[]);
    run_ast(file, "parse_json('[1,2,3]') - 1", &[]);
    run_ast(file, "parse_json('[1,2,3]') - 2", &[]);
    run_ast(file, "parse_json('[1,2,3]') - -1", &[]);
    run_ast(file, "parse_json('[1,2,3]') - -2", &[]);
    run_ast(file, "parse_json('[1,2,3]') - -3", &[]);
    run_ast(file, "parse_json('[1,2,3]') - -4", &[]);
    run_ast(
        file,
        r#"parse_json('[1,2,{"a":[1,2,3],"b":[40,50,60]}]') - 2"#,
        &[],
    );
}

fn test_delete_by_keypath_op(file: &mut impl Write) {
    run_ast(file, "parse_json('[1,2,3]') #- NULL", &[]);
    run_ast(file, "parse_json('[1,2,3]') #- '{}'", &[]);
    run_ast(file, "parse_json('[1,2,3]') #- '{0}'", &[]);
    run_ast(file, "parse_json('[1,2,3]') #- '{-1}'", &[]);
    run_ast(file, "parse_json('[1,2,3]') #- '{3}'", &[]);
    run_ast(file, r#"parse_json('{"a":1,"b":[1,2,3]}') #- '{b}'"#, &[]);
    run_ast(file, r#"parse_json('{"a":1,"b":[1,2,3]}') #- '{c}'"#, &[]);
    run_ast(file, r#"parse_json('{"a":1,"b":[1,2,3]}') #- '{b,2}'"#, &[]);
    run_ast(file, r#"parse_json('{"a":1,"b":[1,2,3]}') #- '{b,-2}'"#, &[
    ]);
    run_ast(file, r#"parse_json('{"a":1,"b":[1,2,3]}') #- '{b,20}'"#, &[
    ]);
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":[1,2,3]}') #- '{b,20,c,e}'"#,
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"a":1,"b":[{"c":1,"d":10},2,3]}') #- '{b,0,d}'"#,
        &[],
    );

    run_ast(file, "parse_json(s) #- '{0,a}'", &[(
        "s",
        StringType::from_data_with_validity(
            vec![r#"[{"a":1},2,3]"#, r#"[1,2,3]"#, "", r#"{"a":"b"}"#],
            vec![true, true, false, true],
        ),
    )]);

    run_ast(file, "parse_json(s) #- k", &[
        (
            "s",
            StringType::from_data_with_validity(
                vec![r#"[1,{"a":2},3]"#, r#"{"k":[1,2,3]}"#, "", r#"{"a":"b"}"#],
                vec![true, true, false, true],
            ),
        ),
        (
            "k",
            StringType::from_data_with_validity(vec!["{1,a}", "{k,-1}", "{k}", "{c}"], vec![
                true, true, false, true,
            ]),
        ),
    ]);
}

fn test_array_insert(file: &mut impl Write) {
    run_ast(
        file,
        r#"array_insert('[0,1,2,3]'::variant, 2, '"hello"'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_insert('[0,1,2,3]'::variant, 10, '100'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_insert('[0,1,2,3]'::variant, 0, 'true'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_insert('[0,1,2,3]'::variant, -1, '{"k":"v"}'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_insert('1'::variant, 1, '{"k":"v"}'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_insert('{"k":"v"}'::variant, 2, 'true'::variant)"#,
        &[],
    );

    run_ast(file, "array_insert(parse_json(v), 2, parse_json(n))", &[
        (
            "v",
            StringType::from_data_with_validity(
                vec!["[1,2,3,null]", r#"["A","B"]"#, "", r#"{"a":"b"}"#],
                vec![true, true, false, true],
            ),
        ),
        (
            "n",
            StringType::from_data_with_validity(vec![r#""hi""#, "", "true", "[1,2,3]"], vec![
                true, false, true, true,
            ]),
        ),
    ]);
}

fn test_array_distinct(file: &mut impl Write) {
    run_ast(file, r#"array_distinct('[0,1,1,2,2,2,3,4]'::variant)"#, &[]);
    run_ast(
        file,
        r#"array_distinct('["A","A","B","C","A","C"]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_distinct('["A","A",10,false,null,false,null,10]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_distinct('[[1,2,2],3,4,[1,2,2]]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_distinct('[{"k":"v"},"A","A","B",{"k":"v"}]'::variant)"#,
        &[],
    );
    run_ast(file, r#"array_distinct('1'::variant)"#, &[]);
    run_ast(file, r#"array_distinct('{"k":"v"}'::variant)"#, &[]);

    run_ast(file, "array_distinct(parse_json(v))", &[(
        "v",
        StringType::from_data_with_validity(
            vec![
                "[1,1,2,3,3,null,2,1,null]",
                r#"["A","B","A","B","C"]"#,
                "",
                r#"{"a":"b"}"#,
            ],
            vec![true, true, false, true],
        ),
    )]);
}

fn test_array_intersection(file: &mut impl Write) {
    run_ast(
        file,
        r#"array_intersection('["A","B","C"]'::variant, '["B","C"]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_intersection('["A","B","B","B","C"]'::variant, '["B","B"]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_intersection('[1,2]'::variant, '[3,4]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_intersection('[null,102,null]'::variant, '[null,null,103]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_intersection('[{"a":1,"b":2},1,2]'::variant, '[{"a":1,"b":2},3,4]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_intersection('[{"a":1,"b":2},1,2]'::variant, '[{"a":2,"c":3},3,4]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_intersection('[{"a":1,"b":2,"c":3}]'::variant, '[{"c":3,"b":2,"a":1},3,4]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_intersection('1'::variant, '1'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_intersection('1'::variant, '2'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_intersection('{"k":"v"}'::variant, '{"k":"v"}'::variant)"#,
        &[],
    );

    run_ast(
        file,
        "array_intersection(parse_json(v1), parse_json(v2))",
        &[
            (
                "v1",
                StringType::from_data_with_validity(
                    vec![
                        "[1,2,3,3,null,null]",
                        r#"["A","B","A","B","C"]"#,
                        "",
                        r#"{"a":"b"}"#,
                    ],
                    vec![true, true, false, true],
                ),
            ),
            (
                "v2",
                StringType::from_data_with_validity(
                    vec!["[1,1,2,3,4,5,null]", r#"["X","Y","Z"]"#, "", r#"{"a":"b"}"#],
                    vec![true, true, false, true],
                ),
            ),
        ],
    );
}

fn test_array_except(file: &mut impl Write) {
    run_ast(
        file,
        r#"array_except('["A","B","C"]'::variant, '["B","C"]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_except('["A","B","B","B","C"]'::variant, '["B","B"]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_except('[1,2]'::variant, '[3,4]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_except('[null,102,null]'::variant, '[null,null,103]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_except('[{"a":1,"b":2},1,2]'::variant, '[{"a":1,"b":2},3,4]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_except('[{"a":1,"b":2},1,2]'::variant, '[{"a":2,"c":3},3,4]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_except('[{"a":1,"b":2,"c":3}]'::variant, '[{"c":3,"b":2,"a":1},3,4]'::variant)"#,
        &[],
    );
    run_ast(file, r#"array_except('1'::variant, '1'::variant)"#, &[]);
    run_ast(file, r#"array_except('1'::variant, '2'::variant)"#, &[]);
    run_ast(
        file,
        r#"array_except('{"k":"v"}'::variant, '{"k":"v"}'::variant)"#,
        &[],
    );

    run_ast(file, "array_except(parse_json(v1), parse_json(v2))", &[
        (
            "v1",
            StringType::from_data_with_validity(
                vec![
                    "[1,2,3,3,null,null]",
                    r#"["A","B","A","B","C"]"#,
                    "",
                    r#"{"a":"b"}"#,
                ],
                vec![true, true, false, true],
            ),
        ),
        (
            "v2",
            StringType::from_data_with_validity(
                vec!["[1,1,2,3,4,5,null]", r#"["X","Y","Z"]"#, "", r#"{"a":"b"}"#],
                vec![true, true, false, true],
            ),
        ),
    ]);
}

fn test_array_overlap(file: &mut impl Write) {
    run_ast(
        file,
        r#"array_overlap('["A","B","C"]'::variant, '["B","C"]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_overlap('["A","B","B","B","C"]'::variant, '["B","B"]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_overlap('[1,2]'::variant, '[3,4]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_overlap('[null,102,null]'::variant, '[null,null,103]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_overlap('[{"a":1,"b":2},1,2]'::variant, '[{"a":1,"b":2},3,4]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_overlap('[{"a":1,"b":2},1,2]'::variant, '[{"a":2,"c":3},3,4]'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"array_overlap('[{"a":1,"b":2,"c":3}]'::variant, '[{"c":3,"b":2,"a":1},3,4]'::variant)"#,
        &[],
    );
    run_ast(file, r#"array_overlap('1'::variant, '1'::variant)"#, &[]);
    run_ast(file, r#"array_overlap('1'::variant, '2'::variant)"#, &[]);
    run_ast(
        file,
        r#"array_overlap('{"k":"v"}'::variant, '{"k":"v"}'::variant)"#,
        &[],
    );

    run_ast(file, "array_overlap(parse_json(v1), parse_json(v2))", &[
        (
            "v1",
            StringType::from_data_with_validity(
                vec![
                    "[1,2,3,3,null,null]",
                    r#"["A","B","A","B","C"]"#,
                    "",
                    r#"{"a":"b"}"#,
                ],
                vec![true, true, false, true],
            ),
        ),
        (
            "v2",
            StringType::from_data_with_validity(
                vec!["[1,1,2,3,4,5,null]", r#"["X","Y","Z"]"#, "", r#"{"a":"b"}"#],
                vec![true, true, false, true],
            ),
        ),
    ]);
}

fn test_object_insert(file: &mut impl Write) {
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'a', 'hello')"#,
        &[],
    );
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'n', 100)"#,
        &[],
    );
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'z', [10,20])"#,
        &[],
    );
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'x', '{"a":"b"}'::variant)"#,
        &[],
    );
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'v', null)"#,
        &[],
    );
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'b', null)"#,
        &[],
    );
    run_ast(file, r#"object_insert('{}'::variant, 'v', 'vv')"#, &[]);
    run_ast(file, r#"object_insert('123'::variant, 'v', 'vv')"#, &[]);
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'a', 'hello', true)"#,
        &[],
    );
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'n', 100, true)"#,
        &[],
    );
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'z', [10,20], true)"#,
        &[],
    );
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'x', '{"a":"b"}'::variant, true)"#,
        &[],
    );
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'v', null, true)"#,
        &[],
    );
    run_ast(
        file,
        r#"object_insert('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'b', null, true)"#,
        &[],
    );
    run_ast(file, r#"object_insert('{}'::variant, 'v', 'vv', true)"#, &[
    ]);
    run_ast(
        file,
        r#"object_insert('123'::variant, 'v', 'vv', true)"#,
        &[],
    );

    run_ast(file, "object_insert(parse_json(v), 'x', parse_json(n))", &[
        (
            "v",
            StringType::from_data_with_validity(
                vec![
                    r#"{"k":"v"}"#,
                    r#"{"m":"n"}"#,
                    "",
                    r#"{"a":"b","c":"d","y":"z"}"#,
                ],
                vec![true, true, false, true],
            ),
        ),
        (
            "n",
            StringType::from_data_with_validity(vec![r#""hi""#, "", "true", "[1,2,3]"], vec![
                true, false, true, true,
            ]),
        ),
    ]);
    run_ast(
        file,
        "object_insert(parse_json(v), 'c', parse_json(n), true)",
        &[
            (
                "v",
                StringType::from_data_with_validity(
                    vec![
                        r#"{"k":"v"}"#,
                        r#"{"m":"n"}"#,
                        "",
                        r#"{"a":"b","c":"d","y":"z"}"#,
                    ],
                    vec![true, true, false, true],
                ),
            ),
            (
                "n",
                StringType::from_data_with_validity(vec![r#""hi""#, "", "true", "[1,2,3]"], vec![
                    true, false, true, true,
                ]),
            ),
        ],
    );
}

fn test_object_delete(file: &mut impl Write) {
    run_ast(
        file,
        r#"object_delete('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'a', 'b', 'c')"#,
        &[],
    );
    run_ast(
        file,
        r#"object_delete('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'm', 'n', 'm', 'x')"#,
        &[],
    );
    run_ast(
        file,
        r#"object_delete('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'z', null)"#,
        &[],
    );
    run_ast(file, r#"object_delete('{}'::variant, 'v', 'vv')"#, &[]);
    run_ast(file, r#"object_delete('123'::variant, 'v', 'vv')"#, &[]);

    run_ast(file, "object_delete(parse_json(v), 'a', 'm')", &[(
        "v",
        StringType::from_data_with_validity(
            vec![
                r#"{"k":"v"}"#,
                r#"{"m":"n"}"#,
                "",
                r#"{"a":"b","c":"d","y":"z"}"#,
            ],
            vec![true, true, false, true],
        ),
    )]);
}

fn test_object_pick(file: &mut impl Write) {
    run_ast(
        file,
        r#"object_pick('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'a', 'b', 'c')"#,
        &[],
    );
    run_ast(
        file,
        r#"object_pick('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'm', 'n', 'm', 'x')"#,
        &[],
    );
    run_ast(
        file,
        r#"object_pick('{"b":12,"d":34,"m":[1,2],"x":{"k":"v"}}'::variant, 'z', null)"#,
        &[],
    );
    run_ast(file, r#"object_pick('{}'::variant, 'v', 'vv')"#, &[]);
    run_ast(file, r#"object_pick('123'::variant, 'v', 'vv')"#, &[]);

    run_ast(file, "object_pick(parse_json(v), 'a', 'm')", &[(
        "v",
        StringType::from_data_with_validity(
            vec![
                r#"{"k":"v"}"#,
                r#"{"m":"n"}"#,
                "",
                r#"{"a":"b","c":"d","y":"z"}"#,
            ],
            vec![true, true, false, true],
        ),
    )]);
}

fn test_strip_null_value(file: &mut impl Write) {
    run_ast(file, "strip_null_value(parse_json('1234'))", &[]);
    run_ast(file, "strip_null_value(parse_json('null'))", &[]);
    run_ast(file, "strip_null_value(null)", &[]);

    run_ast(file, "strip_null_value(parse_json(s))", &[(
        "s",
        StringType::from_data(vec![
            r#"{ "a": 1, "b": null }"#,
            "null",
            "[\"a\",\"b\",\"c\"]",
        ]),
    )]);

    run_ast(file, "strip_null_value(parse_json(s))", &[(
        "s",
        StringType::from_data_with_validity(
            vec![
                r#"{ "a": 1, "b": null }"#,
                "null",
                "",
                "[\"a\",\"b\",\"c\"]",
            ],
            vec![true, true, false, true],
        ),
    )]);
}

fn test_array_append(file: &mut impl Write) {
    // Test with simple arrays
    run_ast(file, "array_append(parse_json('[1, 2, 3]'), 4)", &[]);
    run_ast(file, "array_append(parse_json('[]'), 1)", &[]);

    // Test with null array
    run_ast(file, "array_append(null, 1)", &[]);

    // Test appending null
    run_ast(file, "array_append(parse_json('[1, 2, 3]'), null)", &[]);

    // Test with various data types
    run_ast(file, "array_append(parse_json('[1, 2]'), 'string')", &[]);
    run_ast(file, "array_append(parse_json('[1, 2]'), true)", &[]);
    run_ast(
        file,
        "array_append(parse_json('[1, 2]'), parse_json('{\"a\": 1}'))",
        &[],
    );

    // Test with column data
    run_ast(file, "array_append(parse_json(c1), c2)", &[
        ("c1", StringType::from_data(vec!["[1, 2, 3]", "[]", "null"])),
        ("c2", StringType::from_data(vec!["a", "b", "c"])),
    ]);
}

fn test_array_prepend(file: &mut impl Write) {
    // Test with simple arrays
    run_ast(file, "array_prepend(4, parse_json('[1, 2, 3]'))", &[]);
    run_ast(file, "array_prepend(1, parse_json('[]'))", &[]);

    // Test with null array
    run_ast(file, "array_prepend(1, null)", &[]);

    // Test prepending null
    run_ast(file, "array_prepend(null, parse_json('[1, 2, 3]'))", &[]);

    // Test with various data types
    run_ast(file, "array_prepend('string', parse_json('[1, 2]'))", &[]);
    run_ast(file, "array_prepend(true, parse_json('[1, 2]'))", &[]);
    run_ast(
        file,
        "array_prepend(parse_json('{\"a\": 1}'), parse_json('[1, 2]'))",
        &[],
    );

    // Test with column data
    run_ast(file, "array_prepend(c2, parse_json(c1))", &[
        ("c1", StringType::from_data(vec!["[1, 2, 3]", "[]", "null"])),
        ("c2", StringType::from_data(vec!["a", "b", "c"])),
    ]);
}

fn test_array_compact(file: &mut impl Write) {
    // Test with arrays containing nulls
    run_ast(file, "array_compact(parse_json('[1, null, 3, null]'))", &[]);
    run_ast(file, "array_compact(parse_json('[null, null]'))", &[]);

    // Test with array without nulls
    run_ast(file, "array_compact(parse_json('[1, 2, 3]'))", &[]);

    // Test with null
    run_ast(file, "array_compact(null)", &[]);

    // Test with non-array
    run_ast(file, "array_compact(parse_json('\"not an array\"'))", &[]);

    // Test with column data
    run_ast(file, "array_compact(parse_json(c1))", &[(
        "c1",
        StringType::from_data(vec![
            "[1, null, 3, null, \"a\"]",
            "[null, [1, true], null]",
            "[1, 2, 3]",
        ]),
    )]);
}

fn test_array_flatten(file: &mut impl Write) {
    // Test with nested arrays
    run_ast(file, "array_flatten(parse_json('[[1, 2], [3, 4]]'))", &[]);
    run_ast(file, "array_flatten(parse_json('[[], [1, 2]]'))", &[]);

    // Test with empty array
    run_ast(file, "array_flatten(parse_json('[]'))", &[]);

    // Test with null
    run_ast(file, "array_flatten(null)", &[]);

    // Test with non-array
    run_ast(file, "array_flatten(parse_json('\"not an array\"'))", &[]);

    // Test with column data
    run_ast(file, "array_flatten(parse_json(c1))", &[(
        "c1",
        StringType::from_data(vec!["[[1, 2], [3, \"aa\"]]", "[[], [1, 2]]", "[[]]"]),
    )]);
}

fn test_array_indexof(file: &mut impl Write) {
    // Test with string arrays
    run_ast(
        file,
        "array_indexof(parse_json('[\"a\", \"b\", \"c\"]'), 'b')",
        &[],
    );

    // Test with number arrays
    run_ast(file, "array_indexof(parse_json('[1, 2, 3]'), 2)", &[]);

    // Test with value not in array
    run_ast(file, "array_indexof(parse_json('[1, 2, 3]'), 4)", &[]);

    // Test with empty array
    run_ast(file, "array_indexof(parse_json('[]'), 'a')", &[]);

    // Test with null array
    run_ast(file, "array_indexof(null, 'a')", &[]);

    // Test with column data
    run_ast(file, "array_indexof(parse_json(c1), c2)", &[
        (
            "c1",
            StringType::from_data(vec!["[1, 2, 3]", "[\"a\", \"b\"]", "null"]),
        ),
        ("c2", StringType::from_data(vec!["a", "b", "c"])),
    ]);
}

fn test_array_remove(file: &mut impl Write) {
    // Test removing a string value
    run_ast(
        file,
        "array_remove(parse_json('[\"a\", \"b\", \"c\", \"b\"]'), 'b')",
        &[],
    );

    // Test removing a number value
    run_ast(file, "array_remove(parse_json('[1, 2, 3, 2]'), 2)", &[]);

    // Test removing a value not in array
    run_ast(file, "array_remove(parse_json('[1, 2, 3]'), 4)", &[]);

    // Test with empty array
    run_ast(file, "array_remove(parse_json('[]'), 'a')", &[]);

    // Test with null array
    run_ast(file, "array_remove(null, 'a')", &[]);

    // Test with column data
    run_ast(file, "array_remove(parse_json(c1), c2)", &[
        (
            "c1",
            StringType::from_data(vec!["[1, 2, 3]", "[\"a\", \"b\"]", "null"]),
        ),
        ("c2", StringType::from_data(vec!["a", "b", "c"])),
    ]);
}

fn test_array_remove_first(file: &mut impl Write) {
    // Test with simple arrays
    run_ast(file, "array_remove_first(parse_json('[1, 2, 3]'))", &[]);
    run_ast(file, "array_remove_first(parse_json('[1]'))", &[]);

    // Test with empty array
    run_ast(file, "array_remove_first(parse_json('[]'))", &[]);

    // Test with null
    run_ast(file, "array_remove_first(null)", &[]);

    // Test with column data
    run_ast(file, "array_remove_first(parse_json(c1))", &[(
        "c1",
        StringType::from_data(vec!["[1, 2, 3]", "[\"a\", \"b\"]", "null"]),
    )]);
}

fn test_array_remove_last(file: &mut impl Write) {
    // Test with simple arrays
    run_ast(file, "array_remove_last(parse_json('[1, 2, 3]'))", &[]);
    run_ast(file, "array_remove_last(parse_json('[1]'))", &[]);

    // Test with empty array
    run_ast(file, "array_remove_last(parse_json('[]'))", &[]);

    // Test with null
    run_ast(file, "array_remove_last(null)", &[]);

    // Test with column data
    run_ast(file, "array_remove_last(parse_json(c1))", &[(
        "c1",
        StringType::from_data(vec!["[1, 2, 3]", "[\"a\", \"b\"]", "null"]),
    )]);
}

fn test_array_reverse(file: &mut impl Write) {
    // Test with number arrays
    run_ast(file, "array_reverse(parse_json('[1, 2, 3]'))", &[]);

    // Test with string arrays
    run_ast(
        file,
        "array_reverse(parse_json('[\"a\", \"b\", \"c\"]'))",
        &[],
    );

    // Test with empty array
    run_ast(file, "array_reverse(parse_json('[]'))", &[]);

    // Test with null
    run_ast(file, "array_reverse(null)", &[]);

    // Test with column data
    run_ast(file, "array_reverse(parse_json(c1))", &[(
        "c1",
        StringType::from_data(vec!["[1, 2, 3]", "[\"a\", \"b\"]", "null"]),
    )]);
}

fn test_array_unique(file: &mut impl Write) {
    // Test with arrays containing duplicates
    run_ast(file, "array_unique(parse_json('[1, 2, 2, 3, 3]'))", &[]);
    run_ast(
        file,
        "array_unique(parse_json('[\"a\", \"b\", \"b\", \"c\"]'))",
        &[],
    );

    // Test with array without duplicates
    run_ast(file, "array_unique(parse_json('[1, 2, 3]'))", &[]);

    // Test with empty array
    run_ast(file, "array_unique(parse_json('[]'))", &[]);

    // Test with null
    run_ast(file, "array_unique(null)", &[]);

    // Test with column data
    run_ast(file, "array_unique(parse_json(c1))", &[(
        "c1",
        StringType::from_data(vec!["[1, 1, 2, 3]", "[\"a\", \"b\", \"a\"]", "null"]),
    )]);
}

fn test_array_contains(file: &mut impl Write) {
    // Test with simple arrays
    run_ast(file, "contains(parse_json('[1, 2, 3]'), 3)", &[]);
    run_ast(file, "contains(parse_json('[]'), 1)", &[]);
    run_ast(file, "contains(parse_json('[1, 2, 3]'), null)", &[]);

    // Test with various data types
    run_ast(file, "contains(parse_json('[1, 2]'), 'string')", &[]);
    run_ast(file, "contains(parse_json('[1, 2, true]'), true)", &[]);
    run_ast(
        file,
        "contains(parse_json('[1, 2, 3]'), parse_json('{\"a\": 1}'))",
        &[],
    );

    // Test with column data
    run_ast(file, "contains(parse_json(c1), c2)", &[
        (
            "c1",
            StringType::from_data(vec!["[1, 2, \"a\"]", "[]", "null"]),
        ),
        ("c2", StringType::from_data(vec!["a", "b", "c"])),
    ]);
}

fn test_array_slice(file: &mut impl Write) {
    // Test with simple arrays
    run_ast(file, "slice(parse_json('[]'), 1)", &[]);
    run_ast(file, "slice(parse_json('[0, 1, 2, 3]'), 2)", &[]);
    run_ast(file, "slice(parse_json('[1]'), 1, 2)", &[]);
    run_ast(file, "slice(parse_json('true'), 1, 2)", &[]);
    run_ast(file, "slice(parse_json('[null, 1, 2, 3]'), 0, 2)", &[]);
    run_ast(
        file,
        "slice(parse_json('[\"a\", \"b\", \"c\", \"d\"]'), -3, -1)",
        &[],
    );

    // Test with column data
    run_ast(file, "slice(parse_json(c1), c2, c3)", &[
        (
            "c1",
            StringType::from_data(vec!["[1, 2, \"a\"]", "[4, 5, 6, 7, 8]", "null"]),
        ),
        ("c2", Int64Type::from_data(vec![1, -3, 3])),
        ("c3", Int64Type::from_data(vec![4, -1, 3])),
    ]);
}
