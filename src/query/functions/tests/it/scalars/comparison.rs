// Copyright 2021 Datafuse Labs
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

use databend_common_expression::types::*;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_comparison() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("comparison.txt").unwrap();

    test_eq(file);
    test_noteq(file);
    test_lt(file);
    test_lte(file);
    test_gt(file);
    test_gte(file);
    test_like(file);
    test_regexp(file);
}

fn test_eq(file: &mut impl Write) {
    run_ast(file, "'1'='2'", &[]);
    run_ast(file, "null=null", &[]);
    run_ast(file, "1=2", &[]);
    run_ast(file, "1.0=1", &[]);
    run_ast(file, "2.222>2.11", &[]);
    run_ast(file, "true=null", &[]);
    run_ast(file, "true=false", &[]);
    run_ast(file, "false=false", &[]);
    run_ast(file, "true=true", &[]);
    run_ast(file, "[]=[]", &[]);
    run_ast(file, "[1, 2]=[1, 2]", &[]);
    run_ast(file, "[true]=[]", &[]);
    run_ast(file, "(1, 'a') = (1,)", &[]);
    run_ast(file, "(1, 'a') = (1, 'a')", &[]);
    run_ast(file, "(1, 'a') = (1, 'b')", &[]);
    run_ast(file, "today()='2020-01-01'", &[]);
    run_ast(
        file,
        "to_timestamp(-315360000000000)=to_timestamp(-100)",
        &[],
    );

    run_ast(file, "lhs = rhs", &[
        (
            "lhs",
            UInt8Type::from_data(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ),
        (
            "rhs",
            Int64Type::from_data(vec![0i64, -1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ),
    ]);
    run_ast(file, "1.1=1.1", &[]);
    run_ast(
        file,
        r#"parse_json('[1,2,3,["a","b","c"]]') = parse_json('[1,2,3,["a","b","c"]]')"#,
        &[],
    );
    let table = [
        (
            "lhs",
            StringType::from_data(vec![
                r#"null"#,
                r#"true"#,
                r#"9223372036854775807"#,
                r#"-32768"#,
                r#"1234.5678"#,
                r#"{"k":"v","a":"b"}"#,
                r#"[1,2,3,["a","b","c"]]"#,
            ]),
        ),
        (
            "rhs",
            StringType::from_data(vec![
                r#"null"#,
                r#"true"#,
                r#"9223372036854775807"#,
                r#"-32768"#,
                r#"1234.5678"#,
                r#"{"k":"v","a":"d"}"#,
                r#"[1,2,3,["a","b","c"]]"#,
            ]),
        ),
    ];
    run_ast(file, "parse_json(lhs) = parse_json(rhs)", &table);
    run_ast(file, "lhs = rhs", &table);
}

fn test_noteq(file: &mut impl Write) {
    run_ast(file, "'1'!='2'", &[]);
    run_ast(file, "1!=2", &[]);
    run_ast(file, "1.1!=1.1", &[]);
    run_ast(file, "true != true", &[]);
    run_ast(file, "true != null", &[]);
    run_ast(file, "true != false", &[]);
    run_ast(file, "[] != []", &[]);
    run_ast(file, "['a'] != ['a']", &[]);
    run_ast(file, "['a'] != ['b']", &[]);
    run_ast(file, "(1, 'a') != (1,)", &[]);
    run_ast(file, "(1, 'a') != (1, 'a')", &[]);
    run_ast(file, "(1, 'a') != (1, 'b')", &[]);
    run_ast(
        file,
        "to_timestamp(-315360000000000)!=to_timestamp(-100)",
        &[],
    );
    run_ast(
        file,
        r#"parse_json('"databend"') != parse_json('"databend"')"#,
        &[],
    );
    let table = [
        (
            "lhs",
            StringType::from_data(vec![
                r#"null"#,
                r#"true"#,
                r#"9223372036854775807"#,
                r#"[1,2,3,["a","b","c"]]"#,
            ]),
        ),
        (
            "rhs",
            StringType::from_data(vec![
                r#"null"#,
                r#"true"#,
                r#"9223372036854775807"#,
                r#"[1,2,3,["a","b","c"]]"#,
            ]),
        ),
    ];
    run_ast(file, "parse_json(lhs) != parse_json(rhs)", &table);
    run_ast(file, "lhs != rhs", &table);
}

fn test_lt(file: &mut impl Write) {
    run_ast(file, "'1'<'2'", &[]);
    run_ast(file, "3<2", &[]);
    run_ast(file, "1.1<1.1", &[]);
    run_ast(file, "true < true", &[]);
    run_ast(file, "true < null", &[]);
    run_ast(file, "true < false", &[]);
    run_ast(file, "[] < []", &[]);
    run_ast(file, "[1, 2] < [2, 3]", &[]);
    run_ast(file, "(1, 'b') < (1, 'a')", &[]);
    run_ast(file, "(1, 'a') < (1, 'b')", &[]);
    run_ast(file, "(1, 'a') < (2, 'a')", &[]);
    run_ast(
        file,
        "to_timestamp(-315360000000000)<to_timestamp(-100)",
        &[],
    );
    run_ast(file, r#"parse_json('"true"') < parse_json('"false"')"#, &[]);
    let table = [
        (
            "lhs",
            StringType::from_data(vec![
                r#"null"#,
                r#"true"#,
                r#"9223372036854775807"#,
                r#"-32768"#,
                r#"1234.5678"#,
                r#"1.912e2"#,
                r#"[1,2,3,["a","b","c"]]"#,
            ]),
        ),
        (
            "rhs",
            StringType::from_data(vec![
                r#"null"#,
                r#"true"#,
                r#"9223372036854775800"#,
                r#"-33768"#,
                r#"1234.5678"#,
                r#"1.912e2"#,
                r#"[0,2,3,["a","b","c"]]"#,
            ]),
        ),
    ];
    run_ast(file, "parse_json(lhs) >= parse_json(rhs)", &table);
    run_ast(file, "lhs < rhs", &table);
}

fn test_lte(file: &mut impl Write) {
    run_ast(file, "'5'<='2'", &[]);
    run_ast(file, "1<=2", &[]);
    run_ast(file, "1.1<=2.1", &[]);
    run_ast(file, "true <= true", &[]);
    run_ast(file, "true <= null", &[]);
    run_ast(file, "true <= false", &[]);
    run_ast(file, "[] <= []", &[]);
    run_ast(file, "[1, 2] <= [2, 3]", &[]);
    run_ast(file, "(1, 'b') <= (1, 'a')", &[]);
    run_ast(file, "(1, 'a') <= (1, 'b')", &[]);
    run_ast(file, "(1, 'a') <= (2, 'a')", &[]);
    run_ast(file, "parse_json('null') <= parse_json('null')", &[]);
    run_ast(
        file,
        "to_timestamp(-315360000000000)<=to_timestamp(-100)",
        &[],
    );
    run_ast(
        file,
        "to_timestamp(-315360000000000)<=to_timestamp(-315360000000000)",
        &[],
    );
    let table = [
        (
            "lhs",
            StringType::from_data(vec![
                r#""databend""#,
                r#"{"k":"v","a":"b"}"#,
                r#"[1,2,3,["a","b","c"]]"#,
            ]),
        ),
        (
            "rhs",
            StringType::from_data(vec![
                r#""databend""#,
                r#"{"k":"a","a":"d"}"#,
                r#"[0,2,3,["a","b","c"]]"#,
            ]),
        ),
    ];
    run_ast(file, "parse_json(lhs) <= parse_json(rhs)", &table);
    run_ast(file, "lhs <= rhs", &table);
}

fn test_gt(file: &mut impl Write) {
    run_ast(file, "'3'>'2'", &[]);
    run_ast(file, "1>2", &[]);
    run_ast(file, "1.2>1.1", &[]);
    run_ast(file, "true > true", &[]);
    run_ast(file, "true > null", &[]);
    run_ast(file, "true > false", &[]);
    run_ast(file, "[] > []", &[]);
    run_ast(file, "[1, 2] > [2, 3]", &[]);
    run_ast(file, "(1, 'b') > (1, 'a')", &[]);
    run_ast(file, "(1, 'a') > (1, 'b')", &[]);
    run_ast(file, "(1, 'a') > (2, 'a')", &[]);
    run_ast(
        file,
        "to_timestamp(-315360000000000)>to_timestamp(-100)",
        &[],
    );
    run_ast(
        file,
        "to_timestamp(-315360000000000)>to_timestamp(-315360000000000)",
        &[],
    );
    run_ast(
        file,
        r#"parse_json('{"k":"v","a":"b"}') > parse_json('{"k":"v","a":"d"}')"#,
        &[],
    );
    let table = [
        (
            "lhs",
            StringType::from_data(vec![
                r#"null"#,
                r#"true"#,
                r#"9223372036854775807"#,
                r#"-32768"#,
                r#"1234.5678"#,
            ]),
        ),
        (
            "rhs",
            StringType::from_data(vec![
                r#"null"#,
                r#"true"#,
                r#"9223372036854775806"#,
                r#"-32768"#,
                r#"1234.5678"#,
            ]),
        ),
    ];
    run_ast(file, "parse_json(lhs) > parse_json(rhs)", &table);
    run_ast(file, "lhs > rhs", &table);
}

fn test_gte(file: &mut impl Write) {
    run_ast(file, "'2'>='1'", &[]);
    run_ast(file, "1>=2", &[]);
    run_ast(file, "1.1>=1.1", &[]);
    run_ast(file, "true >= true", &[]);
    run_ast(file, "true >= null", &[]);
    run_ast(file, "true >= false", &[]);
    run_ast(file, "[] >= []", &[]);
    run_ast(file, "[1, 2] >= [2, 3]", &[]);
    run_ast(file, "(1, 'b') >= (1, 'a')", &[]);
    run_ast(file, "(1, 'a') >= (1, 'b')", &[]);
    run_ast(file, "(1, 'a') >= (2, 'a')", &[]);
    run_ast(
        file,
        "to_timestamp(-315360000000000)>=to_timestamp(-100)",
        &[],
    );
    run_ast(
        file,
        "to_timestamp(-315360000000000)>=to_timestamp(-315360000000000)",
        &[],
    );
    run_ast(file, "parse_json('1.912e2') >= parse_json('1.912e2')", &[]);
    let table = [
        (
            "lhs",
            StringType::from_data(vec![
                r#"9223372036854775807"#,
                r#"-32768"#,
                r#"1234.5678"#,
                r#"1.912e2"#,
                r#""\\\"abc\\\"""#,
                r#"{"k":"v","a":"b"}"#,
                r#"[1,2,3,["a","b","d"]]"#,
            ]),
        ),
        (
            "rhs",
            StringType::from_data(vec![
                r#"9223372036854775806"#,
                r#"-32768"#,
                r#"1234.5678"#,
                r#"1.912e2"#,
                r#""\\\"abc\\\"""#,
                r#"{"k":"v","a":"d"}"#,
                r#"[1,2,3,["a","b","c"]]"#,
            ]),
        ),
    ];
    run_ast(file, "parse_json(lhs) >= parse_json(rhs)", &table);
    run_ast(file, "lhs >= rhs", &table);
}

fn test_like(file: &mut impl Write) {
    run_ast(file, "'1' like '2'", &[]);
    run_ast(file, "'hello\n' like 'h%'", &[]);
    run_ast(file, "'h\n' like 'h_'", &[]);
    run_ast(file, r#"'%' like '\%'"#, &[]);
    run_ast(file, r#"'v%xx' like '_\%%'"#, &[]);

    let columns = [(
        "lhs",
        StringType::from_data(vec!["abc", "abd", "abe", "abf"]),
    )];
    run_ast(file, "lhs like 'a%'", &columns);
    run_ast(file, "lhs like 'b%'", &columns);
    run_ast(file, "lhs like 'c'", &columns);

    let columns = [
        (
            "lhs",
            StringType::from_data(vec!["abc", "abd", "abe", "abf"]),
        ),
        ("rhs", StringType::from_data(vec!["a%", "_b_", "abe", "a"])),
    ];
    run_ast(file, "lhs like rhs", &columns);

    run_ast(file, "parse_json('\"hello\"') like 'h%'", &[]);
    run_ast(file, "parse_json('{\"abc\":1,\"def\":22}') like '%e%'", &[]);
    run_ast(
        file,
        "parse_json('{\"k1\":\"abc\",\"k2\":\"def\"}') like '%e%'",
        &[],
    );

    let columns = [(
        "lhs",
        StringType::from_data(vec!["\"abc\"", "{\"abd\":12}", "[\"abe\",\"abf\"]"]),
    )];
    run_ast(file, "parse_json(lhs) like 'a%'", &columns);
    run_ast(file, "parse_json(lhs) like '%ab%'", &columns);
}

fn test_regexp(file: &mut impl Write) {
    let columns = [
        (
            "lhs",
            StringType::from_data(vec!["abc", "abd", "abe", "abf", "abc", ""]),
        ),
        (
            "rhs",
            StringType::from_data(vec!["^a", "^b", "abe", "a", "", ""]),
        ),
    ];

    run_ast(file, "lhs regexp rhs", &columns);
    run_ast(file, "lhs rlike rhs", &columns);
}
