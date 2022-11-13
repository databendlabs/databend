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
use common_expression::types::NumberDataType;
use common_expression::utils::ColumnFrom;
use common_expression::Column;
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

    let like_columns = [
        (
            "lhs",
            DataType::String,
            Column::from_data(vec!["abc", "abd", "abe", "abf"]),
        ),
        (
            "rhs",
            DataType::String,
            Column::from_data(vec!["a%", "_b_", "abe", "a"]),
        ),
    ];
    test_like(file, &like_columns);
    test_notlike(file, &like_columns);

    let regexp_columns = [
        (
            "lhs",
            DataType::String,
            Column::from_data(vec!["abc", "abd", "abe", "abf", "abc", ""]),
        ),
        (
            "rhs",
            DataType::String,
            Column::from_data(vec!["^a", "^b", "abe", "a", "", ""]),
        ),
    ];
    test_regexp(file, &regexp_columns);
    test_notregexp(file, &regexp_columns);
}

fn test_eq(file: &mut impl Write) {
    run_ast(file, "'1'='2'", &[]);
    run_ast(file, "null=null", &[]);
    run_ast(file, "1=2", &[]);
    run_ast(file, "1.0=1", &[]);
    run_ast(file, "true=null", &[]);
    run_ast(file, "true=false", &[]);
    run_ast(file, "false=false", &[]);
    run_ast(file, "true=true", &[]);
    run_ast(
        file,
        "to_timestamp(-315360000000000)=to_timestamp(-100)",
        &[],
    );

    run_ast(file, "lhs = rhs", &[
        (
            "lhs",
            DataType::Number(NumberDataType::UInt8),
            Column::from_data(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ),
        (
            "rhs",
            DataType::Number(NumberDataType::Int64),
            Column::from_data(vec![0i64, -1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
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
            DataType::String,
            Column::from_data(vec![
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
            DataType::String,
            Column::from_data(vec![
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
            DataType::String,
            Column::from_data(vec![
                r#"null"#,
                r#"true"#,
                r#"9223372036854775807"#,
                r#"[1,2,3,["a","b","c"]]"#,
            ]),
        ),
        (
            "rhs",
            DataType::String,
            Column::from_data(vec![
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
    run_ast(
        file,
        "to_timestamp(-315360000000000)<to_timestamp(-100)",
        &[],
    );
    run_ast(file, r#"parse_json('"true"') < parse_json('"false"')"#, &[]);
    let table = [
        (
            "lhs",
            DataType::String,
            Column::from_data(vec![
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
            DataType::String,
            Column::from_data(vec![
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
            DataType::String,
            Column::from_data(vec![
                r#""databend""#,
                r#"{"k":"v","a":"b"}"#,
                r#"[1,2,3,["a","b","c"]]"#,
            ]),
        ),
        (
            "rhs",
            DataType::String,
            Column::from_data(vec![
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
            DataType::String,
            Column::from_data(vec![
                r#"null"#,
                r#"true"#,
                r#"9223372036854775807"#,
                r#"-32768"#,
                r#"1234.5678"#,
            ]),
        ),
        (
            "rhs",
            DataType::String,
            Column::from_data(vec![
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
            DataType::String,
            Column::from_data(vec![
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
            DataType::String,
            Column::from_data(vec![
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

fn test_like(file: &mut impl Write, columns: &[(&str, DataType, Column)]) {
    run_ast(file, "'1' like '2'", &[]);
    run_ast(file, "'hello\n' like 'h%'", &[]);
    run_ast(file, "'h\n' like 'h_'", &[]);
    run_ast(file, r#"'%' like '\%'"#, &[]);
    run_ast(file, r#"'v%xx' like '_\%%'"#, &[]);

    let like_columns = [(
        "lhs",
        DataType::String,
        Column::from_data(vec!["abc", "abd", "abe", "abf"]),
    )];
    run_ast(file, "lhs like 'a%'", &like_columns);
    run_ast(file, "lhs like rhs", columns);
}

fn test_notlike(file: &mut impl Write, columns: &[(&str, DataType, Column)]) {
    run_ast(file, "'1' not like '2'", &[]);

    let like_columns = [(
        "lhs",
        DataType::String,
        Column::from_data(vec!["abc", "abd", "abe", "abf"]),
    )];
    run_ast(file, "lhs not like 'a%'", &like_columns);
    run_ast(file, "lhs not like rhs", columns);
}

fn test_regexp(file: &mut impl Write, columns: &[(&str, DataType, Column)]) {
    run_ast(file, "lhs regexp rhs", columns);
    run_ast(file, "lhs rlike rhs", columns);
}

fn test_notregexp(file: &mut impl Write, columns: &[(&str, DataType, Column)]) {
    run_ast(file, "lhs not regexp rhs", columns);
    run_ast(file, "lhs not rlike rhs", columns);
}
