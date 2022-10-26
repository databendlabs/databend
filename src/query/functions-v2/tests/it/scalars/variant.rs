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

use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ColumnFrom;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_variant() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("variant.txt").unwrap();

    test_parse_json(file);
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
        DataType::String,
        Column::from_data(vec![
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
}
