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

use common_ast::parser::rule::statement::*;
use common_ast::parser::rule::util::Input;
use common_ast::parser::token::*;
use nom::Parser;
use nom_supreme::error::ErrorTree;
use pretty_assertions::assert_eq;

#[test]
fn test_truncate_table() {
    assert_parse(
        truncate_table,
        &tokenise("truncate table a;").unwrap(),
        "TRUNCATE TABLE a",
    );
    assert_parse(
        truncate_table,
        &tokenise(r#"truncate table "a".b;"#).unwrap(),
        r#"TRUNCATE TABLE "a".b"#,
    );
}

#[test]
fn test_drop_table() {
    assert_parse(
        drop_table,
        &tokenise("drop table a;").unwrap(),
        "DROP TABLE a",
    );
    assert_parse(
        drop_table,
        &tokenise(r#"drop table if exists a."b";"#).unwrap(),
        r#"DROP TABLE IF EXISTS a."b""#,
    );
}

fn assert_parse<'a, P, Output>(mut parser: P, source: Input<'a>, expected: &str)
where
    P: Parser<Input<'a>, Output, ErrorTree<Input<'a>>>,
    Output: PartialEq + std::fmt::Debug + std::fmt::Display,
{
    let (i, output) = parser.parse(source).unwrap();

    assert_eq!(&format!("{}", output), expected);
    assert_eq!(i, &[]);
}
