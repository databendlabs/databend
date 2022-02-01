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
use pretty_assertions::assert_eq;

macro_rules! assert_parse {
    ($parser:expr, $source:literal, $expected:literal $(,)*) => {
        let tokens = tokenise($source).unwrap();
        let res: nom::IResult<_, _, nom_supreme::error::ErrorTree<Input>> = $parser.parse(&tokens);
        let (i, output) = res.unwrap();

        assert_eq!(&format!("{}", output), $expected);
        assert_eq!(i, &[]);
    };
}

#[test]
fn test_statement() {
    assert_parse!(truncate_table, "truncate table a;", "TRUNCATE TABLE a");
    assert_parse!(
        truncate_table,
        r#"truncate table "a".b;"#,
        r#"TRUNCATE TABLE "a".b"#,
    );
    assert_parse!(drop_table, "drop table a;", "DROP TABLE a");
    assert_parse!(
        drop_table,
        r#"drop table if exists a."b";"#,
        r#"DROP TABLE IF EXISTS a."b""#,
    );
}
