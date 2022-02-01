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

use common_ast::parser::rule::expr::*;
use common_ast::parser::rule::statement::*;
use common_ast::parser::rule::util::Input;
use common_ast::parser::token::*;
use nom::Parser;
use pretty_assertions::assert_eq;

macro_rules! assert_parse {
    ($parser:expr, $source:literal, $expected:literal $(,)*) => {
        let tokens = tokenise($source).unwrap();
        let res: nom::IResult<_, _, nom_supreme::error::ErrorTree<Input>> =
            $parser.parse(&(tokens));
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

// TODO (andylokandy): test tree structure, maybe add parentheses?
#[test]
fn test_expr() {
    assert_parse!(expr, "1 + a * c.d", "1 + a * c.d");
    assert_parse!(expr, "col1 not between 1 and 2", "col1 NOT BETWEEN 1 AND 2");
    assert_parse!(expr, "sum(col1)", "sum(col1)");
    assert_parse!(
        expr,
        "G.E.B IS NOT NULL AND col1 not between col2 and (1 + col3) DIV sum(col4)",
        "G.E.B IS NOT NULL AND col1 NOT BETWEEN col2 AND 1 + col3 DIV sum(col4)"
    );
}
