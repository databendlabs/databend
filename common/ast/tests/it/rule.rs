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

use common_ast::parser::rule::error::pretty_print_error;
use common_ast::parser::rule::error::Error;
use common_ast::parser::rule::expr::expr;
use common_ast::parser::rule::statement::statement;
use common_ast::parser::token::*;
use indoc::indoc;
use nom::Parser;
use pretty_assertions::assert_eq;

macro_rules! assert_parse {
    ($parser:expr, $source:literal, $expected:expr $(,)*) => {
        let tokens = tokenise($source).unwrap();
        let res: nom::IResult<_, _, Error> = $parser.parse(&(tokens));
        let (i, output) = res.unwrap();

        assert_eq!(&format!("{}", output), $expected);
        assert_eq!(i[0].kind, TokenKind::EOI);
    };
}

macro_rules! assert_parse_error {
    ($parser:expr, $source:literal, $expected:expr $(,)*) => {
        let tokens = tokenise($source).unwrap();
        let res: nom::IResult<_, _, Error> = $parser.parse(&(tokens));
        let err = res.unwrap_err();

        let output = pretty_print_error($source, err).trim_end().to_string();
        if output != $expected.trim() {
            panic!("assertion failed: error message mismatched\noutput:\n{output}");
        }
    };
}

#[test]
fn test_statement() {
    assert_parse!(statement, "truncate table a;", "TRUNCATE TABLE a");
    assert_parse!(
        statement,
        r#"truncate table "a".b;"#,
        r#"TRUNCATE TABLE "a".b"#,
    );
    assert_parse!(statement, "drop table a;", "DROP TABLE a");
    assert_parse!(
        statement,
        r#"drop table if exists a."b";"#,
        r#"DROP TABLE IF EXISTS a."b""#,
    );
}

// TODO (andylokandy): test tree structure, maybe add parentheses?
#[test]
fn test_expr() {
    assert_parse!(expr, "a", "a");
    assert_parse!(expr, "1 + a * c.d", "1 + a * c.d");
    assert_parse!(expr, "col1 not between 1 and 2", "col1 NOT BETWEEN 1 AND 2");
    assert_parse!(expr, "sum(col1)", "sum(col1)");
    assert_parse!(
        expr,
        "G.E.B IS NOT NULL AND col1 not between col2 and (1 + col3) DIV sum(col4)",
        "G.E.B IS NOT NULL AND col1 NOT BETWEEN col2 AND 1 + col3 DIV sum(col4)"
    );
}

#[test]
fn test_statement_error() {
    assert_parse_error!(statement, "drop table if a.b;", indoc! {
        r#"
            error: 
              --> SQL:1:12
              |
            1 | drop table if a.b;
              | ----       ^^ expected token <Ident>
              | |           
              | while parsing DROP TABLE statement
        "#
    });
    assert_parse_error!(statement, "truncate table a", indoc! {
        r#"
            error: 
              --> SQL:1:17
              |
            1 | truncate table a
              | --------        ^ expected token ";"
              | |               
              | while parsing TRUNCATE TABLE statement
        "#
    });
}

#[test]
fn test_expr_error() {
    assert_parse_error!(expr, "(a and ) 1", indoc! {
        r#"
            error: 
              --> SQL:1:8
              |
            1 | (a and ) 1
              |        ^ unexpected end of expression
        "#
    });
    assert_parse_error!(expr, "a + +", indoc! {
        r#"
            error: 
              --> SQL:1:5
              |
            1 | a + +
              |     ^ unable to parse the binary operator
        "#
    });
    assert_parse_error!(
        expr,
        "G.E.B IS NOT NULL AND\n\tcol1 NOT BETWEEN col2 AND\n\t\tAND 1 + col3 DIV sum(col4)",
        indoc! {
            r#"
                error: 
                  --> SQL:3:3
                  |
                1 | G.E.B IS NOT NULL AND
                2 |     col1 NOT BETWEEN col2 AND
                3 |         AND 1 + col3 DIV sum(col4)
                  |         ^^^ unexpected end of expression
            "#
        }
    );
}
