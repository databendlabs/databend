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

use common_ast::parser::rule::error::pretty_print_error;
use common_ast::parser::rule::expr::expr;
use common_ast::parser::rule::statement::statement;
use common_ast::parser::token::*;
use goldenfile::Mint;
use nom::Parser;
use pretty_assertions::assert_eq;

macro_rules! test_parse {
    ($file:expr, $parser:expr, $source:literal $(,)*) => {
        let tokens = tokenise($source).unwrap();
        match $parser.parse(&(tokens)) {
            Ok((i, output)) => {
                writeln!($file, "---------- Input ----------").unwrap();
                writeln!($file, "{}", $source).unwrap();
                writeln!($file, "---------- Output ---------").unwrap();
                writeln!($file, "{}", output).unwrap();
                writeln!($file, "---------- AST ------------").unwrap();
                writeln!($file, "{:#?}", output).unwrap();
                writeln!($file, "\n").unwrap();
                assert_eq!(i[0].kind, TokenKind::EOI);
            }
            Err(err) => {
                let report = pretty_print_error($source, err).trim_end().to_string();
                writeln!($file, "---------- Input ----------").unwrap();
                writeln!($file, "{}", $source).unwrap();
                writeln!($file, "---------- Output ---------").unwrap();
                writeln!($file, "{}", report).unwrap();
                writeln!($file, "\n").unwrap();
            }
        }
    };
}

#[test]
fn test_statement() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("statement.txt").unwrap();
    test_parse!(file, statement, "truncate table a;");
    test_parse!(file, statement, r#"truncate table "a".b;"#,);
    test_parse!(file, statement, "drop table a;");
    test_parse!(file, statement, r#"drop table if exists a."b";"#,);
}

#[test]
fn test_expr() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("expr.txt").unwrap();
    test_parse!(file, expr, "a");
    test_parse!(file, expr, "1 + a * c.d");
    test_parse!(file, expr, "col1 not between 1 and 2");
    test_parse!(file, expr, "sum(col1)");
    test_parse!(
        file,
        expr,
        "G.E.B IS NOT NULL AND col1 not between col2 and (1 + col3) DIV sum(col4)",
    );
    test_parse!(
        file,
        expr,
        "sum(CASE WHEN n2.n_name = 'GERMANY' THEN ol_amount ELSE 0 END) / CASE WHEN sum(ol_amount) = 0 THEN 1 ELSE sum(ol_amount) END",
    );
    test_parse!(
        file,
        expr,
        "p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= CAST (1 AS smallint) AND l_quantity <= CAST (1 + 10 AS smallint)
            AND p_size BETWEEN CAST (1 AS smallint) AND CAST (5 AS smallint)
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'",
    );
}

#[test]
fn test_statement_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("statement-error.txt").unwrap();
    test_parse!(file, statement, "drop table if a.b;");
    test_parse!(file, statement, "truncate table a");
}

#[test]
fn test_expr_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("expr-error.txt").unwrap();
    test_parse!(file, expr, "(a and ) 1");
    test_parse!(file, expr, "a + +");
    test_parse!(
        file,
        expr,
        "G.E.B IS NOT NULL AND\n\tcol1 NOT BETWEEN col2 AND\n\t\tAND 1 + col3 DIV sum(col4)"
    );
}
