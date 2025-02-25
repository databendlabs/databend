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

use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;
use pretty_assertions::assert_eq;

use crate::ast::ExplainKind;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Literal;
use crate::ast::SelectTarget;
use crate::ast::Statement;
use crate::ast::StatementWithFormat;
use crate::parser::common::comma_separated_list0;
use crate::parser::common::comma_separated_list1;
use crate::parser::common::ident;
use crate::parser::common::transform_span;
use crate::parser::common::IResult;
use crate::parser::error::display_parser_error;
use crate::parser::expr::expr;
use crate::parser::expr::values;
use crate::parser::input::Dialect;
use crate::parser::input::Input;
use crate::parser::input::ParseMode;
use crate::parser::statement::insert_stmt;
use crate::parser::statement::replace_stmt;
use crate::parser::statement::statement;
use crate::parser::token::Token;
use crate::parser::token::TokenKind;
use crate::parser::token::Tokenizer;
use crate::parser::Backtrace;
use crate::ParseError;
use crate::Range;
use crate::Result;

pub fn tokenize_sql(sql: &str) -> Result<Vec<Token>> {
    Tokenizer::new(sql).collect::<Result<Vec<_>>>()
}

/// Parse a SQL string into `Statement`s.
#[fastrace::trace]
pub fn parse_sql(tokens: &[Token], dialect: Dialect) -> Result<(Statement, Option<String>)> {
    let stmt = run_parser(tokens, dialect, ParseMode::Default, false, statement)?;

    #[cfg(debug_assertions)]
    assert_reparse(tokens[0].source, stmt.clone());

    Ok((stmt.stmt, stmt.format))
}

/// Parse udf function into Expr
pub fn parse_expr(tokens: &[Token], dialect: Dialect) -> Result<Expr> {
    run_parser(tokens, dialect, ParseMode::Default, false, expr)
}

pub fn parse_comma_separated_exprs(tokens: &[Token], dialect: Dialect) -> Result<Vec<Expr>> {
    run_parser(tokens, dialect, ParseMode::Default, true, |i| {
        comma_separated_list0(expr)(i)
    })
}

pub fn parse_comma_separated_idents(tokens: &[Token], dialect: Dialect) -> Result<Vec<Identifier>> {
    run_parser(tokens, dialect, ParseMode::Default, true, |i| {
        comma_separated_list1(ident)(i)
    })
}

pub fn parse_values(tokens: &[Token], dialect: Dialect) -> Result<Vec<Expr>> {
    run_parser(tokens, dialect, ParseMode::Default, false, values)
}

pub fn parse_raw_insert_stmt(tokens: &[Token], dialect: Dialect) -> Result<Statement> {
    run_parser(
        tokens,
        dialect,
        ParseMode::Default,
        false,
        insert_stmt(true),
    )
}

pub fn parse_raw_replace_stmt(tokens: &[Token], dialect: Dialect) -> Result<Statement> {
    run_parser(
        tokens,
        dialect,
        ParseMode::Default,
        false,
        replace_stmt(true),
    )
}

pub fn run_parser<O>(
    tokens: &[Token],
    dialect: Dialect,
    mode: ParseMode,
    allow_partial: bool,
    mut parser: impl FnMut(Input) -> IResult<O>,
) -> Result<O> {
    let backtrace = Backtrace::new();
    let input = Input {
        tokens,
        dialect,
        mode,
        backtrace: &backtrace,
    };
    match parser(input) {
        Ok((rest, res)) => {
            let is_complete = rest[0].kind == TokenKind::EOI;
            if is_complete || allow_partial {
                Ok(res)
            } else {
                Err(ParseError(
                    transform_span(&rest[..1]),
                    "unable to parse rest of the sql".to_string(),
                ))
            }
        }
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            let source = tokens[0].source;
            Err(ParseError(None, display_parser_error(err, source)))
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}

/// Check that the statement can be displayed and reparsed without loss
#[allow(dead_code)]
fn assert_reparse(sql: &str, stmt: StatementWithFormat) {
    let stmt = reset_ast(stmt);

    let new_sql = stmt.to_string();
    let new_tokens = crate::parser::tokenize_sql(&new_sql).unwrap();
    let new_stmt = run_parser(
        &new_tokens,
        Dialect::PostgreSQL,
        ParseMode::Default,
        false,
        statement,
    )
    .map_err(|err| panic!("{} in {}", err.1, new_sql))
    .unwrap();

    let new_stmt = reset_ast(new_stmt);
    assert_eq!(stmt, new_stmt, "\nleft:\n{}\nright:\n{}", sql, new_sql);
}

#[allow(dead_code)]
fn reset_ast(mut stmt: StatementWithFormat) -> StatementWithFormat {
    #[derive(VisitorMut)]
    #[visitor(Range(enter), Literal(enter), ExplainKind(enter), SelectTarget(enter))]
    struct ResetAST;

    impl ResetAST {
        fn enter_range(&mut self, range: &mut Range) {
            range.start = 0;
            range.end = 0;
        }

        fn enter_literal(&mut self, literal: &mut Literal) {
            *literal = Literal::Null;
        }

        fn enter_explain_kind(&mut self, kind: &mut ExplainKind) {
            match kind {
                ExplainKind::Ast(_) => *kind = ExplainKind::Ast("".to_string()),
                ExplainKind::Syntax(_) => *kind = ExplainKind::Syntax("".to_string()),
                ExplainKind::Memo(_) => *kind = ExplainKind::Memo("".to_string()),
                _ => (),
            }
        }

        fn enter_select_target(&mut self, target: &mut SelectTarget) {
            if let SelectTarget::StarColumns { column_filter, .. } = target {
                *column_filter = None
            }
        }
    }

    stmt.drive_mut(&mut ResetAST);

    stmt
}
