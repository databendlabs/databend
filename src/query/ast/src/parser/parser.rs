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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::ast::Expr;
use crate::ast::Statement;
use crate::error::display_parser_error;
use crate::input::Dialect;
use crate::input::Input;
use crate::parser::expr;
use crate::parser::expr::subexpr;
use crate::parser::expr::values_with_placeholder;
use crate::parser::statement::statement;
use crate::parser::token::Token;
use crate::parser::token::TokenKind;
use crate::parser::token::Tokenizer;
use crate::util::comma_separated_list0;
use crate::util::transform_span;
use crate::Backtrace;

pub fn tokenize_sql(sql: &str) -> Result<Vec<Token>> {
    Tokenizer::new(sql).collect::<Result<Vec<_>>>()
}

/// Parse a SQL string into `Statement`s.
pub fn parse_sql<'a>(
    sql_tokens: &'a [Token<'a>],
    dialect: Dialect,
) -> Result<(Statement, Option<String>)> {
    let backtrace = Backtrace::new();
    match statement(Input(sql_tokens, dialect, &backtrace)) {
        Ok((rest, stmts)) if rest[0].kind == TokenKind::EOI => Ok((stmts.stmt, stmts.format)),
        Ok((rest, _)) => Err(ErrorCode::SyntaxException(
            "unable to parse rest of the sql".to_string(),
        )
        .set_span(transform_span(&rest[..1]))),
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            let source = sql_tokens[0].source;
            Err(ErrorCode::SyntaxException(display_parser_error(
                err, source,
            )))
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}

/// Parse udf function into Expr
pub fn parse_expr<'a>(sql_tokens: &'a [Token<'a>], dialect: Dialect) -> Result<Expr> {
    let backtrace = Backtrace::new();
    match expr::expr(Input(sql_tokens, dialect, &backtrace)) {
        Ok((rest, expr)) if rest[0].kind == TokenKind::EOI => Ok(expr),
        Ok((rest, _)) => Err(ErrorCode::SyntaxException(
            "unable to parse rest of the sql".to_string(),
        )
        .set_span(transform_span(&rest[..1]))),
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            let source = sql_tokens[0].source;
            Err(ErrorCode::SyntaxException(display_parser_error(
                err, source,
            )))
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}

pub fn parse_comma_separated_exprs<'a>(
    sql_tokens: &'a [Token<'a>],
    dialect: Dialect,
) -> Result<Vec<Expr>> {
    let backtrace = Backtrace::new();
    let mut comma_separated_exprs_parser = comma_separated_list0(subexpr(0));
    match comma_separated_exprs_parser(Input(sql_tokens, dialect, &backtrace)) {
        Ok((_rest, exprs)) => Ok(exprs),
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            let source = sql_tokens[0].source;
            Err(ErrorCode::SyntaxException(display_parser_error(
                err, source,
            )))
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}

pub fn parser_values_with_placeholder<'a>(
    sql_tokens: &'a [Token<'a>],
    dialect: Dialect,
) -> Result<Vec<Option<Expr>>> {
    let backtrace = Backtrace::new();
    match values_with_placeholder(Input(sql_tokens, dialect, &backtrace)) {
        Ok((rest, exprs)) if rest[0].kind == TokenKind::EOI => Ok(exprs),
        Ok((rest, _)) => Err(ErrorCode::SyntaxException(
            "unable to parse rest of the sql".to_string(),
        )
        .set_span(transform_span(&rest[..1]))),
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            let source = sql_tokens[0].source;
            Err(ErrorCode::SyntaxException(display_parser_error(
                err, source,
            )))
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}
