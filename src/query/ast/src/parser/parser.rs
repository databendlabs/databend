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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::input::ParseMode;
use super::statement::insert_stmt;
use super::statement::replace_stmt;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Statement;
use crate::parser::common::comma_separated_list0;
use crate::parser::common::comma_separated_list1;
use crate::parser::common::ident;
use crate::parser::common::transform_span;
use crate::parser::common::IResult;
use crate::parser::error::display_parser_error;
use crate::parser::expr::expr;
use crate::parser::expr::values_with_placeholder;
use crate::parser::input::Dialect;
use crate::parser::input::Input;
use crate::parser::statement::statement;
use crate::parser::token::Token;
use crate::parser::token::TokenKind;
use crate::parser::token::Tokenizer;
use crate::parser::Backtrace;

pub fn tokenize_sql(sql: &str) -> Result<Vec<Token>> {
    Tokenizer::new(sql).collect::<Result<Vec<_>>>()
}

/// Parse a SQL string into `Statement`s.
#[minitrace::trace]
pub fn parse_sql(tokens: &[Token], dialect: Dialect) -> Result<(Statement, Option<String>)> {
    let stmt = run_parser(tokens, dialect, ParseMode::Default, false, statement)?;

    #[cfg(debug_assertions)]
    {
        // Check that the statement can be displayed and reparsed without loss
        let res: Result<(), ErrorCode> = try {
            let reparse_sql = stmt.stmt.to_string();
            let reparse_tokens = crate::parser::tokenize_sql(&reparse_sql)?;
            let reparsed = run_parser(
                &reparse_tokens,
                Dialect::PostgreSQL,
                ParseMode::Default,
                false,
                statement,
            )?;
            let reparsed_sql = reparsed.stmt.to_string();
            assert_eq!(reparse_sql, reparsed_sql, "AST:\n{:#?}", stmt.stmt);
        };
        res.unwrap_or_else(|e| {
            let original_sql = tokens[0].source.to_string();
            panic!(
                "Failed to reparse SQL:\n{}\nAST:\n{:#?}\n{}",
                original_sql, stmt.stmt, e
            );
        });
    }

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

pub fn parse_values_with_placeholder(
    tokens: &[Token],
    dialect: Dialect,
) -> Result<Vec<Option<Expr>>> {
    run_parser(
        tokens,
        dialect,
        ParseMode::Default,
        false,
        values_with_placeholder,
    )
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
                Err(
                    ErrorCode::SyntaxException("unable to parse rest of the sql".to_string())
                        .set_span(transform_span(&rest[..1])),
                )
            }
        }
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            let source = tokens[0].source;
            Err(ErrorCode::SyntaxException(display_parser_error(
                err, source,
            )))
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}
