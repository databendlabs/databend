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

pub mod error;
pub mod expr;
pub mod query;
pub mod statement;
pub mod token;
pub mod unescape;
pub mod util;

use common_exception::ErrorCode;
use common_exception::Result;

use self::error::DisplayError;
use crate::ast::Expr;
use crate::ast::Statement;
use crate::parser::error::Backtrace;
use crate::parser::statement::statements;
use crate::parser::token::Token;
use crate::parser::token::TokenKind;
use crate::parser::token::Tokenizer;
use crate::parser::util::Input;

pub fn tokenize_sql(sql: &str) -> Result<Vec<Token>> {
    Tokenizer::new(sql).collect::<Result<Vec<_>>>()
}

/// Parse a SQL string into `Statement`s.
pub fn parse_sql<'a>(
    sql_tokens: &'a [Token<'a>],
    backtrace: &'a Backtrace<'a>,
) -> Result<Vec<Statement<'a>>> {
    match statements(Input(sql_tokens, backtrace)) {
        Ok((rest, stmts)) if rest[0].kind == TokenKind::EOI => Ok(stmts),
        Ok((rest, _)) => Err(ErrorCode::SyntaxException(
            rest[0].display_error("unable to parse rest of the sql".to_string()),
        )),
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            Err(ErrorCode::SyntaxException(err.display_error(())))
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}

/// Parse udf function into Expr
pub fn parse_udf<'a>(
    sql_tokens: &'a [Token<'a>],
    backtrace: &'a Backtrace<'a>,
) -> Result<Expr<'a>> {
    match expr::expr(Input(sql_tokens, backtrace)) {
        Ok((rest, expr)) if rest[0].kind == TokenKind::EOI => Ok(expr),
        Ok((rest, _)) => Err(ErrorCode::SyntaxException(
            rest[0].display_error("unable to parse rest of the sql".to_string()),
        )),
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            Err(ErrorCode::SyntaxException(err.display_error(())))
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}
