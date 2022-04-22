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
pub mod util;

use common_exception::ErrorCode;
use common_exception::Result;
use nom::combinator::map;

use crate::ast::Statement;
use crate::parser::error::pretty_print_error;
use crate::parser::statement::statement;
use crate::parser::token::TokenKind;
use crate::parser::token::Tokenizer;
use crate::rule;

/// Parse a SQL string into `Statement`s.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let tokens = Tokenizer::new(sql).collect::<Result<Vec<_>>>()?;
    let stmt = map(rule! { #statement ~ ";" }, |(stmt, _)| stmt);
    let mut stmts = rule! { #stmt+ };

    match stmts(tokens.as_slice()) {
        Ok((rest, stmts)) if rest[0].kind == TokenKind::EOI => Ok(stmts),
        Ok((rest, _)) => Err(ErrorCode::SyntaxException(pretty_print_error(sql, vec![(
            rest[0].span.clone(),
            "unable to parse rest of the sql".to_owned(),
        )]))),
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => Err(ErrorCode::SyntaxException(
            pretty_print_error(sql, err.to_labels()),
        )),
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}
