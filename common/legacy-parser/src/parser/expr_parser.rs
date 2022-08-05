//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use sqlparser::ast::Expr;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Tokenizer;

pub struct ExprParser;

impl ExprParser {
    pub fn parse_expr(expr: &str) -> Result<Expr, ParserError> {
        let dialect = &MySqlDialect {};
        let mut tokenizer = Tokenizer::new(dialect, expr);
        let (tokens, position_map) = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens, position_map, dialect);
        parser.parse_expr()
    }

    pub fn parse_exprs(expr: &str) -> Result<Vec<Expr>, ParserError> {
        let dialect = &MySqlDialect {};
        let mut tokenizer = Tokenizer::new(dialect, expr);
        let (tokens, position_map) = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens, position_map, dialect);

        parser.expect_token(&Token::LParen)?;
        let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
        parser.expect_token(&Token::RParen)?;

        Ok(exprs)
    }
}
