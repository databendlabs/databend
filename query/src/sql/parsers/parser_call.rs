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

use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::sql::statements::DfCall;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    pub(crate) fn parse_call(&mut self) -> Result<DfStatement, ParserError> {
        let name = self.parser.parse_literal_string()?;
        self.parser.expect_token(&Token::LParen)?;
        let mut args = vec![];
        if !self.parser.consume_token(&Token::RParen) {
            args = self.parse_list(&Token::Comma)?;
            self.parser.expect_token(&Token::RParen)?;
        }
        Ok(DfStatement::Call(DfCall { name, args }))
    }
}
