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

use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    pub(crate) fn parse_presign(&mut self) -> Result<DfStatement<'a>, ParserError> {
        // Consume all remaining tokens;
        loop {
            if let Token::EOF = self.parser.next_token() {
                break;
            }
        }

        // Presign is a placeholder, we will forward to the new planner
        Ok(DfStatement::Presign)
    }
}
