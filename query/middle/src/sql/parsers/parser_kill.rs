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
//
// Borrow from apache/arrow/rust/datafusion/src/sql/sql_parser
// See notice.md

use sqlparser::parser::ParserError;

use crate::sql::statements::DfKillStatement;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // Parse 'KILL statement'.
    pub(crate) fn parse_kill_query(&mut self) -> Result<DfStatement, ParserError> {
        match self.consume_token("KILL") {
            true if self.consume_token("QUERY") => self.parse_kill::<true>(),
            true if self.consume_token("CONNECTION") => self.parse_kill::<false>(),
            true => self.parse_kill::<false>(),
            false => self.expected("Must KILL", self.parser.peek_token()),
        }
    }

    // Parse 'KILL statement'.
    fn parse_kill<const KILL_QUERY: bool>(&mut self) -> Result<DfStatement, ParserError> {
        Ok(DfStatement::KillStatement(DfKillStatement {
            object_id: self.parser.parse_identifier()?,
            kill_query: KILL_QUERY,
        }))
    }
}
