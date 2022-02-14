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

use crate::sql::statements::DfUseTenant;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // Parse 'sudo ...'.
    pub(crate) fn parse_admin_command(&mut self) -> Result<DfStatement, ParserError> {
        self.parser.next_token();
        match self.consume_token("USE") {
            true if self.consume_token("TENANT") => self.parse_use_tenant(),
            _ => self.expected("Unsupported sudo command", self.parser.peek_token()),
        }
    }

    // Parse 'sudo use tenant [tenant id]'.
    fn parse_use_tenant(&mut self) -> Result<DfStatement, ParserError> {
        let name = self.parser.parse_object_name()?;
        Ok(DfStatement::UseTenant(DfUseTenant { name }))
    }
}
