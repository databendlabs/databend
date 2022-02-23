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

use sqlparser::ast::Statement;
use sqlparser::parser::ParserError;

use crate::sql::statements::DfDeleteStatement;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    pub(crate) fn parse_delete(&mut self) -> Result<DfStatement, ParserError> {
        self.parser.next_token();
        let native_query = self.parser.parse_delete()?;

        if let Statement::Delete {
            table_name,
            selection,
        } = native_query
        {
            let stmt = DfDeleteStatement {
                from: table_name,
                selection,
            };
            Ok(DfStatement::Delete(Box::new(stmt)))
        } else {
            Err(ParserError::ParserError(format!(
                "expecting delete stmt, but got {}",
                native_query
            )))
        }
    }
}
