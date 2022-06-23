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

use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::sql::statements::DfExistsTable;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // Parse 'Exists statement'.
    pub(crate) fn parse_exists(&mut self) -> Result<DfStatement<'a>, ParserError> {
        match self.parser.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TABLE => {
                    let table_name = self.parser.parse_object_name()?;
                    let exists = DfExistsTable { name: table_name };
                    Ok(DfStatement::ExistsTable(exists))
                }
                _ => self.expected("exists table statement", Token::Word(w)),
            },
            unexpected => self.expected("exists table  statement", unexpected),
        }
    }
}
