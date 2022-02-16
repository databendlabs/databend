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
//
// Borrow from apache/arrow/rust/datafusion/src/sql/sql_parser
// See notice.md

use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::sql::statements::DfShowDatabases;
use crate::sql::statements::DfShowFunctions;
use crate::sql::statements::DfShowKind;
use crate::sql::statements::DfShowTables;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // parse show tables.
    pub(crate) fn parse_show_tables(&mut self) -> Result<DfStatement, ParserError> {
        let tok = self.parser.next_token();
        match &tok {
            Token::EOF | Token::SemiColon => Ok(DfStatement::ShowTables(DfShowTables::create(
                DfShowKind::All,
            ))),
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => Ok(DfStatement::ShowTables(DfShowTables::create(
                    DfShowKind::Like(self.parser.parse_identifier()?),
                ))),
                Keyword::WHERE => Ok(DfStatement::ShowTables(DfShowTables::create(
                    DfShowKind::Where(self.parser.parse_expr()?),
                ))),
                Keyword::FROM | Keyword::IN => Ok(DfStatement::ShowTables(DfShowTables::create(
                    DfShowKind::FromOrIn(self.parser.parse_object_name()?),
                ))),
                _ => self.expected("like or where", tok),
            },
            _ => self.expected("like or where", tok),
        }
    }

    // parse show databases where database = xxx or where database
    pub(crate) fn parse_show_databases(&mut self) -> Result<DfStatement, ParserError> {
        let tok = self.parser.next_token();
        match &tok {
            Token::EOF | Token::SemiColon => Ok(DfStatement::ShowDatabases(
                DfShowDatabases::create(DfShowKind::All),
            )),
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => Ok(DfStatement::ShowDatabases(DfShowDatabases::create(
                    DfShowKind::Like(self.parser.parse_identifier()?),
                ))),
                Keyword::WHERE => Ok(DfStatement::ShowDatabases(DfShowDatabases::create(
                    DfShowKind::Where(self.parser.parse_expr()?),
                ))),
                Keyword::FROM | Keyword::IN => Ok(DfStatement::ShowDatabases(
                    DfShowDatabases::create(DfShowKind::FromOrIn(self.parser.parse_object_name()?)),
                )),
                _ => self.expected("like or where", tok),
            },
            _ => self.expected("like or where", tok),
        }
    }

    // parse show functions statement
    pub(crate) fn parse_show_functions(&mut self) -> Result<DfStatement, ParserError> {
        let tok = self.parser.next_token();
        match &tok {
            Token::EOF | Token::SemiColon => Ok(DfStatement::ShowFunctions(
                DfShowFunctions::create(DfShowKind::All),
            )),
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => Ok(DfStatement::ShowFunctions(DfShowFunctions::create(
                    DfShowKind::Like(self.parser.parse_identifier()?),
                ))),
                Keyword::WHERE => Ok(DfStatement::ShowFunctions(DfShowFunctions::create(
                    DfShowKind::Where(self.parser.parse_expr()?),
                ))),
                _ => self.expected("like or where", tok),
            },
            _ => self.expected("like or where", tok),
        }
    }
}
