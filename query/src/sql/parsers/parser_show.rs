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

use crate::sql::statements::DfDescribeTable;
use crate::sql::statements::DfShowDatabases;
use crate::sql::statements::DfShowFunctions;
use crate::sql::statements::DfShowKind;
use crate::sql::statements::DfShowStages;
use crate::sql::statements::DfShowTabStat;
use crate::sql::statements::DfShowTables;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // parse show tables.
    pub(crate) fn parse_show_tables(&mut self, full: bool) -> Result<DfStatement<'a>, ParserError> {
        let mut fromdb = None;
        let history = self.consume_token("HISTORY");

        if self.consume_token("FROM") | self.consume_token("IN") {
            fromdb = Some(self.parser.parse_object_name()?.0[0].value.clone());
        }
        let kind = self.parse_show_kind()?;
        Ok(DfStatement::ShowTables(DfShowTables::create(
            kind, full, fromdb, history,
        )))
    }

    //parse show table status
    pub(crate) fn parse_show_tab_stat(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let mut fromdb = None;
        if self.consume_token("FROM") | self.consume_token("IN") {
            fromdb = Some(self.parser.parse_object_name()?.0[0].value.clone());
        }
        let kind = self.parse_show_kind()?;
        Ok(DfStatement::ShowTabStat(DfShowTabStat::create(
            kind, fromdb,
        )))
    }

    // parse show databases where database = xxx or where database
    pub(crate) fn parse_show_databases(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let kind = self.parse_show_kind()?;
        Ok(DfStatement::ShowDatabases(DfShowDatabases::create(kind)))
    }

    // parse show functions statement
    pub(crate) fn parse_show_functions(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let kind = self.parse_show_kind()?;
        Ok(DfStatement::ShowFunctions(DfShowFunctions::create(kind)))
    }

    pub(crate) fn parse_show_stages(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let kind = self.parse_show_kind()?;
        Ok(DfStatement::ShowStages(DfShowStages::create(kind)))
    }

    pub(crate) fn parse_show_kind(&mut self) -> Result<DfShowKind, ParserError> {
        let tok = self.parser.next_token();
        match &tok {
            Token::EOF | Token::SemiColon => Ok(DfShowKind::All),
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => Ok(DfShowKind::Like(self.parser.parse_identifier()?)),
                Keyword::WHERE => Ok(DfShowKind::Where(self.parser.parse_expr()?)),
                _ => self.expected("like or where", tok),
            },
            _ => self.expected("like or where", tok),
        }
    }

    // parse `show fields from` statement
    // Convert it to the `desc <table>`
    pub(crate) fn parse_show_fields(&mut self) -> Result<DfStatement<'a>, ParserError> {
        if !self.consume_token("FROM") {
            self.expect_token("from")?;
        }

        let table_name = self.parser.parse_object_name()?;
        let desc = DfDescribeTable { name: table_name };
        Ok(DfStatement::DescribeTable(desc))
    }
}
