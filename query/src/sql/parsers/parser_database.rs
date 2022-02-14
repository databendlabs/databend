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

use std::collections::HashMap;

use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::sql::statements::DfCreateDatabase;
use crate::sql::statements::DfDropDatabase;
use crate::sql::statements::DfShowCreateDatabase;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // Create database.
    pub(crate) fn parse_create_database(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_object_name()?;
        let (engine, engine_options) = self.parse_database_engine()?;

        let create = DfCreateDatabase {
            if_not_exists,
            name,
            engine,
            engine_options,
            options: HashMap::new(),
        };

        Ok(DfStatement::CreateDatabase(create))
    }

    // Drop database.
    pub(crate) fn parse_drop_database(&mut self) -> Result<DfStatement, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let db_name = self.parser.parse_object_name()?;

        let drop = DfDropDatabase {
            if_exists,
            name: db_name,
        };

        Ok(DfStatement::DropDatabase(drop))
    }

    // Show create database.
    pub(crate) fn parse_show_create_database(&mut self) -> Result<DfStatement, ParserError> {
        let db_name = self.parser.parse_object_name()?;
        let show_create_database = DfShowCreateDatabase { name: db_name };
        Ok(DfStatement::ShowCreateDatabase(show_create_database))
    }

    fn parse_database_engine(&mut self) -> Result<(String, HashMap<String, String>), ParserError> {
        // TODO make ENGINE as a keyword
        if !self.consume_token("ENGINE") {
            return Ok(("".to_string(), HashMap::new()));
        }

        self.parser.expect_token(&Token::Eq)?;
        let engine = self.parser.next_token().to_string();
        let options = if self.parser.consume_token(&Token::LParen) {
            let options = self.parse_options()?;
            self.parser.expect_token(&Token::RParen)?;
            options
        } else {
            HashMap::new()
        };
        Ok((engine, options))
    }
}
