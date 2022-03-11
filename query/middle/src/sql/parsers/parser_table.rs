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

use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOptionDef;
use sqlparser::ast::TableConstraint;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Word;

use crate::parser_err;
use crate::sql::statements::DfCreateTable;
use crate::sql::statements::DfDescribeTable;
use crate::sql::statements::DfDropTable;
use crate::sql::statements::DfQueryStatement;
use crate::sql::statements::DfShowCreateTable;
use crate::sql::statements::DfTruncateTable;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // Create table.
    pub(crate) fn parse_create_table(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;

        // Parse the table which we copy schema from. This is for create table like statement.
        // https://dev.mysql.com/doc/refman/8.0/en/create-table-like.html
        let mut table_like = None;
        if self.parser.parse_keyword(Keyword::LIKE) {
            table_like = Some(self.parser.parse_object_name()?);
        }

        let (columns, _) = self.parse_columns()?;
        if !columns.is_empty() && table_like.is_some() {
            return parser_err!("mix create table like statement and column definition.");
        }

        let engine = self.parse_table_engine()?;

        // parse table options: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
        let options = self.parse_options()?;

        let mut query = None;
        if let Token::Word(Word { keyword, .. }) = self.parser.peek_token() {
            let mut has_query = false;
            if keyword == Keyword::AS {
                self.parser.next_token();
                has_query = true;
            }
            if has_query || keyword == Keyword::SELECT {
                let native = self.parser.parse_query()?;
                query = Some(Box::new(DfQueryStatement::try_from(native)?))
            }
        }

        let create = DfCreateTable {
            if_not_exists,
            name: table_name,
            columns,
            engine,
            options,
            like: table_like,
            query,
        };

        Ok(DfStatement::CreateTable(create))
    }

    // Drop table.
    pub(crate) fn parse_drop_table(&mut self) -> Result<DfStatement, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;

        let drop = DfDropTable {
            if_exists,
            name: table_name,
        };

        Ok(DfStatement::DropTable(drop))
    }

    // Truncate table.
    pub(crate) fn parse_truncate_table(&mut self) -> Result<DfStatement, ParserError> {
        let table_name = self.parser.parse_object_name()?;
        let purge = self.parser.parse_keyword(Keyword::PURGE);
        let statement = DfTruncateTable {
            name: table_name,
            purge,
        };
        Ok(DfStatement::TruncateTable(statement))
    }

    // Show create table.
    pub(crate) fn parse_show_create_table(&mut self) -> Result<DfStatement, ParserError> {
        let table_name = self.parser.parse_object_name()?;
        let show_create_table = DfShowCreateTable { name: table_name };
        Ok(DfStatement::ShowCreateTable(show_create_table))
    }

    // Desc table.
    pub(crate) fn parse_desc_table(&mut self) -> Result<DfStatement, ParserError> {
        let table_name = self.parser.parse_object_name()?;
        let desc = DfDescribeTable { name: table_name };
        Ok(DfStatement::DescribeTable(desc))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parser.parse_identifier()?;
        let data_type = self.parser.parse_data_type()?;
        let collation = if self.parser.parse_keyword(Keyword::COLLATE) {
            Some(self.parser.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parser.parse_identifier()?);
                if let Some(option) = self.parser.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.parser.peek_token(),
                    );
                }
            } else if let Some(option) = self.parser.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    // This is a copy of the equivalent implementation in sqlparser.
    fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen) || self.parser.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parser.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token() {
                let column_def = self.parse_column_def()?;
                columns.push(column_def);
            } else {
                return self.expected(
                    "column name or constraint definition",
                    self.parser.peek_token(),
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    /// Parses the set of valid formats
    fn parse_table_engine(&mut self) -> Result<String, ParserError> {
        // TODO make ENGINE as a keyword
        if !self.consume_token("ENGINE") {
            return Ok("FUSE".to_string());
        }

        self.parser.expect_token(&Token::Eq)?;
        Ok(self.parser.next_token().to_string())
    }
}
