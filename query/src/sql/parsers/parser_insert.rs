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
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Read;

use common_exception::ErrorCode;
use common_io::prelude::BufReadExt;
use sqlparser::ast::ObjectName;
use sqlparser::ast::SetExpr;
use sqlparser::ast::Statement;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::parser_err;
use crate::sql::statements::DfInsertStatement;
use crate::sql::statements::InsertSource;
use crate::sql::DfParser;
use crate::sql::DfStatement;

/// Since we only support single sql running, here we only do optimizztion
/// for the first insert-values sql.
impl<'a> DfParser<'a> {
    pub(crate) fn parse_insert(&mut self) -> Result<DfStatement<'a>, ParserError> {
        self.parser.next_token();
        let statement = if self.first_statement {
            // `parse_insert_without_values` will not do values to Vec<Vec<Expr>> here.
            // And other insert sql will do the same before. Need to submit pr to our sqlparser repo.
            let statement = self.parser.parse_insert_without_values();
            // skip to next statement
            while self.parser.peek_token() != Token::SemiColon
                && self.parser.peek_token() != Token::EOF
            {
                self.parser.next_token();
            }

            statement
        } else {
            self.parser.parse_insert()
        }?;

        match statement {
            Statement::Insert {
                or,
                table_name,
                columns,
                overwrite,
                source,
                partitioned,
                format,
                after_columns,
                table,
                on,
            } => {
                let insert_source = match source {
                    None => Ok(InsertSource::Empty),
                    Some(source) => match source.body {
                        SetExpr::Values(_) => {
                            self.raw_values_str(&table_name, table)
                                .map(InsertSource::Values)
                                .map_err(|e| ParserError::ParserError(e.message()))
                        }
                        SetExpr::Select(_) => Ok(InsertSource::Select(source)),
                        _ => Err(ParserError::ParserError(
                            "Insert must be have values or select source.".to_string(),
                        )),
                    },
                }?;

                Ok(DfStatement::InsertQuery(DfInsertStatement {
                    or,
                    table_name,
                    columns,
                    overwrite,
                    source: insert_source,
                    partitioned,
                    format,
                    after_columns,
                    table,
                    on,
                }))
            }
            _ => parser_err!("Expect set insert statement"),
        }
    }

    fn raw_values_str(&self, table_name: &ObjectName, table: bool) -> Result<&'a str, ErrorCode> {
        let mut reader = BufReader::new(Cursor::new(self.sql));

        // Insert
        let _ = reader.ignore_spaces()?;
        let expected_bs = b"insert";
        let mut buf = vec![];
        reader.until(b' ', &mut buf)?;
        if buf[0..buf.len() - 1].to_ascii_lowercase() != expected_bs {
            return Err(ErrorCode::BadBytes("bad insert token".to_string()));
        }

        // Into/overwrite
        let _ = reader.ignore_spaces()?;
        let mut buf = vec![];
        reader.until(b' ', &mut buf)?;
        let lowercase_bs = &buf[0..buf.len() - 1].to_ascii_lowercase();
        if lowercase_bs != b"into" && lowercase_bs != b"overwrite" {
            return Err(ErrorCode::BadBytes("Bad into/overwrite token".to_string()));
        }

        // Optional table token(insert into table t values(1))
        if table {
            let _ = reader.ignore_spaces()?;
            reader.consume(5);
        }

        // Table name
        let _ = reader.ignore_spaces()?;
        let table = format!("{}", table_name);
        if !reader.ignore_bytes(table.as_bytes())? {
            return Err(ErrorCode::BadBytes("Bad db.table".to_string()));
        }

        // Column list (a, b, c)
        let _ = reader.ignore_spaces()?;
        if reader.ignore_byte(b'(')? {
            let _ = reader.until(b')', &mut buf)?;
        }

        // Values
        let _ = reader.ignore_spaces()?;
        let values_buf = &mut [0u8; 6];
        reader.read_exact(values_buf).unwrap();
        let lowercase_bs = &values_buf.to_ascii_lowercase();
        if lowercase_bs != b"values" {
            return Err(ErrorCode::BadBytes("Bad values token".to_string()));
        }

        let _ = reader.ignore_spaces()?;
        let buf_len = reader.buffer().len();
        let position = reader.into_inner().position() as usize - buf_len;

        Ok(&self.sql[position..])
    }
}
