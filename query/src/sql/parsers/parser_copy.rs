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

use std::collections::BTreeMap;

use sqlparser::ast::ObjectName;
use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::IsOptional;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::sql::statements::DfCopy;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // copy into table from [?] ...
    pub(crate) fn parse_copy(&mut self) -> Result<DfStatement<'a>, ParserError> {
        self.parser.expect_keyword(Keyword::INTO)?;
        let mut location = match self.parser.next_token() {
            Token::AtString(s) => {
                format!("@{}", s)
            }
            _ => {
                self.parser.prev_token();
                "".to_string()
            }
        };

        let name;
        let mut query = None;
        let mut columns = vec![];
        if location.starts_with("@") {
            self.parser.expect_keyword(Keyword::FROM)?;
            if self.parser.consume_token(&Token::LParen) {
                query = Some(self.parser.parse_query()?);
                name = ObjectName(vec![]);
                self.parser.expect_token(&Token::RParen)?;
            } else {
                name = self.parser.parse_object_name()?;
                let subquery = format!("SELECT * FROM {}", name);
                let mut inner_parser = DfParser::new_with_dialect(&subquery, &GenericDialect {})?;
                query = Some(inner_parser.parser.parse_query()?);
            }
        } else {
            name = self.parser.parse_object_name()?;
            columns = self
                .parser
                .parse_parenthesized_column_list(IsOptional::Optional)?;

            // from 's3://mybucket/data/files'
            self.parser.expect_keyword(Keyword::FROM)?;
            location = self.parser.parse_literal_string()?;
        }

        // credentials=(aws_key_id='$AWS_ACCESS_KEY_ID' aws_secret_key='$AWS_SECRET_ACCESS_KEY')
        let mut credential_options = BTreeMap::default();
        if self.consume_token("CREDENTIALS") {
            self.expect_token("=")?;
            self.expect_token("(")?;
            credential_options = self.parse_options()?;
            self.expect_token(")")?;
        }

        // encryption=(master_key = '$MASER_KEY')
        let mut encryption_options = BTreeMap::default();
        if self.consume_token("ENCRYPTION") {
            self.expect_token("=")?;
            self.expect_token("(")?;
            encryption_options = self.parse_options()?;
            self.expect_token(")")?;
        }

        // FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] )
        let mut files: Vec<String> = vec![];
        if self.consume_token("FILES") {
            self.expect_token("=")?;
            self.expect_token("(")?;
            files = self.parse_list(&Token::Comma)?;
            self.expect_token(")")?;
        }

        // PATTERN = '<regex_pattern>'
        let mut pattern = "".to_string();
        if self.consume_token("PATTERN") {
            self.expect_token("=")?;
            pattern = self.parse_value_or_ident()?;
        }

        // file_format = (type = csv field_delimiter = '|' skip_header = 1)
        let mut file_format_options = BTreeMap::default();
        if self.consume_token("FILE_FORMAT") {
            self.expect_token("=")?;
            self.expect_token("(")?;
            file_format_options = self.parse_options()?;
            self.expect_token(")")?;
        }

        /*
         copyOptions ::=
         ON_ERROR = { CONTINUE | SKIP_FILE | SKIP_FILE_<num> | SKIP_FILE_<num>% | ABORT_STATEMENT }
         SIZE_LIMIT = <num>
        */
        let mut on_error = "".to_string();
        if self.consume_token("ON_ERROR") {
            self.expect_token("=")?;
            on_error = self.parse_value_or_ident()?;
        }

        let mut size_limit = "".to_string();
        if self.consume_token("SIZE_LIMIT") {
            self.expect_token("=")?;
            size_limit = self.parse_value_or_ident()?;
        }

        // VALIDATION_MODE = RETURN_<n>_ROWS | RETURN_ERRORS | RETURN_ALL_ERRORS
        let mut validation_mode = "".to_string();
        if self.consume_token("VALIDATION_MODE") {
            self.expect_token("=")?;
            validation_mode = self.parse_value_or_ident()?;
        }

        Ok(DfStatement::Copy(DfCopy {
            name,
            columns,
            location,
            credential_options,
            encryption_options,
            file_format_options,
            files,
            pattern,
            on_error,
            size_limit,
            validation_mode,
            query,
        }))
    }
}
