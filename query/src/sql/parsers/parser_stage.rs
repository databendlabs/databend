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

use crate::sql::statements::DfCreateUserStage;
use crate::sql::statements::DfDescribeUserStage;
use crate::sql::statements::DfDropUserStage;
use crate::sql::statements::DfList;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    pub(crate) fn parse_create_stage(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_literal_string()?;

        let mut credential_options = HashMap::default();
        let mut encryption_options = HashMap::default();

        // Is External
        let mut location = "".to_string();
        if self.consume_token("URL") {
            self.expect_token("=")?;
            location = self.parser.parse_literal_string()?;

            // credentials=(aws_key_id='$AWS_ACCESS_KEY_ID' aws_secret_key='$AWS_SECRET_ACCESS_KEY')
            if self.consume_token("CREDENTIALS") {
                self.expect_token("=")?;
                self.expect_token("(")?;
                credential_options = self.parse_options()?;
                self.expect_token(")")?;
            }

            // encryption=(master_key = '$MASER_KEY')
            if self.consume_token("ENCRYPTION") {
                self.expect_token("=")?;
                self.expect_token("(")?;
                encryption_options = self.parse_options()?;
                self.expect_token(")")?;
            }
        }

        // file_format = (type = csv field_delimiter = '|' skip_header = 1)
        let mut file_format_options = HashMap::default();
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

        let comments = if self.consume_token("COMMENTS") {
            self.parser.expect_token(&Token::Eq)?;
            self.parser.parse_literal_string()?
        } else {
            String::from("")
        };

        let create = DfCreateUserStage {
            if_not_exists,
            stage_name: name,
            location,
            credential_options,
            encryption_options,
            on_error,
            size_limit,
            validation_mode,
            comments,
            file_format_options,
        };

        Ok(DfStatement::CreateStage(create))
    }

    pub(crate) fn parse_drop_stage(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parser.parse_literal_string()?;

        let drop_stage = DfDropUserStage { if_exists, name };
        Ok(DfStatement::DropStage(drop_stage))
    }

    // Desc stage.
    pub(crate) fn parse_desc_stage(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let table_name = self.parser.parse_object_name()?;
        let desc = DfDescribeUserStage { name: table_name };
        Ok(DfStatement::DescribeStage(desc))
    }

    // list stage files
    pub(crate) fn parse_list_cmd(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let location = match self.parser.next_token() {
            Token::AtString(s) => Ok(format!("@{}", s)),
            unexpected => self.expected("@string_literal", unexpected),
        }?;

        // PATTERN = '<regex_pattern>'
        let mut pattern = "".to_string();
        if self.consume_token("PATTERN") {
            self.expect_token("=")?;
            pattern = self.parse_value_or_ident()?;
        }
        Ok(DfStatement::List(DfList { location, pattern }))
    }
}
