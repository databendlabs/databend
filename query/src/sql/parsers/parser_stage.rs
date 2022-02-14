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

use common_io::prelude::OptionsDeserializer;
use common_meta_types::Credentials;
use common_meta_types::FileFormat;
use common_meta_types::StageParams;
use serde::Deserialize;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::parser_err;
use crate::sql::statements::DfCreateStage;
use crate::sql::statements::DfDescribeStage;
use crate::sql::statements::DfDropStage;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // Create stage.
    pub(crate) fn parse_create_stage(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_literal_string()?;
        let url = if self.consume_token("URL") {
            self.parser.expect_token(&Token::Eq)?;
            self.parser.parse_literal_string()?
        } else {
            return parser_err!("Missing URL");
        };

        if !url.to_uppercase().starts_with("S3") {
            return parser_err!("Not supported storage");
        }

        let credentials = self.parse_stage_credentials()?;
        let stage_params = StageParams::new(url.as_str(), credentials);
        let file_format = self.parse_stage_file_format()?;

        let comments = if self.consume_token("COMMENTS") {
            self.parser.expect_token(&Token::Eq)?;
            self.parser.parse_literal_string()?
        } else {
            String::from("")
        };

        let create = DfCreateStage {
            if_not_exists,
            stage_name: name,
            stage_params,
            file_format,
            comments,
        };

        Ok(DfStatement::CreateStage(create))
    }

    // Drop stage.
    pub(crate) fn parse_drop_stage(&mut self) -> Result<DfStatement, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let stage_name = self.parser.parse_literal_string()?;

        let drop = DfDropStage {
            if_exists,
            stage_name,
        };
        Ok(DfStatement::DropStage(drop))
    }

    // Desc stage.
    pub(crate) fn parse_desc_stage(&mut self) -> Result<DfStatement, ParserError> {
        let obj_name = self.parser.parse_object_name()?;
        let desc = DfDescribeStage { name: obj_name };
        Ok(DfStatement::DescribeStage(desc))
    }

    fn parse_stage_credentials(&mut self) -> Result<Credentials, ParserError> {
        let options = if self.consume_token("CREDENTIALS") {
            self.parser.expect_token(&Token::Eq)?;
            self.parser.expect_token(&Token::LParen)?;
            let options = self.parse_options()?;
            self.parser.expect_token(&Token::RParen)?;

            options
        } else {
            HashMap::new()
        };

        let credentials = Credentials::deserialize(OptionsDeserializer::new(&options))
            .map_err(|e| ParserError::ParserError(format!("Invalid credentials options: {}", e)))?;
        Ok(credentials)
    }

    fn parse_stage_file_format(&mut self) -> Result<FileFormat, ParserError> {
        let options = if self.consume_token("FILE_FORMAT") {
            self.parser.expect_token(&Token::Eq)?;
            self.parser.expect_token(&Token::LParen)?;
            let options = self.parse_options()?;

            self.parser.expect_token(&Token::RParen)?;

            options
        } else {
            HashMap::new()
        };
        let file_format = FileFormat::deserialize(OptionsDeserializer::new(&options))
            .map_err(|e| ParserError::ParserError(format!("Invalid file format options: {}", e)))?;
        Ok(file_format)
    }
}
