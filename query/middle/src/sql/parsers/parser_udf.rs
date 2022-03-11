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
use sqlparser::tokenizer::Word;

use crate::parser_err;
use crate::sql::statements::DfAlterUDF;
use crate::sql::statements::DfCreateUDF;
use crate::sql::statements::DfDropUDF;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    fn parse_udf_parameters(&mut self) -> Result<Vec<String>, ParserError> {
        let mut params = vec![];
        let mut found_right_paren = false;
        let mut expect_next_param = false;

        self.parser.expect_token(&Token::LParen)?;
        loop {
            let next_token = self.parser.next_token();
            if next_token == Token::EOF || next_token == Token::SemiColon {
                break;
            }
            if next_token == Token::RParen {
                found_right_paren = true;
                break;
            }

            match next_token {
                Token::Word(Word {
                    value,
                    quote_style,
                    keyword,
                }) => {
                    if keyword != Keyword::NoKeyword {
                        return parser_err!(format!(
                            "Keyword can not be parameter, got: {}",
                            value
                        ));
                    }
                    if let Some(quote) = quote_style {
                        return parser_err!(format!(
                            "Quote is not allowed in parameters, remove: {}",
                            quote
                        ));
                    }
                    if params.contains(&value) {
                        return parser_err!(format!(
                            "Duplicate parameter is not allowed, keep only one: {}",
                            &value
                        ));
                    }

                    expect_next_param = false;
                    params.push(value);
                }
                Token::Comma => {
                    expect_next_param = true;
                }
                other => {
                    return parser_err!(format!("Expect words or comma, but got: {}", other));
                }
            }
        }

        if !found_right_paren {
            return parser_err!("Can not find complete parameters, `)` is missing");
        }
        if expect_next_param {
            return parser_err!("Found a redundant `,` in the parameters");
        }

        Ok(params)
    }

    fn parse_udf_definition_expr(&mut self, until_token: Vec<&str>) -> Result<String, ParserError> {
        // Match ->
        self.parser.expect_token(&Token::Minus)?;
        let next_token = self.parser.next_token_no_skip();
        if next_token != Some(&Token::Gt) {
            return parser_err!(format!("Expected >, found: {:#?}", next_token));
        }

        let definition = self.consume_token_until_or_end(until_token).join("");
        if definition.is_empty() {
            return parser_err!("UDF definition can not be empty");
        }

        Ok(definition)
    }

    pub(crate) fn parse_create_udf(&mut self) -> Result<DfStatement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let udf_name = self.parser.parse_literal_string()?;
        self.parser.expect_keyword(Keyword::AS)?;

        let desc_token = "DESC";
        let parameters = self.parse_udf_parameters()?;
        let definition = self.parse_udf_definition_expr(vec![desc_token])?;

        let description = self.parse_udf_desc(desc_token)?;
        let create_udf = DfCreateUDF {
            if_not_exists,
            udf_name,
            parameters,
            definition,
            description,
        };

        Ok(DfStatement::CreateUDF(create_udf))
    }

    pub(crate) fn parse_alter_udf(&mut self) -> Result<DfStatement, ParserError> {
        let udf_name = self.parser.parse_literal_string()?;
        let as_token = Token::make_keyword("AS");
        self.parser.expect_token(&as_token)?;

        let desc_token = "DESC";
        let parameters = self.parse_udf_parameters()?;
        let definition = self.parse_udf_definition_expr(vec![desc_token])?;

        let description = self.parse_udf_desc(desc_token)?;
        let update_udf = DfAlterUDF {
            udf_name,
            parameters,
            definition,
            description,
        };

        Ok(DfStatement::AlterUDF(update_udf))
    }

    pub(crate) fn parse_drop_udf(&mut self) -> Result<DfStatement, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let udf_name = self.parser.parse_literal_string()?;

        let drop_udf = DfDropUDF {
            if_exists,
            udf_name,
        };
        Ok(DfStatement::DropUDF(drop_udf))
    }

    fn parse_udf_desc(&mut self, desc_token: &str) -> Result<String, ParserError> {
        if self.consume_token(desc_token) {
            self.parser.expect_token(&Token::Eq)?;
            Ok(self.parser.parse_literal_string()?)
        } else {
            Ok(String::from(""))
        }
    }
}
