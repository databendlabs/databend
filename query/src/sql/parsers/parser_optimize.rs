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

use common_planners::Optimization;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;

use crate::sql::statements::DfOptimizeTable;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    pub(crate) fn parse_optimize(&mut self) -> Result<DfStatement, ParserError> {
        // syntax: "optimize TABLE t [purge | compact | all]",  default action is "purge"
        self.expect_token("OPTIMIZE")?;
        self.parser.expect_keyword(Keyword::TABLE)?;
        let object_name = self.parser.parse_object_name()?;
        let operation = match self.parser.next_token() {
            Token::EOF => Ok(Optimization::PURGE),
            Token::Word(w) => match w.keyword {
                Keyword::ALL => Ok(Optimization::ALL),
                Keyword::PURGE => Ok(Optimization::PURGE),
                Keyword::NoKeyword if w.value.to_uppercase().as_str() == "COMPACT" => {
                    Ok(Optimization::COMPACT)
                }
                _ => self.expected("one of PURGE, COMPACT, ALL", Token::Word(w)),
            },
            t => self.expected("Nothing, or one of PURGE, COMPACT, ALL", t),
        }?;

        Ok(DfStatement::OptimizeTable(DfOptimizeTable {
            name: object_name,
            operation,
        }))
    }
}
