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

use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOptionDef;
use sqlparser::ast::TableConstraint;
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Word;

use crate::parser_err;
use crate::sql::statements::DfCreateView;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // Create view.
    // syntax reference to https://clickhouse.com/docs/zh/sql-reference/statements/create/view/
    pub(crate) fn parse_create_view(&mut self) -> Result<DfStatement, ParserError> {
        // let materialized = self.parse_keyword(Keyword::MATERIALIZED);
        let if_not_exists =
        self.parser
            .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_object_name()?;
        
        if self.consume_token("AS") {
            // let query = Box::new(self.parse_query()?);
            // TODO(veeupup) we should get subquery str
            let create = DfCreateView {
                if_not_exists,
                name,
                subquery: "subquery".to_string()
            };
            Ok(DfStatement::CreateView(create))
        }else {
            parser_err!("need `AS` after VIEW NAME")
        }
    }
}
