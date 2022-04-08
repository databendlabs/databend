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

use crate::parser_err;
use crate::sql::statements::DfAlterView;
use crate::sql::statements::DfCreateView;
use crate::sql::statements::DfDropView;
use crate::sql::statements::DfQueryStatement;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // Create view.
    // syntax reference to https://clickhouse.com/docs/zh/sql-reference/statements/create/view/
    pub(crate) fn parse_create_view(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let name = self.parser.parse_object_name()?;

        if self.consume_token("AS") {
            let native_query = self.parser.parse_query()?;
            let query = DfQueryStatement::try_from(native_query.clone())?;
            let subquery = format!("{}", native_query);
            let create = DfCreateView {
                if_not_exists,
                name,
                subquery,
                query,
            };
            Ok(DfStatement::CreateView(create))
        } else {
            parser_err!("need `AS` after VIEW NAME")
        }
    }

    pub(crate) fn parse_drop_view(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let if_exists = self.parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name()?;

        let drop = DfDropView {
            if_exists,
            name: table_name,
        };

        Ok(DfStatement::DropView(drop))
    }

    pub(crate) fn parse_alter_view(&mut self) -> Result<DfStatement<'a>, ParserError> {
        let name = self.parser.parse_object_name()?;
        if self.consume_token("AS") {
            let native_query = self.parser.parse_query()?;
            let query = DfQueryStatement::try_from(native_query.clone())?;
            let subquery = format!("{}", native_query);
            let alter = DfAlterView {
                name,
                subquery,
                query,
            };
            Ok(DfStatement::AlterView(alter))
        } else {
            parser_err!("need `AS` after VIEW NAME")
        }
    }
}
