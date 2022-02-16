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
use sqlparser::parser::IsOptional;
use sqlparser::parser::ParserError;

use crate::sql::statements::DfCopy;
use crate::sql::DfParser;
use crate::sql::DfStatement;

impl<'a> DfParser<'a> {
    // copy into mycsvtable
    // from @my_ext_stage/tutorials/dataloading/contacts1.csv format CSV [options];
    pub(crate) fn parse_copy(&mut self) -> Result<DfStatement, ParserError> {
        self.parser.expect_keyword(Keyword::INTO)?;
        let name = self.parser.parse_object_name()?;
        let columns = self
            .parser
            .parse_parenthesized_column_list(IsOptional::Optional)?;
        self.parser.expect_keyword(Keyword::FROM)?;
        let location = self.parser.parse_literal_string()?;

        self.parser.expect_keyword(Keyword::FORMAT)?;
        let format = self.parser.next_token().to_string();

        let options = self.parse_options()?;

        Ok(DfStatement::Copy(DfCopy {
            name,
            columns,
            location,
            format,
            options,
        }))
    }
}
