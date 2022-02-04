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

use nom::combinator::map;

use crate::parser::ast::Statement;
use crate::parser::rule::util::ident;
use crate::parser::rule::util::IResult;
use crate::parser::rule::util::Input;
use crate::parser::token::*;
use crate::rule;

pub fn statement(i: Input) -> IResult<Statement> {
    let truncate_table = map(
        rule! {
            TRUNCATE ~ TABLE ~ ( #ident ~ "." )? ~ #ident ~ ";"
        },
        |(_, _, database, table, _)| Statement::TruncateTable {
            database: database.map(|(name, _)| name),
            table,
        },
    );
    let drop_table = map(
        rule! {
            DROP ~ TABLE ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident ~ ";"
        },
        |(_, _, if_exists, database, table, _)| Statement::DropTable {
            if_exists: if_exists.is_some(),
            database: database.map(|(name, _)| name),
            table,
        },
    );

    rule!(
        #truncate_table : "TRUNCATE TABLE statement"
        | #drop_table : "DROP TABLE statement"
    )(i)
}
