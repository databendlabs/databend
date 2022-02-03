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

use nom::IResult;

use crate::parser::ast::Statement;
use crate::parser::rule::util::ident;
use crate::parser::rule::util::Input;
use crate::parser::rule::util::ParseError;
use crate::parser::token::*;
use crate::rule;

pub fn truncate_table<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, Statement, Error>
where Error: ParseError<Input<'a>> {
    let (i, (_, _, database, table, _)) = rule! {
        TRUNCATE ~ TABLE ~ ( #ident ~ "." )? ~ #ident ~ ";"
    }(i)?;

    Ok((i, Statement::TruncateTable {
        database: database.map(|(name, _)| name),
        table,
    }))
}

pub fn drop_table<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, Statement, Error>
where Error: ParseError<Input<'a>> {
    let (i, (_, _, if_exists, database, table, _)) = rule! {
        DROP ~ TABLE ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident ~ ";"
    }(i)?;

    Ok((i, Statement::DropTable {
        if_exists: if_exists.is_some(),
        database: database.map(|(name, _)| name),
        table,
    }))
}
