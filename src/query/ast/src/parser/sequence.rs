// Copyright 2021 Datafuse Labs
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

use nom_rule::rule;

use super::common::ident;
use super::expr::literal_string;
use super::statement::parse_create_option;
use crate::ast::CreateSequenceStmt;
use crate::ast::DropSequenceStmt;
use crate::ast::Statement;
use crate::parser::common::map_res;
use crate::parser::common::IResult;
use crate::parser::common::*;
use crate::parser::input::Input;
use crate::parser::token::*;

pub fn sequence(i: Input) -> IResult<Statement> {
    rule!(
         #create_sequence: "`CREATE [OR REPLACE] SEQUENCE [IF NOT EXISTS] <sequence> [COMMENT = '<string_literal>']`"
         | #drop_sequence: "`DROP [IF EXISTS] <sequence>`"
    )(i)
}

fn create_sequence(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ SEQUENCE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #ident
            ~ ( COMMENT ~ "=" ~ #literal_string )?
        },
        |(_, opt_or_replace, _, opt_if_not_exists, sequence, opt_comment)| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateSequence(CreateSequenceStmt {
                create_option,
                sequence,
                comment: opt_comment.map(|(_, _, comment)| comment),
            }))
        },
    )(i)
}

fn drop_sequence(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            DROP ~ SEQUENCE ~ ( IF ~ ^EXISTS )? ~ #ident
        },
        |(_, _, opt_if_exists, sequence)| {
            Ok(Statement::DropSequence(DropSequenceStmt {
                sequence,
                if_exists: opt_if_exists.is_some(),
            }))
        },
    )(i)
}
