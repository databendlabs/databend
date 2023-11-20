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

use nom::combinator::map;

use crate::ast::CreateStreamStmt;
use crate::ast::DescribeStreamStmt;
use crate::ast::DropStreamStmt;
use crate::ast::ShowStreamsStmt;
use crate::ast::Statement;
use crate::ast::StreamPoint;
use crate::parser::expr::literal_string;
use crate::parser::statement::show_limit;
use crate::parser::token::TokenKind::*;
use crate::rule;
use crate::util::dot_separated_idents_1_to_2;
use crate::util::dot_separated_idents_1_to_3;
use crate::util::IResult;
use crate::Input;

pub fn stream_table(i: Input) -> IResult<Statement> {
    rule!(
         #create_stream: "`CREATE STREAM [IF NOT EXISTS] [<database>.]<stream> ON TABLE [<database>.]<table> [<stream_point>] [COMMENT = '<string_literal>']`"
         | #drop_stream: "`DROP STREAM [IF EXISTS] [<database>.]<stream>`"
         | #show_streams: "`SHOW [FULL] STREAMS [FROM <database>] [<show_limit>]`"
         | #describe_stream: "`DESCRIBE STREAM [<database>.]<stream>`"
    )(i)
}

fn create_stream(i: Input) -> IResult<Statement> {
    map(
        rule! {
            CREATE ~ STREAM ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ ON ~ TABLE ~ #dot_separated_idents_1_to_2
            ~ ( #stream_point )?
            ~ ( COMMENT ~ "=" ~ #literal_string )?
        },
        |(
            _,
            _,
            opt_if_not_exists,
            (catalog, database, stream),
            _,
            _,
            (table_database, table),
            stream_point,
            opt_comment,
        )| {
            Statement::CreateStream(CreateStreamStmt {
                if_not_exists: opt_if_not_exists.is_some(),
                catalog,
                database,
                stream,
                table_database,
                table,
                stream_point,
                comment: opt_comment.map(|(_, _, comment)| comment),
            })
        },
    )(i)
}

fn drop_stream(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ STREAM ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3
        },
        |(_, _, opt_if_exists, (catalog, database, stream))| {
            Statement::DropStream(DropStreamStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
                database,
                stream,
            })
        },
    )(i)
}

fn stream_point(i: Input) -> IResult<StreamPoint> {
    let mut at_stream = map(
        rule! { AT ~ "(" ~ STREAM ~ "=>" ~  #dot_separated_idents_1_to_2 ~ ")" },
        |(_, _, _, _, (database, name), _)| StreamPoint::AtStream { database, name },
    );
    rule!(
        #at_stream
    )(i)
}

fn show_streams(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ FULL? ~ STREAMS ~ ( ( FROM | IN ) ~ #dot_separated_idents_1_to_2 )? ~ #show_limit?
        },
        |(_, opt_full, _, ctl_db, limit)| {
            let (catalog, database) = match ctl_db {
                Some((_, (Some(c), d))) => (Some(c), Some(d)),
                Some((_, (None, d))) => (None, Some(d)),
                _ => (None, None),
            };
            Statement::ShowStreams(ShowStreamsStmt {
                catalog,
                database,
                full: opt_full.is_some(),
                limit,
            })
        },
    )(i)
}

fn describe_stream(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ( DESC | DESCRIBE ) ~ STREAM ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, stream))| {
            Statement::DescribeStream(DescribeStreamStmt {
                catalog,
                database,
                stream,
            })
        },
    )(i)
}
