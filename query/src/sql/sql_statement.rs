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

use nom::bytes::complete::tag;
use nom::bytes::complete::take_till1;
use nom::character::complete::digit1;
use nom::character::complete::multispace0;
use nom::character::complete::multispace1;
use nom::IResult;

use crate::sql::statements::DfDeleteStatement;
use crate::sql::statements::DfExplain;
use crate::sql::statements::DfInsertStatement;
use crate::sql::statements::DfQueryStatement;

/// Tokens parsed by `DFParser` are converted into these values.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum DfStatement<'a> {
    Query(Box<DfQueryStatement>),
    Explain(DfExplain<'a>),
    InsertQuery(DfInsertStatement<'a>),
    Delete(Box<DfDeleteStatement>),

    // “See You Again” is a tribute to the late Furious actor Paul Walker
    // who died tragically in November of 2013 after his car crashed and
    // burst into flames in Valencia, CA.
    // The first verse is from the perspective of Vin Diesel and
    // the Furious 7 cast members, while the second is from Paul Walker.
    //
    // It's been a long day without you, my friend
    // And I'll tell you all about it when I see you again
    // We've come a long way from where we began
    // Oh, I'll tell you all about it when I see you again
    // When I see you again
    //
    // It's been a long day without you, my friend
    // And I'll tell you all about it when I see you again
    // We've come a long way from where we began
    // Oh, I'll tell you all about it when I see you again
    // When I see you again
    // Oh-oh-oh-oh, oh-oh-oh, oh-oh-oh-oh (Uh)
    // Yeah yeah (Yeah)
    // Ooh-ooh-ooh-ooh-ooh, ooh-ooh-ooh-ooh
    // Ooh-ooh-ooh-ooh-ooh, ooh-ooh-ooh-ooh-ooh (Yo)
    // When I see you again (Yo, uh)
    // Oh-oh-oh-oh, oh-oh-oh, oh-oh-oh-oh
    // See you again, yeah, yeah, oh-oh (Yo, yo)
    // Ooh-ooh-ooh-ooh-ooh, ooh-ooh-ooh-ooh
    // Ooh-ooh-ooh-ooh-ooh, ooh-ooh-ooh-ooh-ooh (Uh-huh, yup)
    // When I see you again
    //
    // We dedicate this song to the old planner.
    // It's been fun with you, but it's time for us to depart, see you again!
    //
    // While reading `SeeYouAgain`, we will forward the entire query to the new
    // planner directly.
    SeeYouAgain,
}

/// Comment hints from SQL.
/// It'll be enabled when using `--comment` in mysql client.
/// Eg: `SELECT * FROM system.number LIMIT 1; -- { ErrorCode 25 }`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DfHint {
    pub error_code: Option<u16>,
    pub comment: String,
    pub prefix: String,
}

impl DfHint {
    pub fn create_from_comment(comment: &str, prefix: &str) -> Self {
        let error_code = match Self::parse_code(comment) {
            Ok((_, c)) => c,
            Err(_) => None,
        };

        Self {
            error_code,
            comment: comment.to_owned(),
            prefix: prefix.to_owned(),
        }
    }

    //  { ErrorCode 25 }
    pub fn parse_code(comment: &str) -> IResult<&str, Option<u16>> {
        let (comment, _) = take_till1(|c| c == '{')(comment)?;
        let (comment, _) = tag("{")(comment)?;
        let (comment, _) = multispace0(comment)?;
        let (comment, _) = tag("ErrorCode")(comment)?;
        let (comment, _) = multispace1(comment)?;
        let (comment, code) = digit1(comment)?;

        let code = code.parse::<u16>().ok();
        Ok((comment, code))
    }
}
