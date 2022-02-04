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

use nom::branch::alt;
use nom::combinator::map;

use crate::parser::ast::Identifier;
use crate::parser::rule::error::Error;
use crate::parser::rule::error::ErrorKind;
use crate::parser::token::*;

pub type Input<'a> = &'a [Token<'a>];
pub type IResult<'a, Output> = nom::IResult<Input<'a>, Output, Error<'a>>;

pub fn match_text(text: &'static str) -> impl FnMut(Input) -> IResult<&Token> {
    move |i| match i.get(0).filter(|token| token.text == text) {
        Some(token) => Ok((&i[1..], token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectText(text),
        ))),
    }
}

pub fn match_token(kind: TokenKind) -> impl FnMut(Input) -> IResult<&Token> {
    move |i| match i.get(0).filter(|token| token.kind == kind) {
        Some(token) => Ok((&i[1..], token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectToken(kind),
        ))),
    }
}

pub fn ident(i: Input) -> IResult<Identifier> {
    alt((
        map(match_token(TokenKind::Ident), |token| Identifier {
            name: token.text.to_string(),
            quote: None,
        }),
        map(match_token(TokenKind::QuotedIdent), |token| Identifier {
            name: token.text[1..token.text.len() - 1].to_string(),
            quote: Some('"'),
        }),
    ))(i)
}

pub fn literal_u64(i: Input) -> IResult<u64> {
    match_token(LiteralNumber)(i).and_then(|(input_inner, token)| {
        token
            .text
            .parse()
            .map(|num| (input_inner, num))
            .map_err(|err| {
                nom::Err::Error(Error::from_error_kind(i, ErrorKind::ParseIntError(err)))
            })
    })
}

#[macro_export]
macro_rules! rule {
    ($($tt:tt)*) => { nom_rule::rule!(
        $crate::parser::rule::util::match_text,
        $crate::parser::rule::util::match_token,
        $($tt)*)
    }
}
