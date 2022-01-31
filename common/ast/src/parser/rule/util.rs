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
use nom::error::ErrorKind;
use nom::error::ParseError;
use nom::IResult;

use crate::parser::ast::Identifier;
use crate::parser::token::Token;
use crate::parser::token::TokenKind;

pub type Input<'a> = &'a [Token<'a>];

pub fn satisfy<'a, F, Error>(cond: F) -> impl Fn(Input<'a>) -> IResult<Input<'a>, &'a Token, Error>
where
    F: Fn(&Token) -> bool,
    Error: ParseError<Input<'a>>,
{
    move |i| match i.get(0).map(|t| {
        let b = cond(t);
        (t, b)
    }) {
        Some((t, true)) => Ok((&i[1..], t)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::Satisfy,
        ))),
    }
}

pub fn match_text<'a, Error>(
    text: &'static str,
) -> impl FnMut(Input<'a>) -> IResult<Input<'a>, &'a Token, Error>
where Error: ParseError<Input<'a>> {
    move |i| satisfy(|token: &Token| token.text == text)(i)
}

pub fn match_token<'a, Error>(
    kind: TokenKind,
) -> impl FnMut(Input<'a>) -> IResult<Input<'a>, &'a Token, Error>
where Error: ParseError<Input<'a>> {
    move |i| satisfy(|token: &Token| token.kind == kind)(i)
}

pub fn ident<'a, Error>(i: Input<'a>) -> IResult<Input<'a>, Identifier, Error>
where Error: ParseError<Input<'a>> {
    alt((
        map(satisfy(|token| token.kind == TokenKind::Ident), |token| {
            Identifier {
                name: token.text.to_string(),
                quote: None,
            }
        }),
        map(
            satisfy(|token| token.kind == TokenKind::QuotedIdent),
            |token| Identifier {
                name: token.text[1..token.text.len() - 1].to_string(),
                quote: Some('"'),
            },
        ),
    ))(i)
}

#[macro_export]
macro_rules! rule {
    ($($tt:tt)*) => { nom_rule::rule!(
        ($crate::parser::rule::util::match_text),
        ($crate::parser::rule::util::match_token),
        $($tt)*)
    }
}
