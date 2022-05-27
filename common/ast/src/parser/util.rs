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

use std::num::ParseIntError;
use std::ops::Range;
use std::ops::RangeFrom;
use std::ops::RangeFull;
use std::ops::RangeTo;

use nom::branch::alt;
use nom::combinator::map;
use nom::Slice as _;

use crate::ast::Identifier;
use crate::parser::error::Backtrace;
use crate::parser::error::Error;
use crate::parser::error::ErrorKind;
use crate::parser::token::*;

pub type IResult<'a, Output> = nom::IResult<Input<'a>, Output, Error<'a>>;

/// Input tokens slice with a backtrace that records all errors including
/// the optional branch.
#[derive(Debug, Clone, Copy)]
pub struct Input<'a>(pub &'a [Token<'a>], pub &'a Backtrace<'a>);

pub fn match_text(text: &'static str) -> impl FnMut(Input) -> IResult<&Token> {
    move |i| match i.0.get(0).filter(|token| token.text() == text) {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectText(text),
        ))),
    }
}

pub fn match_token(kind: TokenKind) -> impl FnMut(Input) -> IResult<&Token> {
    move |i| match i.0.get(0).filter(|token| token.kind == kind) {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectToken(kind),
        ))),
    }
}

#[macro_export]
macro_rules! rule {
    ($($tt:tt)*) => { nom_rule::rule!(
        $crate::parser::util::match_text,
        $crate::parser::util::match_token,
        $($tt)*)
    }
}

pub fn ident(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_ident(false))(i)
}

pub fn ident_after_as(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_ident(true))(i)
}

pub fn function_name(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_function_name(false))(i)
}

pub fn function_name_after_as(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_function_name(true))(i)
}

fn non_reserved_identifier(
    is_reserved_keyword: fn(&TokenKind) -> bool,
) -> impl FnMut(Input) -> IResult<Identifier> {
    move |i| {
        alt((
            map(
                alt((rule! { Ident }, non_reserved_keyword(is_reserved_keyword))),
                |token| Identifier {
                    span: token.clone(),
                    name: token.text().to_string(),
                    quote: None,
                },
            ),
            move |i| {
                match_token(QuotedString)(i).and_then(|(i2, token)| {
                    if token.text().starts_with('\'') {
                        Err(nom::Err::Error(Error::from_error_kind(
                            i,
                            ErrorKind::ExpectToken(Ident),
                        )))
                    } else {
                        Ok((i2, Identifier {
                            span: token.clone(),
                            name: token.text()[1..token.text().len() - 1].to_string(),
                            quote: Some(token.text().chars().next().unwrap()),
                        }))
                    }
                })
            },
        ))(i)
    }
}

fn non_reserved_keyword(
    is_reserved_keyword: fn(&TokenKind) -> bool,
) -> impl FnMut(Input) -> IResult<&Token> {
    move |i: Input| match i
        .0
        .get(0)
        .filter(|token| token.kind.is_keyword() && !is_reserved_keyword(&token.kind))
    {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectToken(Ident),
        ))),
    }
}

pub fn literal_string(i: Input) -> IResult<String> {
    match_token(QuotedString)(i).and_then(|(i2, token)| {
        if token.text().starts_with('\'') {
            Ok((i2, token.text()[1..token.text().len() - 1].to_string()))
        } else {
            Err(nom::Err::Error(Error::from_error_kind(
                i,
                ErrorKind::ExpectToken(QuotedString),
            )))
        }
    })
}

pub fn at_string(i: Input) -> IResult<String> {
    match_token(AtString)(i).and_then(|(i2, token)| {
        Ok((i2, token.text()[1..token.text().len() - 1].to_string()))
    })
}

pub fn literal_u64(i: Input) -> IResult<u64> {
    match_token(LiteralNumber)(i).and_then(|(i2, token)| {
        token
            .text()
            .parse()
            .map(|num| (i2, num))
            .map_err(|err: ParseIntError| nom::Err::Error(Error::from_error_kind(i, err.into())))
    })
}

pub fn comma_separated_list0<'a, T>(
    item: impl FnMut(Input<'a>) -> IResult<'a, T>,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<T>> {
    // TODO: fork one
    separated_list0(match_text(","), item)
}

pub fn comma_separated_list1<'a, T>(
    item: impl FnMut(Input<'a>) -> IResult<'a, T>,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<T>> {
    separated_list1(match_text(","), item)
}

pub fn comma_separated_list1_allow_trailling<'a, T>(
    item: impl FnMut(Input<'a>) -> IResult<'a, T>,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<T>> {
    nom::multi::separated_list1(match_text(","), item)
}

/// A fork of `separated_list0` from nom, but never forgive parser error
/// after a separator is encountered, and always forgive the first element
/// failure.
pub fn separated_list0<I, O, O2, E, F, G>(
    mut sep: G,
    mut f: F,
) -> impl FnMut(I) -> nom::IResult<I, Vec<O>, E>
where
    I: Clone + nom::InputLength,
    F: nom::Parser<I, O, E>,
    G: nom::Parser<I, O2, E>,
    E: nom::error::ParseError<I>,
{
    move |mut i: I| {
        let mut res = Vec::new();

        match f.parse(i.clone()) {
            Err(_) => return Ok((i, res)),
            Ok((i1, o)) => {
                res.push(o);
                i = i1;
            }
        }

        loop {
            let len = i.input_len();
            match sep.parse(i.clone()) {
                Err(nom::Err::Error(_)) => return Ok((i, res)),
                Err(e) => return Err(e),
                Ok((i1, _)) => {
                    // infinite loop check: the parser must always consume
                    if i1.input_len() == len {
                        return Err(nom::Err::Error(E::from_error_kind(
                            i1,
                            nom::error::ErrorKind::SeparatedList,
                        )));
                    }

                    match f.parse(i1.clone()) {
                        Err(e) => return Err(e),
                        Ok((i2, o)) => {
                            res.push(o);
                            i = i2;
                        }
                    }
                }
            }
        }
    }
}

/// A fork of `separated_list1` from nom, but never forgive parser error
/// after a separator is encountered.
pub fn separated_list1<I, O, O2, E, F, G>(
    mut sep: G,
    mut f: F,
) -> impl FnMut(I) -> nom::IResult<I, Vec<O>, E>
where
    I: Clone + nom::InputLength,
    F: nom::Parser<I, O, E>,
    G: nom::Parser<I, O2, E>,
    E: nom::error::ParseError<I>,
{
    move |mut i: I| {
        let mut res = Vec::new();

        // Parse the first element
        match f.parse(i.clone()) {
            Err(e) => return Err(e),
            Ok((i1, o)) => {
                res.push(o);
                i = i1;
            }
        }

        loop {
            let len = i.input_len();
            match sep.parse(i.clone()) {
                Err(nom::Err::Error(_)) => return Ok((i, res)),
                Err(e) => return Err(e),
                Ok((i1, _)) => {
                    // infinite loop check: the parser must always consume
                    if i1.input_len() == len {
                        return Err(nom::Err::Error(E::from_error_kind(
                            i1,
                            nom::error::ErrorKind::SeparatedList,
                        )));
                    }

                    match f.parse(i1.clone()) {
                        Err(e) => return Err(e),
                        Ok((i2, o)) => {
                            res.push(o);
                            i = i2;
                        }
                    }
                }
            }
        }
    }
}

impl<'a> std::ops::Deref for Input<'a> {
    type Target = [Token<'a>];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a> nom::InputLength for Input<'a> {
    fn input_len(&self) -> usize {
        self.0.input_len()
    }
}

impl<'a> nom::Offset for Input<'a> {
    fn offset(&self, second: &Self) -> usize {
        let fst = self.0.as_ptr();
        let snd = second.0.as_ptr();

        (snd as usize - fst as usize) / std::mem::size_of::<Token>()
    }
}

impl<'a> nom::Slice<Range<usize>> for Input<'a> {
    fn slice(&self, range: Range<usize>) -> Self {
        Input(&self.0[range], self.1)
    }
}

impl<'a> nom::Slice<RangeTo<usize>> for Input<'a> {
    fn slice(&self, range: RangeTo<usize>) -> Self {
        Input(&self.0[range], self.1)
    }
}

impl<'a> nom::Slice<RangeFrom<usize>> for Input<'a> {
    fn slice(&self, range: RangeFrom<usize>) -> Self {
        Input(&self.0[range], self.1)
    }
}

impl<'a> nom::Slice<RangeFull> for Input<'a> {
    fn slice(&self, _: RangeFull) -> Self {
        *self
    }
}
