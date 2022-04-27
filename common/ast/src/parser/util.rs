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

use crate::ast::Identifier;
use crate::parser::error::Error;
use crate::parser::error::ErrorKind;
use crate::parser::token::*;

pub type Input<'a> = &'a [Token<'a>];
pub type IResult<'a, Output> = nom::IResult<Input<'a>, Output, Error<'a>>;

pub fn match_text(text: &'static str) -> impl FnMut(Input) -> IResult<&Token> {
    move |i| match i.get(0).filter(|token| token.text() == text) {
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
    non_reserved_identifier(|token| token.is_reserved_ident())(i)
}

pub fn lit_string_ident(i: Input) -> IResult<Identifier> {
    map(literal_string, |name| Identifier {
        name,
        quote: Some('\''),
    })(i)
}

pub fn function_name(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_function_name())(i)
}

fn non_reserved_identifier(
    is_reserved_keyword: fn(&TokenKind) -> bool,
) -> impl FnMut(Input) -> IResult<Identifier> {
    move |i| {
        alt((
            map(
                alt((
                    match_token(TokenKind::Ident),
                    non_reserved_keyword(is_reserved_keyword),
                )),
                |token| Identifier {
                    name: token.text().to_string(),
                    quote: None,
                },
            ),
            map(match_token(TokenKind::QuotedIdent), |token| Identifier {
                name: token.text()[1..token.text().len() - 1].to_string(),
                quote: Some(token.text().chars().next().unwrap()),
            }),
        ))(i)
    }
}

fn non_reserved_keyword(
    is_reserved_keyword: fn(&TokenKind) -> bool,
) -> impl FnMut(Input) -> IResult<&Token> {
    move |i: Input| match i
        .get(0)
        .filter(|token| token.kind.is_keyword() && !is_reserved_keyword(&token.kind))
    {
        Some(token) => Ok((&i[1..], token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectToken(Ident),
        ))),
    }
}

pub fn literal_string(i: Input) -> IResult<String> {
    map(match_token(LiteralString), |token| {
        token.text()[1..token.text().len() - 1].to_string()
    })(i)
}

pub fn literal_u64(i: Input) -> IResult<u64> {
    match_token(LiteralNumber)(i).and_then(|(i2, token)| {
        token.text().parse().map(|num| (i2, num)).map_err(|err| {
            nom::Err::Error(Error::from_error_kind(i, ErrorKind::ParseIntError(err)))
        })
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

/// A fork of `map_res` from nom, but use `ErrorKind`.
pub fn map_res<'a, O1, O2, F, G>(
    mut parser: F,
    mut f: G,
) -> impl FnMut(Input<'a>) -> nom::IResult<Input<'a>, O2, Error<'a>>
where
    F: nom::Parser<Input<'a>, O1, Error<'a>>,
    G: FnMut(O1) -> Result<O2, ErrorKind>,
{
    move |input| {
        let i = input.clone();
        let (input, output) = parser.parse(input)?;
        match f(output) {
            Ok(output) => Ok((input, output)),
            Err(e) => Err(nom::Err::Error(Error::from_error_kind(i, e))),
        }
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
