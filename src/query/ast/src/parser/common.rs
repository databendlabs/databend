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

use nom::branch::alt;
use nom::combinator::consumed;
use nom::combinator::map;
use nom::multi::many1;
use nom::sequence::terminated;
use nom::Offset;
use nom::Slice;
use nom_rule::rule;
use pratt::PrattError;
use pratt::PrattParser;
use pratt::Precedence;

use crate::ast::quote::QuotedIdent;
use crate::ast::ColumnID;
use crate::ast::DatabaseRef;
use crate::ast::Identifier;
use crate::ast::IdentifierType;
use crate::ast::SetType;
use crate::ast::TableRef;
use crate::parser::input::Input;
use crate::parser::input::WithSpan;
use crate::parser::query::with_options;
use crate::parser::token::*;
use crate::parser::Error;
use crate::parser::ErrorKind;
use crate::Range;
use crate::Span;

pub type IResult<'a, Output> = nom::IResult<Input<'a>, Output, Error<'a>>;

pub fn match_text(text: &'static str) -> impl FnMut(Input) -> IResult<&Token> {
    move |i| match i.tokens.first().filter(|token| token.text() == text) {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectText(text),
        ))),
    }
}

pub fn match_token(kind: TokenKind) -> impl FnMut(Input) -> IResult<&Token> {
    move |i| match i.tokens.first().filter(|token| token.kind == kind) {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectToken(kind),
        ))),
    }
}

pub fn any_token(i: Input) -> IResult<&Token> {
    match i.tokens.first().filter(|token| token.kind != EOI) {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::Other("expected any token but reached the end"),
        ))),
    }
}

pub fn lambda_params(i: Input) -> IResult<Vec<Identifier>> {
    let single_param = map(rule! {#ident}, |param| vec![param]);
    let multi_params = map(
        rule! { "(" ~ #comma_separated_list1(ident) ~ ")" },
        |(_, params, _)| params,
    );
    rule!(
        #single_param
        | #multi_params
    )(i)
}

pub fn ident(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_ident(false))(i)
}

pub fn grant_ident(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_grant_reserved_ident(false, true))(i)
}

pub fn plain_ident(i: Input) -> IResult<Identifier> {
    plain_identifier(|token| token.is_reserved_ident(false))(i)
}

pub fn ident_after_as(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_ident(true))(i)
}

pub fn function_name(i: Input) -> IResult<Identifier> {
    non_reserved_identifier(|token| token.is_reserved_function_name())(i)
}

pub fn stage_name(i: Input) -> IResult<Identifier> {
    let anonymous_stage = map(consumed(rule! { "~" }), |(span, _)| {
        Identifier::from_name(transform_span(span.tokens), "~")
    });

    rule!(
        #plain_ident
        | #anonymous_stage
    )(i)
}

fn plain_identifier(
    is_reserved_keyword: fn(&TokenKind) -> bool,
) -> impl FnMut(Input) -> IResult<Identifier> {
    move |i| {
        map(
            rule! {
                Ident
                | #non_reserved_keyword(is_reserved_keyword)
            },
            |token| Identifier {
                span: transform_span(&[token.clone()]),
                name: token.text().to_string(),
                quote: None,
                ident_type: IdentifierType::None,
            },
        )(i)
    }
}

fn quoted_identifier(i: Input) -> IResult<Identifier> {
    match_token(LiteralString)(i).and_then(|(i2, token)| {
        if token
            .text()
            .chars()
            .next()
            .filter(|c| i.dialect.is_ident_quote(*c))
            .is_some()
        {
            let QuotedIdent(ident, quote) = token.text().parse().map_err(|_| {
                nom::Err::Error(Error::from_error_kind(
                    i,
                    ErrorKind::Other("invalid identifier"),
                ))
            })?;
            Ok((i2, Identifier {
                span: transform_span(&[token.clone()]),
                name: ident,
                quote: Some(quote),
                ident_type: IdentifierType::None,
            }))
        } else {
            Err(nom::Err::Error(Error::from_error_kind(
                i,
                ErrorKind::ExpectToken(Ident),
            )))
        }
    })
}

fn identifier_hole(i: Input) -> IResult<Identifier> {
    check_template_mode(map(
        consumed(rule! {
            IDENTIFIER ~ ^"(" ~ #template_hole ~ ^")"
        }),
        |(span, (_, _, name, _))| Identifier {
            span: transform_span(span.tokens),
            name,
            quote: None,
            ident_type: IdentifierType::Hole,
        },
    ))(i)
}

fn identifier_variable(i: Input) -> IResult<Identifier> {
    map(
        consumed(rule! {
            IDENTIFIER ~ ^"(" ~ ^#variable_ident ~ ^")"
        }),
        |(span, (_, _, name, _))| Identifier {
            span: transform_span(span.tokens),
            name,
            quote: None,
            ident_type: IdentifierType::Variable,
        },
    )(i)
}

fn non_reserved_identifier(
    is_reserved_keyword: fn(&TokenKind) -> bool,
) -> impl FnMut(Input) -> IResult<Identifier> {
    move |i| {
        rule!(
            #plain_identifier(is_reserved_keyword)
            | #quoted_identifier
            | #identifier_hole
            | #identifier_variable
        )(i)
    }
}

fn non_reserved_keyword(
    is_reserved_keyword: fn(&TokenKind) -> bool,
) -> impl FnMut(Input) -> IResult<&Token> {
    move |i: Input| match i
        .tokens
        .first()
        .filter(|token| token.kind.is_keyword() && !is_reserved_keyword(&token.kind))
    {
        Some(token) => Ok((i.slice(1..), token)),
        _ => Err(nom::Err::Error(Error::from_error_kind(
            i,
            ErrorKind::ExpectToken(Ident),
        ))),
    }
}

pub fn database_ref(i: Input) -> IResult<DatabaseRef> {
    map(dot_separated_idents_1_to_2, |(catalog, database)| {
        DatabaseRef { catalog, database }
    })(i)
}

pub fn table_ref(i: Input) -> IResult<TableRef> {
    map(
        rule! {
           #dot_separated_idents_1_to_3 ~ #with_options?
        },
        |((catalog, database, table), with_options)| TableRef {
            catalog,
            database,
            table,
            with_options,
        },
    )(i)
}

pub fn set_type(i: Input) -> IResult<SetType> {
    map(
        rule! {
           (GLOBAL | SESSION | VARIABLE)?
        },
        |res| match res {
            Some(token) => match token.kind {
                GLOBAL => SetType::SettingsGlobal,
                SESSION => SetType::SettingsSession,
                VARIABLE => SetType::Variable,
                _ => unreachable!(),
            },
            None => SetType::SettingsSession,
        },
    )(i)
}

pub fn column_id(i: Input) -> IResult<ColumnID> {
    alt((
        map_res(rule! { ColumnPosition }, |token| {
            let name = token.text().to_string();
            let pos = name[1..]
                .parse::<usize>()
                .map_err(|e| nom::Err::Failure(e.into()))?;
            if pos == 0 {
                return Err(nom::Err::Failure(ErrorKind::Other(
                    "column position must be greater than 0",
                )));
            }
            Ok(ColumnID::Position(crate::ast::ColumnPosition {
                pos,
                name,
                span: Some(token.span),
            }))
        }),
        map_res(rule! { #ident }, |ident| Ok(ColumnID::Name(ident))),
    ))(i)
}

pub fn variable_ident(i: Input) -> IResult<String> {
    map(rule! { IdentVariable }, |t| t.text()[1..].to_string())(i)
}

/// Parse one to two idents separated by a dot, fulfilling from the right.
///
/// Example: `table.column`
pub fn dot_separated_idents_1_to_2(i: Input) -> IResult<(Option<Identifier>, Identifier)> {
    map(
        rule! {
           #ident ~ ( "." ~ #ident )?
        },
        |res| match res {
            (ident1, None) => (None, ident1),
            (ident0, Some((_, ident1))) => (Some(ident0), ident1),
        },
    )(i)
}

/// Parse one to three idents separated by a dot, fulfilling from the right.
///
/// Example: `db.table.column`
pub fn dot_separated_idents_1_to_3(
    i: Input,
) -> IResult<(Option<Identifier>, Option<Identifier>, Identifier)> {
    map(
        rule! {
            #ident ~ ( "." ~ #ident ~ ( "." ~ #ident )? )?
        },
        |res| match res {
            (ident2, None) => (None, None, ident2),
            (ident1, Some((_, ident2, None))) => (None, Some(ident1), ident2),
            (ident0, Some((_, ident1, Some((_, ident2))))) => (Some(ident0), Some(ident1), ident2),
        },
    )(i)
}

pub fn comma_separated_list0<'a, T>(
    item: impl FnMut(Input<'a>) -> IResult<'a, T>,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<T>> {
    separated_list0(match_text(","), item)
}

pub fn comma_separated_list0_ignore_trailing<'a, T>(
    item: impl FnMut(Input<'a>) -> IResult<'a, T>,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<T>> {
    nom::multi::separated_list0(match_text(","), item)
}

pub fn comma_separated_list1_ignore_trailing<'a, T>(
    item: impl FnMut(Input<'a>) -> IResult<'a, T>,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<T>> {
    nom::multi::separated_list1(match_text(","), item)
}

pub fn semicolon_terminated_list1<'a, T>(
    item: impl FnMut(Input<'a>) -> IResult<'a, T>,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<T>> {
    many1(terminated(item, match_text(";")))
}

pub fn comma_separated_list1<'a, T>(
    item: impl FnMut(Input<'a>) -> IResult<'a, T>,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<T>> {
    separated_list1(match_text(","), item)
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

/// A fork of `map_res` from nom, but doesn't require `FromExternalError`.
pub fn map_res<'a, O1, O2, F, G>(
    mut parser: F,
    mut f: G,
) -> impl FnMut(Input<'a>) -> IResult<'a, O2>
where
    F: nom::Parser<Input<'a>, O1, Error<'a>>,
    G: FnMut(O1) -> Result<O2, nom::Err<ErrorKind>>,
{
    move |input: Input| {
        let i = input;
        let bt = i.backtrace.clone();
        let (rest, o1) = parser.parse(input)?;
        match f(o1) {
            Ok(o2) => Ok((rest, o2)),
            Err(nom::Err::Error(e)) => {
                i.backtrace.restore(bt);
                Err(nom::Err::Error(Error::from_error_kind(i, e)))
            }
            Err(nom::Err::Failure(e)) => {
                i.backtrace.restore(bt);
                Err(nom::Err::Failure(Error::from_error_kind(i, e)))
            }
            Err(nom::Err::Incomplete(_)) => unreachable!(),
        }
    }
}

/// Try to find an error pattern that user may have made, and hint them with suggestion.
pub fn error_hint<'a, O, F>(
    mut match_error: F,
    message: &'static str,
) -> impl FnMut(Input<'a>) -> IResult<'a, ()>
where
    F: nom::Parser<Input<'a>, O, Error<'a>>,
{
    move |input: Input| match match_error.parse(input) {
        Ok(_) => Err(nom::Err::Error(Error::from_error_kind(
            input,
            ErrorKind::Other(message),
        ))),
        Err(_) => Ok((input, ())),
    }
}

pub fn transform_span(tokens: &[Token]) -> Span {
    Some(Range {
        start: tokens.first().unwrap().span.start,
        end: tokens.last().unwrap().span.end,
    })
}

pub fn run_pratt_parser<'a, I, P, E>(
    mut parser: P,
    iter: &I,
    rest: Input<'a>,
    input: Input<'a>,
) -> IResult<'a, P::Output>
where
    E: std::fmt::Debug,
    P: PrattParser<I, Input = WithSpan<'a, E>, Error = &'static str>,
    I: Iterator<Item = P::Input> + ExactSizeIterator + Clone,
{
    let mut iter_cloned = iter.clone();
    let mut iter = iter.clone().peekable();
    let len = iter.len();
    let expr = parser
        .parse_input(&mut iter, Precedence(0))
        .map_err(|err| {
            // Rollback parsing footprint on unused expr elements.
            input.backtrace.clear();

            let err_kind = match err {
                PrattError::EmptyInput => ErrorKind::Other("expecting an operand"),
                PrattError::UnexpectedNilfix(_) => ErrorKind::Other("unable to parse the element"),
                PrattError::UnexpectedPrefix(_) => {
                    ErrorKind::Other("unable to parse the prefix operator")
                }
                PrattError::UnexpectedInfix(_) => {
                    ErrorKind::Other("missing lhs or rhs for the binary operator")
                }
                PrattError::UnexpectedPostfix(_) => {
                    ErrorKind::Other("unable to parse the postfix operator")
                }
                PrattError::UserError(err) => ErrorKind::Other(err),
            };

            let span = iter_cloned
                .nth(len - iter.len() - 1)
                .map(|elem| elem.span)
                // It's safe to slice one more token because input must contain EOI.
                .unwrap_or_else(|| rest.slice(..1));

            nom::Err::Error(Error::from_error_kind(span, err_kind))
        })?;
    if let Some(elem) = iter.peek() {
        // Rollback parsing footprint on unused expr elements.
        input.backtrace.clear();
        Ok((input.slice(input.offset(&elem.span)..), expr))
    } else {
        Ok((rest, expr))
    }
}

pub fn check_template_mode<'a, O, F>(mut parser: F) -> impl FnMut(Input<'a>) -> IResult<'a, O>
where F: nom::Parser<Input<'a>, O, Error<'a>> {
    move |input: Input| {
        parser.parse(input).and_then(|(i, res)| {
            if input.mode.is_template() {
                Ok((i, res))
            } else {
                i.backtrace.clear();
                let error = Error::from_error_kind(
                    input,
                    ErrorKind::Other("variable is only available in SQL template"),
                );
                Err(nom::Err::Failure(error))
            }
        })
    }
}

pub fn template_hole(i: Input) -> IResult<String> {
    check_template_mode(map(
        rule! {
            ":" ~ ^#plain_ident
        },
        |(_, name)| name.name,
    ))(i)
}

macro_rules! declare_experimental_feature {
    ($check_fn_name: ident, $feature_name: literal) => {
        pub fn $check_fn_name<'a, O, F>(
            is_exclusive: bool,
            mut parser: F,
        ) -> impl FnMut(Input<'a>) -> IResult<'a, O>
        where
            F: nom::Parser<Input<'a>, O, Error<'a>>,
        {
            move |input: Input| {
                parser.parse(input).and_then(|(i, res)| {
                    if input.dialect.is_experimental() {
                        Ok((i, res))
                    } else {
                        i.backtrace.clear();
                        let error = Error::from_error_kind(
                            input,
                            ErrorKind::Other(
                                concat!(
                                    $feature_name,
                                    " only works in experimental dialect, try `set sql_dialect = 'experimental'`"
                                )
                            ),
                        );
                        if is_exclusive {
                            Err(nom::Err::Failure(error))
                        } else {
                            Err(nom::Err::Error(error))
                        }
                    }
                })
            }
        }
    };
}

declare_experimental_feature!(check_experimental_chain_function, "chain function");
declare_experimental_feature!(check_experimental_list_comprehension, "list comprehension");
