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
use nom::combinator::consumed;
use nom::combinator::map;
use nom::combinator::value;
use nom::error::context;
use nom::Slice as _;
use pratt::Affix;
use pratt::Associativity;
use pratt::PrattError;
use pratt::PrattParser;
use pratt::Precedence;

use crate::ast::*;
use crate::parser::error::Error;
use crate::parser::error::ErrorKind;
use crate::parser::expr::*;
use crate::parser::token::*;
use crate::parser::util::*;
use crate::rule;

pub fn query(i: Input) -> IResult<Query> {
    map(
        consumed(rule! {
            #set_expr
            ~ ( ORDER ~ ^BY ~ ^#comma_separated_list1(order_by_expr) )?
            ~ ( LIMIT ~ ^#comma_separated_list1(expr) )?
            ~ ( OFFSET ~ ^#expr )?
            ~ ( FORMAT ~ #ident )?
            : "`SELECT ...`"
        }),
        |(span, (body, opt_order_by_block, opt_limit_block, opt_offset_block, opt_format))| Query {
            span: span.0,
            body,
            order_by: opt_order_by_block
                .map(|(_, _, order_by)| order_by)
                .unwrap_or_default(),
            limit: opt_limit_block.map(|(_, limit)| limit).unwrap_or_default(),
            offset: opt_offset_block.map(|(_, offset)| offset),
            format: opt_format.map(|(_, format)| format.name),
        },
    )(i)
}

pub fn select_target(i: Input) -> IResult<SelectTarget> {
    let qualified_wildcard = map(
        rule! {
            ( #ident ~ "." ~ ( #ident ~ "." )? )? ~ "*"
        },
        |(res, _)| match res {
            Some((fst, _, Some((snd, _)))) => SelectTarget::QualifiedName(vec![
                Indirection::Identifier(fst),
                Indirection::Identifier(snd),
                Indirection::Star,
            ]),
            Some((fst, _, None)) => {
                SelectTarget::QualifiedName(vec![Indirection::Identifier(fst), Indirection::Star])
            }
            None => SelectTarget::QualifiedName(vec![Indirection::Star]),
        },
    );
    let projection = map(
        rule! {
            #expr ~ #alias_name?
        },
        |(expr, alias)| SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias,
        },
    );

    rule!(
        #qualified_wildcard
        | #projection
    )(i)
}

pub fn table_reference(i: Input) -> IResult<TableReference> {
    rule!(
        #joined_tables
        | #parenthesized_joined_tables
        | #subquery
        | #table_function
        | #aliased_table
    )(i)
}

pub fn aliased_table(i: Input) -> IResult<TableReference> {
    map(
        rule! {
            #ident ~ ( "." ~ #ident )? ~ ( "." ~ #ident )? ~ #travel_point? ~ #table_alias?
        },
        |(fst, snd, third, travel_point, alias)| {
            let (catalog, database, table) = match (fst, snd, third) {
                (catalog, Some((_, database)), Some((_, table))) => {
                    (Some(catalog), Some(database), table)
                }
                (database, Some((_, table)), None) => (None, Some(database), table),
                (database, None, Some((_, table))) => (None, Some(database), table),
                (table, None, None) => (None, None, table),
            };

            TableReference::Table {
                catalog,
                database,
                table,
                alias,
                travel_point,
            }
        },
    )(i)
}

pub fn travel_point(i: Input) -> IResult<TimeTravelPoint> {
    map(
        rule! {
            AT ~ "(" ~ SNAPSHOT ~ "=>" ~ #literal_string ~ ")"
        },
        |(_, _, _, _, s, _)| TimeTravelPoint::Snapshot(s),
    )(i)
}

pub fn alias_name(i: Input) -> IResult<Identifier> {
    let as_alias = map(rule! { AS ~ #ident_after_as }, |(_, name)| name);

    rule!(
        #ident
        | #as_alias
    )(i)
}

pub fn table_alias(i: Input) -> IResult<TableAlias> {
    map(alias_name, |name| TableAlias {
        name,
        columns: vec![],
    })(i)
}

pub fn table_function(i: Input) -> IResult<TableReference> {
    map(
        rule! {
            #ident ~ "(" ~ #comma_separated_list0(expr) ~ ")" ~ #table_alias?
        },
        |(name, _, params, _, alias)| TableReference::TableFunction {
            name,
            params,
            alias,
        },
    )(i)
}

pub fn subquery(i: Input) -> IResult<TableReference> {
    map(
        rule! {
            ( #parenthesized_query | #query ) ~ #table_alias?
        },
        |(subquery, alias)| TableReference::Subquery {
            subquery: Box::new(subquery),
            alias,
        },
    )(i)
}

pub fn parenthesized_query(i: Input) -> IResult<Query> {
    map(
        rule! {
            "(" ~ ( #parenthesized_query | #query ) ~ ")"
        },
        |(_, query, _)| query,
    )(i)
}

pub fn parenthesized_joined_tables(i: Input) -> IResult<TableReference> {
    map(
        rule! {
            "(" ~ ( #parenthesized_joined_tables | #joined_tables ) ~ ")"
        },
        |(_, joined_tables, _)| joined_tables,
    )(i)
}

pub fn joined_tables(i: Input) -> IResult<TableReference> {
    struct JoinElement<'a> {
        op: JoinOperator,
        condition: JoinCondition<'a>,
        right: Box<TableReference<'a>>,
    }

    let table_ref_without_join = |i| {
        rule!(
            #parenthesized_joined_tables
            | #subquery
            | #table_function
            | #aliased_table
        )(i)
    };
    let join_condition_on = map(
        rule! {
            ON ~ #expr
        },
        |(_, expr)| JoinCondition::On(Box::new(expr)),
    );
    let join_condition_using = map(
        rule! {
            USING ~ "(" ~ #comma_separated_list1(ident) ~ ")"
        },
        |(_, _, idents, _)| JoinCondition::Using(idents),
    );
    let join_condition = rule! { #join_condition_on | #join_condition_using };

    let join = map(
        rule! {
            #join_operator? ~ JOIN ~ #table_ref_without_join ~ #join_condition?
        },
        |(opt_op, _, right, condition)| JoinElement {
            op: opt_op.unwrap_or(JoinOperator::Inner),
            condition: condition.unwrap_or(JoinCondition::None),
            right: Box::new(right),
        },
    );
    let natural_join = map(
        rule! {
            NATURAL ~ #join_operator? ~ JOIN ~ #table_ref_without_join
        },
        |(_, opt_op, _, right)| JoinElement {
            op: opt_op.unwrap_or(JoinOperator::Inner),
            condition: JoinCondition::Natural,
            right: Box::new(right),
        },
    );

    map(
        rule!(
            #table_ref_without_join ~ ( #join | #natural_join )+
        ),
        |(fst, joins)| {
            joins.into_iter().fold(fst, |acc, elem| {
                let JoinElement {
                    op,
                    condition,
                    right,
                } = elem;
                TableReference::Join(Join {
                    op,
                    condition,
                    left: Box::new(acc),
                    right,
                })
            })
        },
    )(i)
}

pub fn join_operator(i: Input) -> IResult<JoinOperator> {
    alt((
        value(JoinOperator::Inner, rule! { INNER }),
        value(JoinOperator::LeftOuter, rule! { LEFT ~ OUTER? }),
        value(JoinOperator::RightOuter, rule! { RIGHT ~ OUTER? }),
        value(JoinOperator::FullOuter, rule! { FULL ~ OUTER? }),
        value(JoinOperator::CrossJoin, rule! { CROSS }),
    ))(i)
}

pub fn order_by_expr(i: Input) -> IResult<OrderByExpr> {
    map(
        rule! {
            #expr ~ ( ASC | DESC )?
        },
        |(expr, opt_asc)| OrderByExpr {
            expr,
            asc: opt_asc.map(|asc| asc.kind == ASC),
            nulls_first: None,
        },
    )(i)
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetExprElement<'a> {
    Select(SelectStmt<'a>),
    Query(Query<'a>),
    SetOperation {
        span: &'a [Token<'a>],
        op: SetOperator,
        all: bool,
    },
    Group(SetExpr<'a>),
}

pub fn set_expr_element(i: Input) -> IResult<SetExprElement> {
    dbg!(i.0.clone());
    let select = map(
        consumed(rule! {
             SELECT ~ DISTINCT? ~ ^#comma_separated_list1(select_target)
                ~ ( FROM ~ ^#comma_separated_list1(table_reference) )?
                ~ ( WHERE ~ ^#expr )?
                ~ ( GROUP ~ ^BY ~ ^#comma_separated_list1(expr) )?
                ~ ( HAVING ~ ^#expr )?
        }),
        |(
            span,
            (
                _select,
                opt_distinct,
                select_list,
                opt_from_block,
                opt_where_block,
                opt_group_by_block,
                opt_having_block,
            ),
        )| {
            SetExprElement::Select(SelectStmt {
                span: span.0,
                distinct: opt_distinct.is_some(),
                select_list,
                from: opt_from_block
                    .map(|(_, table_refs)| table_refs)
                    .unwrap_or_default(),
                selection: opt_where_block.map(|(_, selection)| selection),
                group_by: opt_group_by_block
                    .map(|(_, _, group_by)| group_by)
                    .unwrap_or_default(),
                having: opt_having_block.map(|(_, having)| having),
            })
        },
    );

    let set_operator = map(
        consumed(rule!((UNION | EXCEPT | INTERSECT) ~ ALL?)),
        |(span, (op, all))| match op.kind {
            TokenKind::UNION => SetExprElement::SetOperation {
                span: span.0,
                op: SetOperator::Union,
                all: all.is_some(),
            },
            TokenKind::INTERSECT => SetExprElement::SetOperation {
                span: span.0,
                op: SetOperator::Intersect,
                all: all.is_some(),
            },
            TokenKind::EXCEPT => SetExprElement::SetOperation {
                span: span.0,
                op: SetOperator::Except,
                all: all.is_some(),
            },
            _ => unreachable!(),
        },
    );

    let group = map(
        rule! {
           "("
           ~ ^#sub_set_expr()
           ~ ^")"
        },
        |(_, set_expr, _)| SetExprElement::Group(set_expr),
    );

    let query_in_set_expr = map(consumed(rule!(#query)), |(span, query)| {
        SetExprElement::Query(Query {
            span: span.0,
            body: query.body,
            order_by: query.order_by,
            limit: query.limit,
            offset: query.offset,
            format: query.format,
        })
    });
    map(
        rule!(#select | #group | #query_in_set_expr | #set_operator),
        |elem| elem,
    )(i)
}

pub fn set_expr(i: Input) -> IResult<SetExpr> {
    context("set_expr", sub_set_expr())(i)
}

pub fn sub_set_expr() -> impl FnMut(Input) -> IResult<SetExpr> {
    move |i| {
        let higher_prec_set_expr_element =
            |i| set_expr_element(i).and_then(|(rest, elem)| Ok((rest, elem)));
        let (rest, set_expr_elements) = rule!(#higher_prec_set_expr_element+)(i)?;
        let mut iter = set_expr_elements.into_iter();
        let set_expr = SetExprParser
            .parse(&mut iter)
            .map_err(|err| map_pratt_error(rest.slice(..1), err))
            .map_err(nom::Err::Error)?;
        if let Some(_) = iter.next() {
            return Err(nom::Err::Error(Error::from_error_kind(
                rest.slice(..1),
                ErrorKind::Other("unable to parse rest of the expression"),
            )));
        }
        Ok((rest, set_expr))
    }
}

struct SetExprParser;

impl<'a, I: Iterator<Item = SetExprElement<'a>>> PrattParser<I> for SetExprParser {
    type Error = pratt::NoError;
    type Input = SetExprElement<'a>;
    type Output = SetExpr<'a>;

    fn query(&mut self, input: &Self::Input) -> pratt::Result<Affix> {
        let affix = match input {
            SetExprElement::SetOperation { op, .. } => match op {
                SetOperator::Union | SetOperator::Except => {
                    Affix::Infix(Precedence(10), Associativity::Left)
                }
                SetOperator::Intersect => Affix::Infix(Precedence(20), Associativity::Left),
            },
            _ => Affix::Nilfix,
        };
        Ok(affix)
    }

    fn primary(&mut self, input: Self::Input) -> pratt::Result<SetExpr<'a>> {
        let set_expr = match input {
            SetExprElement::Select(select) => SetExpr::Select(Box::new(select)),
            SetExprElement::Query(query) => SetExpr::Query(Box::new(query)),
            SetExprElement::Group(expr) => expr,
            _ => unreachable!(),
        };
        Ok(set_expr)
    }

    fn infix(
        &mut self,
        lhs: Self::Output,
        op: Self::Input,
        rhs: Self::Output,
    ) -> pratt::Result<SetExpr<'a>> {
        let set_expr = match op {
            SetExprElement::SetOperation { span, op, all, .. } => {
                SetExpr::SetOperation(Box::new(SetOperation {
                    span,
                    op,
                    all,
                    left: Box::new(lhs),
                    right: Box::new(rhs),
                }))
            }
            _ => unreachable!(),
        };
        Ok(set_expr)
    }

    fn prefix(
        &mut self,
        _op: Self::Input,
        _rhs: Self::Output,
    ) -> Result<Self::Output, Self::Error> {
        todo!()
    }

    fn postfix(
        &mut self,
        _lhs: Self::Output,
        _op: Self::Input,
    ) -> Result<Self::Output, Self::Error> {
        todo!()
    }
}

fn map_pratt_error<'a>(
    next_token: Input<'a>,
    err: PrattError<SetExprElement<'a>, pratt::NoError>,
) -> Error<'a> {
    match err {
        PrattError::EmptyInput => Error::from_error_kind(
            next_token,
            ErrorKind::Other("expected more tokens for expression"),
        ),
        PrattError::UnexpectedNilfix(_) => Error::from_error_kind(
            next_token,
            ErrorKind::Other("unable to parse the expression value"),
        ),
        PrattError::UnexpectedPrefix(_) => Error::from_error_kind(
            next_token,
            ErrorKind::Other("unable to parse the prefix operator"),
        ),
        PrattError::UnexpectedInfix(_) => Error::from_error_kind(
            next_token,
            ErrorKind::Other("unable to parse the binary operator"),
        ),
        PrattError::UnexpectedPostfix(_) => Error::from_error_kind(
            next_token,
            ErrorKind::Other("unable to parse the postfix operator"),
        ),
        PrattError::UserError(_) => unreachable!(),
    }
}
