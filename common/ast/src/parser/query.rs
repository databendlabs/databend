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
use pratt::Affix;
use pratt::Associativity;
use pratt::PrattParser;
use pratt::Precedence;

use crate::ast::*;
use crate::parser::expr::*;
use crate::parser::token::*;
use crate::parser::util::*;
use crate::rule;

pub fn query(i: Input) -> IResult<Query> {
    map(
        consumed(rule! {
            #set_operation
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
pub enum SetOperationElement<'a> {
    SelectStmt {
        distinct: bool,
        select_list: Box<Vec<SelectTarget<'a>>>,
        from: Box<Vec<TableReference<'a>>>,
        selection: Box<Option<Expr<'a>>>,
        group_by: Box<Vec<Expr<'a>>>,
        having: Box<Option<Expr<'a>>>,
    },
    SetOperation {
        op: SetOperator,
        all: bool,
    },
    Group(SetExpr<'a>),
}

pub fn set_operation_element(i: Input) -> IResult<WithSpan<SetOperationElement>> {
    let set_operator = map(
        rule! {
            ( UNION | EXCEPT | INTERSECT ) ~ ALL?
        },
        |(op, all)| {
            let op = match op.kind {
                UNION => SetOperator::Union,
                INTERSECT => SetOperator::Intersect,
                EXCEPT => SetOperator::Except,
                _ => unreachable!(),
            };
            SetOperationElement::SetOperation {
                op,
                all: all.is_some(),
            }
        },
    );

    let select_stmt = map(
        rule! {
             SELECT ~ DISTINCT? ~ ^#comma_separated_list1(select_target)
                ~ ( FROM ~ ^#comma_separated_list1(table_reference) )?
                ~ ( WHERE ~ ^#expr )?
                ~ ( GROUP ~ ^BY ~ ^#comma_separated_list1(expr) )?
                ~ ( HAVING ~ ^#expr )?
        },
        |(
            _select,
            opt_distinct,
            select_list,
            opt_from_block,
            opt_where_block,
            opt_group_by_block,
            opt_having_block,
        )| {
            SetOperationElement::SelectStmt {
                distinct: opt_distinct.is_some(),
                select_list: Box::new(select_list),
                from: Box::new(
                    opt_from_block
                        .map(|(_, table_refs)| table_refs)
                        .unwrap_or_default(),
                ),
                selection: Box::new(opt_where_block.map(|(_, selection)| selection)),
                group_by: Box::new(
                    opt_group_by_block
                        .map(|(_, _, group_by)| group_by)
                        .unwrap_or_default(),
                ),
                having: Box::new(opt_having_block.map(|(_, having)| having)),
            }
        },
    );

    let group = map(
        rule! {
           "("
           ~ ^#set_operation
           ~ ^")"
        },
        |(_, set_expr, _)| SetOperationElement::Group(set_expr),
    );

    let (rest, (span, elem)) = consumed(rule!( #group | #set_operator | #select_stmt))(i)?;
    Ok((rest, WithSpan { span, elem }))
}

pub fn set_operation(i: Input) -> IResult<SetExpr> {
    let (rest, set_operation_elements) = rule!(#set_operation_element+)(i)?;
    let iter = &mut set_operation_elements.into_iter();
    run_pratt_parser(SetOperationParser, iter, rest, i)
}

struct SetOperationParser;

impl<'a, I: Iterator<Item = WithSpan<'a, SetOperationElement<'a>>>> PrattParser<I>
    for SetOperationParser
{
    type Error = pratt::NoError;
    type Input = WithSpan<'a, SetOperationElement<'a>>;
    type Output = SetExpr<'a>;

    fn query(&mut self, input: &Self::Input) -> pratt::Result<Affix> {
        let affix = match &input.elem {
            SetOperationElement::SetOperation { op, .. } => match op {
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
        let set_expr = match input.elem {
            SetOperationElement::Group(expr) => expr,
            SetOperationElement::SelectStmt {
                distinct,
                select_list,
                from,
                selection,
                group_by,
                having,
            } => SetExpr::Select(Box::new(SelectStmt {
                span: input.span.0,
                distinct,
                select_list: *select_list,
                from: *from,
                selection: *selection,
                group_by: *group_by,
                having: *having,
            })),
            _ => unreachable!(),
        };
        Ok(set_expr)
    }

    fn infix(
        &mut self,
        lhs: Self::Output,
        input: Self::Input,
        rhs: Self::Output,
    ) -> pratt::Result<SetExpr<'a>> {
        let set_expr = match input.elem {
            SetOperationElement::SetOperation { op, all, .. } => {
                SetExpr::SetOperation(Box::new(SetOperation {
                    span: input.span.0,
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
        unreachable!()
    }

    fn postfix(
        &mut self,
        _lhs: Self::Output,
        _op: Self::Input,
    ) -> Result<Self::Output, Self::Error> {
        unreachable!()
    }
}
