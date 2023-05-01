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
use nom::combinator::value;
use nom::error::context;
use pratt::Affix;
use pratt::Associativity;
use pratt::PrattParser;
use pratt::Precedence;

use super::stage::select_stage_option;
use super::stage::stage_location;
use crate::ast::*;
use crate::input::Input;
use crate::input::WithSpan;
use crate::parser::expr::*;
use crate::parser::statement::hint;
use crate::parser::token::*;
use crate::rule;
use crate::util::*;

pub fn query(i: Input) -> IResult<Query> {
    context(
        "`SELECT ...`",
        map(set_operation, |set_expr| set_expr.into_query()),
    )(i)
}

pub fn set_operation(i: Input) -> IResult<SetExpr> {
    let (rest, set_operation_elements) = rule!(#set_operation_element+)(i)?;
    let iter = &mut set_operation_elements.into_iter();
    run_pratt_parser(SetOperationParser, iter, rest, i)
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetOperationElement {
    With(With),
    SelectStmt {
        hints: Option<Hint>,
        distinct: bool,
        select_list: Box<Vec<SelectTarget>>,
        from: Box<Vec<TableReference>>,
        selection: Box<Option<Expr>>,
        group_by: Option<GroupBy>,
        having: Box<Option<Expr>>,
        window_list: Option<Vec<WindowDefinition>>,
    },
    SetOperation {
        op: SetOperator,
        all: bool,
    },
    OrderBy {
        order_by: Vec<OrderByExpr>,
    },
    Limit {
        limit: Vec<Expr>,
    },
    Offset {
        offset: Expr,
    },
    IgnoreResult,
    Group(SetExpr),
}

pub fn set_operation_element(i: Input) -> IResult<WithSpan<SetOperationElement>> {
    let with = map(with, SetOperationElement::With);
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
             SELECT ~ #hint? ~ DISTINCT? ~ ^#comma_separated_list1(select_target)
                ~ ( FROM ~ ^#comma_separated_list1(table_reference) )?
                ~ ( WHERE ~ ^#expr )?
                ~ ( GROUP ~ ^BY ~ ^#group_by_items )?
                ~ ( HAVING ~ ^#expr )?
                ~ ( WINDOW ~ ^#comma_separated_list1(window_clause) )?
        },
        |(
            _select,
            opt_hints,
            opt_distinct,
            select_list,
            opt_from_block,
            opt_where_block,
            opt_group_by_block,
            opt_having_block,
            opt_window_block,
        )| {
            SetOperationElement::SelectStmt {
                hints: opt_hints,
                distinct: opt_distinct.is_some(),
                select_list: Box::new(select_list),
                from: Box::new(
                    opt_from_block
                        .map(|(_, table_refs)| table_refs)
                        .unwrap_or_default(),
                ),
                selection: Box::new(opt_where_block.map(|(_, selection)| selection)),
                group_by: opt_group_by_block.map(|(_, _, group_by)| group_by),
                having: Box::new(opt_having_block.map(|(_, having)| having)),
                window_list: opt_window_block.map(|(_, windows)| windows),
            }
        },
    );
    let order_by = map(
        rule! {
            ORDER ~ ^BY ~ ^#comma_separated_list1(order_by_expr)
        },
        |(_, _, order_by)| SetOperationElement::OrderBy { order_by },
    );
    let limit = map(
        rule! {
            LIMIT ~ ^#comma_separated_list1(expr)
        },
        |(_, limit)| SetOperationElement::Limit { limit },
    );
    let offset = map(
        rule! {
            OFFSET ~ ^#expr
        },
        |(_, offset)| SetOperationElement::Offset { offset },
    );
    let ignore_result = map(
        rule! {
            IGNORE_RESULT
        },
        |_| SetOperationElement::IgnoreResult,
    );
    let group = map(
        rule! {
           "(" ~ #set_operation ~ ^")"
        },
        |(_, set_expr, _)| SetOperationElement::Group(set_expr),
    );

    let (rest, (span, elem)) = consumed(rule! {
        #group
        | #with
        | #set_operator
        | #select_stmt
        | #order_by
        | #limit
        | #offset
        | #ignore_result
    })(i)?;
    Ok((rest, WithSpan { span, elem }))
}

struct SetOperationParser;

impl<'a, I: Iterator<Item = WithSpan<'a, SetOperationElement>>> PrattParser<I>
    for SetOperationParser
{
    type Error = &'static str;
    type Input = WithSpan<'a, SetOperationElement>;
    type Output = SetExpr;

    fn query(&mut self, input: &Self::Input) -> Result<Affix, &'static str> {
        let affix = match &input.elem {
            SetOperationElement::SetOperation { op, .. } => match op {
                SetOperator::Union | SetOperator::Except => {
                    Affix::Infix(Precedence(10), Associativity::Left)
                }
                SetOperator::Intersect => Affix::Infix(Precedence(20), Associativity::Left),
            },
            SetOperationElement::With(_) => Affix::Prefix(Precedence(5)),
            SetOperationElement::OrderBy { .. } => Affix::Postfix(Precedence(5)),
            SetOperationElement::Limit { .. } => Affix::Postfix(Precedence(5)),
            SetOperationElement::Offset { .. } => Affix::Postfix(Precedence(5)),
            SetOperationElement::IgnoreResult => Affix::Postfix(Precedence(5)),
            _ => Affix::Nilfix,
        };
        Ok(affix)
    }

    fn primary(&mut self, input: Self::Input) -> Result<Self::Output, &'static str> {
        let set_expr = match input.elem {
            SetOperationElement::Group(expr) => expr,
            SetOperationElement::SelectStmt {
                hints,
                distinct,
                select_list,
                from,
                selection,
                group_by,
                having,
                window_list,
            } => SetExpr::Select(Box::new(SelectStmt {
                span: transform_span(input.span.0),
                hints,
                distinct,
                select_list: *select_list,
                from: *from,
                selection: *selection,
                group_by,
                having: *having,
                window_list,
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
    ) -> Result<Self::Output, &'static str> {
        let set_expr = match input.elem {
            SetOperationElement::SetOperation { op, all, .. } => {
                SetExpr::SetOperation(Box::new(SetOperation {
                    span: transform_span(input.span.0),
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

    fn prefix(&mut self, op: Self::Input, rhs: Self::Output) -> Result<Self::Output, Self::Error> {
        let mut query = rhs.into_query();
        match op.elem {
            SetOperationElement::With(with) => {
                if query.with.is_some() {
                    return Err("duplicated WITH clause");
                }
                query.with = Some(with);
            }
            _ => unreachable!(),
        }
        Ok(SetExpr::Query(Box::new(query)))
    }

    fn postfix(&mut self, lhs: Self::Output, op: Self::Input) -> Result<Self::Output, Self::Error> {
        let mut query = lhs.into_query();
        match op.elem {
            SetOperationElement::OrderBy { order_by } => {
                if !query.order_by.is_empty() {
                    return Err("duplicated ORDER BY clause");
                }
                if !query.limit.is_empty() {
                    return Err("ORDER BY must appear before LIMIT");
                }
                if query.offset.is_some() {
                    return Err("ORDER BY must appear before OFFSET");
                }
                query.order_by = order_by;
            }
            SetOperationElement::Limit { limit } => {
                if !query.limit.is_empty() {
                    return Err("duplicated LIMIT clause");
                }
                if query.offset.is_some() {
                    return Err("LIMIT must appear before OFFSET");
                }
                query.limit = limit;
            }
            SetOperationElement::Offset { offset } => {
                if query.offset.is_some() {
                    return Err("duplicated OFFSET clause");
                }
                query.offset = Some(offset);
            }
            SetOperationElement::IgnoreResult => {
                query.ignore_result = true;
            }
            _ => unreachable!(),
        }
        Ok(SetExpr::Query(Box::new(query)))
    }
}

pub fn with(i: Input) -> IResult<With> {
    let cte = map(
        consumed(rule! {
            #table_alias ~ AS ~ "(" ~ #query ~ ")"
        }),
        |(span, (table_alias, _, _, query, _))| CTE {
            span: transform_span(span.0),
            alias: table_alias,
            query,
        },
    );

    map(
        consumed(rule! {
            WITH ~ RECURSIVE? ~ ^#comma_separated_list1(cte)
        }),
        |(span, (_, recursive, ctes))| With {
            span: transform_span(span.0),
            recursive: recursive.is_some(),
            ctes,
        },
    )(i)
}

pub fn exclude_col(i: Input) -> IResult<Vec<Identifier>> {
    let var = map(
        rule! {
            #ident
        },
        |col| vec![col],
    );
    let vars = map(
        rule! {
             "(" ~ ^#comma_separated_list1(ident) ~ ^")"
        },
        |(_, cols, _)| cols,
    );

    rule!(
        #var
        | #vars
    )(i)
}

pub fn select_target(i: Input) -> IResult<SelectTarget> {
    let qualified_wildcard = map(
        rule! {
            ( #ident ~ "." ~ ( #ident ~ "." )? )? ~ "*" ~ ( EXCLUDE ~ #exclude_col )?
        },
        |(res, star, opt_exclude)| {
            let exclude = opt_exclude.map(|(_, exclude)| exclude);
            match res {
                Some((fst, _, Some((snd, _)))) => SelectTarget::QualifiedName {
                    qualified: vec![
                        Indirection::Identifier(fst),
                        Indirection::Identifier(snd),
                        Indirection::Star(Some(star.span)),
                    ],
                    exclude,
                },
                Some((fst, _, None)) => SelectTarget::QualifiedName {
                    qualified: vec![
                        Indirection::Identifier(fst),
                        Indirection::Star(Some(star.span)),
                    ],
                    exclude,
                },
                None => SelectTarget::QualifiedName {
                    qualified: vec![Indirection::Star(Some(star.span))],
                    exclude,
                },
            }
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

pub fn travel_point(i: Input) -> IResult<TimeTravelPoint> {
    let at_snapshot = map(
        rule! { "(" ~ SNAPSHOT ~ "=>" ~ #literal_string ~ ")" },
        |(_, _, _, s, _)| TimeTravelPoint::Snapshot(s),
    );
    let at_timestamp = map(
        rule! { "(" ~ TIMESTAMP ~ "=>" ~ #expr ~ ")" },
        |(_, _, _, e, _)| TimeTravelPoint::Timestamp(Box::new(e)),
    );

    rule!(
        #at_snapshot | #at_timestamp
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
    map(
        rule! { #alias_name ~ ( "(" ~ ^#comma_separated_list1(ident) ~ ")")? },
        |(name, opt_columns)| TableAlias {
            name,
            columns: opt_columns.map(|(_, cols, _)| cols).unwrap_or_default(),
        },
    )(i)
}

pub fn join_operator(i: Input) -> IResult<JoinOperator> {
    alt((
        value(JoinOperator::Inner, rule! { INNER }),
        value(JoinOperator::LeftSemi, rule! { LEFT? ~ SEMI }),
        value(JoinOperator::RightSemi, rule! { RIGHT ~ SEMI }),
        value(JoinOperator::LeftAnti, rule! { LEFT? ~ ANTI }),
        value(JoinOperator::RightAnti, rule! { RIGHT ~ ANTI }),
        value(JoinOperator::LeftOuter, rule! { LEFT ~ OUTER? }),
        value(JoinOperator::RightOuter, rule! { RIGHT ~ OUTER? }),
        value(JoinOperator::FullOuter, rule! { FULL ~ OUTER? }),
        value(JoinOperator::CrossJoin, rule! { CROSS }),
    ))(i)
}

pub fn order_by_expr(i: Input) -> IResult<OrderByExpr> {
    let nulls_first = map(
        rule! {
            NULLS ~ ( FIRST | LAST )
        },
        |(_, first_last)| first_last.kind == FIRST,
    );

    map(
        rule! {
            #expr ~ ( ASC | DESC )? ~ #nulls_first?
        },
        |(expr, opt_asc, opt_nulls_first)| OrderByExpr {
            expr,
            asc: opt_asc.map(|asc| asc.kind == ASC),
            nulls_first: opt_nulls_first,
        },
    )(i)
}

pub fn table_reference(i: Input) -> IResult<TableReference> {
    let (rest, table_reference_elements) = rule!(#table_reference_element+)(i)?;
    let iter = &mut table_reference_elements.into_iter();
    run_pratt_parser(TableReferenceParser, iter, rest, i)
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableFunctionParam {
    // func(name => arg)
    Named { name: String, value: Expr },
    // func(arg)
    Normal(Expr),
}

pub fn table_function_param(i: Input) -> IResult<TableFunctionParam> {
    let named = map(rule! { #ident ~ "=>" ~ #expr  }, |(name, _, value)| {
        TableFunctionParam::Named {
            name: name.to_string(),
            value,
        }
    });
    let normal = map(rule! { #expr }, TableFunctionParam::Normal);

    rule!(
        #named | #normal
    )(i)
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableReferenceElement {
    Table {
        catalog: Option<Identifier>,
        database: Option<Identifier>,
        table: Identifier,
        alias: Option<TableAlias>,
        travel_point: Option<TimeTravelPoint>,
        pivot: Option<Box<Pivot>>,
        unpivot: Option<Box<Unpivot>>,
    },
    // `TABLE(expr)[ AS alias ]`
    TableFunction {
        name: Identifier,
        params: Vec<TableFunctionParam>,
        alias: Option<TableAlias>,
    },
    // Derived table, which can be a subquery or joined tables or combination of them
    Subquery {
        subquery: Box<Query>,
        alias: Option<TableAlias>,
    },
    // [NATURAL] [INNER|OUTER|CROSS|...] JOIN
    Join {
        op: JoinOperator,
        natural: bool,
    },
    // ON expr | USING (ident, ...)
    JoinCondition(JoinCondition),
    Group(TableReference),
    Stage {
        location: FileLocation,
        options: Vec<SelectStageOption>,
        alias: Option<TableAlias>,
    },
}

pub fn table_reference_element(i: Input) -> IResult<WithSpan<TableReferenceElement>> {
    // PIVOT(expr FOR col IN (ident, ...))
    let pivot = map(
        rule! {
           PIVOT ~ "(" ~ #expr ~ FOR ~ #ident ~ IN ~ "(" ~ #comma_separated_list1(expr) ~ ")" ~ ")"
        },
        |(_pivot, _, aggregate, _for, value_column, _in, _, values, _, _)| Pivot {
            aggregate,
            value_column,
            values,
        },
    );
    // UNPIVOT(ident for ident IN (ident, ...))
    let unpivot = map(
        rule! {
            UNPIVOT ~ "(" ~ #ident ~ FOR ~ #ident ~ IN ~ "(" ~ #comma_separated_list1(ident) ~ ")" ~ ")"
        },
        |(_unpivot, _, value_column, _for, column_name, _in, _, names, _, _)| Unpivot {
            value_column,
            column_name,
            names,
        },
    );
    let aliased_table = map(
        rule! {
            #period_separated_idents_1_to_3 ~ (AT ~ #travel_point)? ~ #table_alias? ~ #pivot? ~ #unpivot?
        },
        |((catalog, database, table), travel_point_opt, alias, pivot, unpivot)| {
            TableReferenceElement::Table {
                catalog,
                database,
                table,
                alias,
                travel_point: travel_point_opt.map(|p| p.1),
                pivot: pivot.map(Box::new),
                unpivot: unpivot.map(Box::new),
            }
        },
    );
    let join = map(
        rule! {
            NATURAL? ~ #join_operator? ~ JOIN
        },
        |(opt_natural, opt_op, _)| TableReferenceElement::Join {
            op: opt_op.unwrap_or(JoinOperator::Inner),
            natural: opt_natural.is_some(),
        },
    );
    let join_condition_on = map(
        rule! {
            ON ~ #expr
        },
        |(_, expr)| TableReferenceElement::JoinCondition(JoinCondition::On(Box::new(expr))),
    );
    let join_condition_using = map(
        rule! {
            USING ~ "(" ~ #comma_separated_list1(ident) ~ ")"
        },
        |(_, _, idents, _)| TableReferenceElement::JoinCondition(JoinCondition::Using(idents)),
    );
    let table_function = map(
        rule! {
            #function_name ~ "(" ~ #comma_separated_list0(table_function_param) ~ ")" ~ #table_alias?
        },
        |(name, _, params, _, alias)| TableReferenceElement::TableFunction {
            name,
            params,
            alias,
        },
    );
    let subquery = map(
        rule! {
            "(" ~ #query ~ ")" ~ #table_alias?
        },
        |(_, subquery, _, alias)| TableReferenceElement::Subquery {
            subquery: Box::new(subquery),
            alias,
        },
    );

    let group = map(
        rule! {
           "(" ~ #table_reference ~ ^")"
        },
        |(_, table_ref, _)| TableReferenceElement::Group(table_ref),
    );
    let stage_location = |i| map(stage_location, FileLocation::Stage)(i);
    let uri_location = |i| map(literal_string, FileLocation::Uri)(i);
    let aliased_stage = map(
        rule! {
            (#stage_location | #uri_location) ~  ("(" ~ ^#comma_separated_list1(select_stage_option) ~")")? ~ #table_alias?
        },
        |(location, options, alias)| {
            let options = match options {
                None => vec![],
                Some((_, v, _)) => v,
            };
            TableReferenceElement::Stage {
                location,
                alias,
                options,
            }
        },
    );

    let (rest, (span, elem)) = consumed(rule! {
        #aliased_stage
        | #table_function
        | #aliased_table
        | #subquery
        | #group
        | #join
        | #join_condition_on
        | #join_condition_using
    })(i)?;
    Ok((rest, WithSpan { span, elem }))
}

struct TableReferenceParser;

impl<'a, I: Iterator<Item = WithSpan<'a, TableReferenceElement>>> PrattParser<I>
    for TableReferenceParser
{
    type Error = &'static str;
    type Input = WithSpan<'a, TableReferenceElement>;
    type Output = TableReference;

    fn query(&mut self, input: &Self::Input) -> Result<Affix, &'static str> {
        let affix = match &input.elem {
            TableReferenceElement::Join { .. } => Affix::Infix(Precedence(10), Associativity::Left),
            TableReferenceElement::JoinCondition(..) => Affix::Postfix(Precedence(5)),
            _ => Affix::Nilfix,
        };
        Ok(affix)
    }

    fn primary(&mut self, input: Self::Input) -> Result<Self::Output, &'static str> {
        let table_ref = match input.elem {
            TableReferenceElement::Group(table_ref) => table_ref,
            TableReferenceElement::Table {
                catalog,
                database,
                table,
                alias,
                travel_point,
                pivot,
                unpivot,
            } => TableReference::Table {
                span: transform_span(input.span.0),
                catalog,
                database,
                table,
                alias,
                travel_point,
                pivot,
                unpivot,
            },
            TableReferenceElement::TableFunction {
                name,
                params,
                alias,
            } => {
                let normal_params = params
                    .iter()
                    .filter_map(|p| match p {
                        TableFunctionParam::Normal(p) => Some(p.clone()),
                        _ => None,
                    })
                    .collect();
                let named_params = params
                    .into_iter()
                    .filter_map(|p| match p {
                        TableFunctionParam::Named { name, value } => Some((name, value)),
                        _ => None,
                    })
                    .collect();
                TableReference::TableFunction {
                    span: transform_span(input.span.0),
                    name,
                    params: normal_params,
                    named_params,
                    alias,
                }
            }
            TableReferenceElement::Subquery { subquery, alias } => TableReference::Subquery {
                span: transform_span(input.span.0),
                subquery,
                alias,
            },
            TableReferenceElement::Stage {
                location,
                options,
                alias,
            } => {
                let options = SelectStageOptions::from(options);
                TableReference::Stage {
                    span: transform_span(input.span.0),
                    location,
                    options,
                    alias,
                }
            }
            _ => unreachable!(),
        };
        Ok(table_ref)
    }

    fn infix(
        &mut self,
        lhs: Self::Output,
        input: Self::Input,
        rhs: Self::Output,
    ) -> Result<Self::Output, &'static str> {
        let table_ref = match input.elem {
            TableReferenceElement::Join { op, natural } => {
                let condition = if natural {
                    JoinCondition::Natural
                } else {
                    JoinCondition::None
                };
                TableReference::Join {
                    span: transform_span(input.span.0),
                    join: Join {
                        op,
                        condition,
                        left: Box::new(lhs),
                        right: Box::new(rhs),
                    },
                }
            }
            _ => unreachable!(),
        };
        Ok(table_ref)
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
        mut lhs: Self::Output,
        op: Self::Input,
    ) -> Result<Self::Output, Self::Error> {
        match op.elem {
            TableReferenceElement::JoinCondition(new_condition) => match &mut lhs {
                TableReference::Join {
                    join: Join { condition, .. },
                    ..
                } => match *condition {
                    JoinCondition::None => {
                        *condition = new_condition;
                        Ok(lhs)
                    }
                    JoinCondition::Natural => Err("join condition conflicting with NATURAL"),
                    _ => Err("join condition already set"),
                },
                _ => Err("join condition must apply to a join"),
            },
            _ => unreachable!(),
        }
    }
}

pub fn group_by_items(i: Input) -> IResult<GroupBy> {
    let normal = map(rule! { ^#comma_separated_list1(expr) }, |groups| {
        GroupBy::Normal(groups)
    });
    let cube = map(
        rule! { CUBE ~ "(" ~ ^#comma_separated_list1(expr) ~ ")" },
        |(_, _, groups, _)| GroupBy::Cube(groups),
    );
    let rollup = map(
        rule! { ROLLUP ~ "(" ~ ^#comma_separated_list1(expr) ~ ")" },
        |(_, _, groups, _)| GroupBy::Rollup(groups),
    );
    let group_set = alt((
        map(rule! {"(" ~ ")"}, |(_, _)| vec![]), // empty grouping set
        map(
            rule! {"(" ~ #comma_separated_list1(expr) ~ ")"},
            |(_, sets, _)| sets,
        ),
        map(rule! { #expr }, |e| vec![e]),
    ));
    let group_sets = map(
        rule! { GROUPING ~ SETS ~ "(" ~ ^#comma_separated_list1(group_set) ~ ")"  },
        |(_, _, _, sets, _)| GroupBy::GroupingSets(sets),
    );
    rule!(#group_sets | #cube | #rollup | #normal)(i)
}

pub fn window_frame_bound(i: Input) -> IResult<WindowFrameBound> {
    alt((
        value(WindowFrameBound::CurrentRow, rule! { CURRENT ~ ROW }),
        value(
            WindowFrameBound::Preceding(None),
            rule! { UNBOUNDED ~ PRECEDING },
        ),
        map(rule! { #subexpr(0) ~ PRECEDING }, |(expr, _)| {
            WindowFrameBound::Preceding(Some(Box::new(expr)))
        }),
        value(
            WindowFrameBound::Following(None),
            rule! { UNBOUNDED ~ FOLLOWING },
        ),
        map(rule! { #subexpr(0) ~ FOLLOWING }, |(expr, _)| {
            WindowFrameBound::Following(Some(Box::new(expr)))
        }),
    ))(i)
}

pub fn window_frame_between(i: Input) -> IResult<(WindowFrameBound, WindowFrameBound)> {
    alt((
        map(
            rule! { BETWEEN ~ #window_frame_bound ~ AND ~ #window_frame_bound },
            |(_, s, _, e)| (s, e),
        ),
        map(rule! {#window_frame_bound}, |s| {
            (s, WindowFrameBound::CurrentRow)
        }),
    ))(i)
}

pub fn window_spec(i: Input) -> IResult<WindowSpec> {
    map(
        rule! {
            (#ident )? ~ (PARTITION ~ ^BY ~ #comma_separated_list1(subexpr(0)))?
            ~ ( ORDER ~ ^BY ~ ^#comma_separated_list1(order_by_expr) )?
            ~ ((ROWS | RANGE) ~ #window_frame_between)?
        },
        |(existing_window_name, opt_partition, opt_order, between)| WindowSpec {
            existing_window_name,
            partition_by: opt_partition.map(|x| x.2).unwrap_or_default(),
            order_by: opt_order.map(|x| x.2).unwrap_or_default(),
            window_frame: between.map(|x| {
                let unit = match x.0.kind {
                    ROWS => WindowFrameUnits::Rows,
                    RANGE => WindowFrameUnits::Range,
                    _ => unreachable!(),
                };
                let bw = x.1;
                WindowFrame {
                    units: unit,
                    start_bound: bw.0,
                    end_bound: bw.1,
                }
            }),
        },
    )(i)
}

pub fn window_spec_ident(i: Input) -> IResult<Window> {
    alt((
        map(
            rule! {
               ("(" ~ #window_spec ~ ")")
            },
            |(_, spec, _)| Window::WindowSpec(spec),
        ),
        map(rule! {#ident}, |window_name| {
            Window::WindowReference(WindowRef { window_name })
        }),
    ))(i)
}

pub fn window_clause(i: Input) -> IResult<WindowDefinition> {
    map(
        rule! {
            #ident ~ (AS ~ "(" ~ #window_spec ~ ")")
        },
        |(ident, window)| WindowDefinition {
            name: ident,
            spec: window.2,
        },
    )(i)
}
