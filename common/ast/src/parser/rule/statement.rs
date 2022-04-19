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
use nom::combinator::cut;
use nom::combinator::map;
use nom::combinator::value;

use crate::parser::ast::*;
use crate::parser::rule::expr::expr;
use crate::parser::rule::util::*;
use crate::parser::token::*;
use crate::rule;

pub fn statement(i: Input) -> IResult<Statement> {
    let truncate_table = map(
        rule! {
            TRUNCATE ~ TABLE ~ ( #ident ~ "." )? ~ #ident ~ ";"
        },
        |(_, _, database, table, _)| Statement::TruncateTable {
            database: database.map(|(name, _)| name),
            table,
        },
    );
    let drop_table = map(
        rule! {
            DROP ~ TABLE ~ ( IF ~ EXISTS )? ~ ( #ident ~ "." )? ~ #ident ~ ";"
        },
        |(_, _, if_exists, database, table, _)| Statement::DropTable {
            if_exists: if_exists.is_some(),
            database: database.map(|(name, _)| name),
            table,
        },
    );

    rule!(
        #truncate_table : "TRUNCATE TABLE statement"
        | #drop_table : "DROP TABLE statement"
    )(i)
}

pub fn query(i: Input) -> IResult<Query> {
    map(
        rule! {
            SELECT ~ DISTINCT? ~ #comma_separated_list1(select_target)
            ~ FROM ~ #comma_separated_list1(table_reference)
            ~ ( WHERE ~ #expr )?
            ~ ( GROUP ~ BY ~ #comma_separated_list1(expr) )?
            ~ ( HAVING ~ #expr )?
            ~ ( ORDER ~ BY ~ #comma_separated_list1(order_by_expr) )?
            ~ ( LIMIT ~ #comma_separated_list1(expr) )?
            : "SELECT statement"
        },
        |(
            _select,
            distinct,
            select_list,
            _from,
            table_refs,
            where_block,
            group_by_block,
            having_block,
            order_by_block,
            limit_block,
        )| {
            let from = table_refs
                .into_iter()
                .reduce(|left, right| {
                    TableReference::Join(Join {
                        op: JoinOperator::CrossJoin,
                        condition: JoinCondition::None,
                        left: Box::new(left),
                        right: Box::new(right),
                    })
                })
                .unwrap();

            Query {
                body: SetExpr::Select(Box::new(SelectStmt {
                    distinct: distinct.is_some(),
                    select_list,
                    from,
                    selection: where_block.map(|(_, selection)| selection),
                    group_by: group_by_block
                        .map(|(_, _, group_by)| group_by)
                        .unwrap_or_default(),
                    having: having_block.map(|(_, having)| having),
                })),
                order_by: order_by_block
                    .map(|(_, _, order_by)| order_by)
                    .unwrap_or_default(),
                limit: limit_block.map(|(_, limit)| limit).unwrap_or_default(),
            }
        },
    )(i)
}

pub fn select_target(i: Input) -> IResult<SelectTarget> {
    let qualified_wildcard = map(
        rule! {
            ( #ident ~ "." ~ ( #ident ~ "." )? )? ~ "*"
        },
        |(res, _)| match res {
            Some((fst, _, Some((snd, _)))) => SelectTarget::Indirections(vec![
                Indirection::Identifier(fst),
                Indirection::Identifier(snd),
                Indirection::Star,
            ]),
            Some((fst, _, None)) => {
                SelectTarget::Indirections(vec![Indirection::Identifier(fst), Indirection::Star])
            }
            None => SelectTarget::Indirections(vec![Indirection::Star]),
        },
    );
    let projection = map(
        rule! {
            #expr ~ ( AS ~ #ident )?
        },
        |(expr, alias)| SelectTarget::Projection {
            expr,
            alias: alias.map(|(_, name)| name),
        },
    );

    rule!(
        #qualified_wildcard
        | #projection
    )(i)
}

pub fn table_reference(i: Input) -> IResult<TableReference> {
    rule! (
        #joined_tables
        | #parenthesized_joined_tables
        | #subquery
        | #aliased_table
    )(i)
}

pub fn aliased_table(i: Input) -> IResult<TableReference> {
    let table_alias = map(rule! { #ident }, |name| TableAlias {
        name,
        columns: vec![],
    });

    map(
        rule! {
            #ident ~ ( "." ~ #ident )? ~ ( AS? ~ #table_alias )?
        },
        |(fst, snd, alias)| {
            let (database, table) = match (fst, snd) {
                (database, Some((_, table))) => (Some(database), table),
                (table, None) => (None, table),
            };

            TableReference::Table {
                database,
                table,
                alias: alias.map(|(_, alias)| alias),
            }
        },
    )(i)
}

pub fn table_alias(i: Input) -> IResult<TableAlias> {
    map(rule! { #ident }, |name| TableAlias {
        name,
        columns: vec![],
    })(i)
}

pub fn subquery(i: Input) -> IResult<TableReference> {
    map(
        rule! {
            ( #parenthesized_query | #query ) ~ AS? ~ #table_alias
        },
        |(subquery, _, alias)| TableReference::Subquery {
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
    struct JoinElement {
        op: JoinOperator,
        condition: JoinCondition,
        right: Box<TableReference>,
    }

    let table_ref_without_join =
        |i| rule! { #aliased_table | #subquery | #parenthesized_joined_tables }(i);
    let join_condition_on = map(
        rule! {
            ON ~ #expr
        },
        |(_, expr)| JoinCondition::On(expr),
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
            #join_operator? ~ JOIN ~ #cut(table_ref_without_join) ~ #cut(join_condition)
        },
        |(op, _, right, condition)| JoinElement {
            op: op.unwrap_or(JoinOperator::Inner),
            condition,
            right: Box::new(right),
        },
    );
    let natural_join = map(
        rule! {
            NATURAL ~ #cut(rule! { JOIN }) ~ #cut(table_ref_without_join)
        },
        |(_, _, right)| JoinElement {
            op: JoinOperator::Inner,
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
        value(JoinOperator::LeftOuter, rule! { LEFT ~ OUTER }),
        value(JoinOperator::RightOuter, rule! { RIGHT ~ OUTER }),
        value(JoinOperator::FullOuter, rule! { FULL ~ OUTER }),
    ))(i)
}

pub fn order_by_expr(i: Input) -> IResult<OrderByExpr> {
    map(
        rule! {
            #expr ~ ( ASC | DESC )?
        },
        |(expr, asc)| OrderByExpr {
            expr,
            asc: asc.map(|asc| asc.kind == ASC),
            nulls_first: None,
        },
    )(i)
}
