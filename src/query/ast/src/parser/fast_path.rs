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

use std::collections::BTreeMap;

use crate::Span;
use crate::ast::*;
use crate::parser::Error;
use crate::parser::common::transform_span;
use crate::parser::expr::parse_float;
use crate::parser::expr::parse_simple_string_literal;
use crate::parser::expr::parse_uint;
use crate::parser::expr::simple_expr_fast_path;
use crate::parser::expr::simple_function_call_fast_path;
use crate::parser::expr::subexpr;
use crate::parser::input::Input;
use crate::parser::token::*;

pub(crate) fn should_try_statement(tokens: &[Token]) -> bool {
    match tokens.first().map(|token| token.kind) {
        Some(SELECT) => should_try_select_query(tokens),
        Some(ALTER | CALL | CREATE | DELETE | DROP | INSERT | SET | SHOW | UPDATE) => true,
        _ => false,
    }
}

pub(crate) fn should_try_select_query(tokens: &[Token]) -> bool {
    let mut previous_from = false;
    for token in tokens {
        match token.kind {
            UNION | EXCEPT | INTERSECT => return false,
            LParen if previous_from => return false,
            FROM => previous_from = true,
            _ => previous_from = false,
        }
    }
    true
}

pub(crate) fn query(i: Input) -> std::result::Result<Option<(Input, Query)>, nom::Err<Error>> {
    if let Some((rest, query)) = select_from_query(i)? {
        return Ok(Some((rest, query)));
    }
    select_function_query(i)
}

fn select_from_query(i: Input) -> std::result::Result<Option<(Input, Query)>, nom::Err<Error>> {
    let mut rest = i.advance(1);
    let (next, select_list) = match simple_select_targets(rest)? {
        Some(res) => res,
        None => return Ok(None),
    };
    rest = next;
    if matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        let consumed = i.tokens.len() - rest.tokens.len();
        let query_span = transform_span(&i.tokens[..consumed]);
        return Ok(Some((
            rest,
            build_simple_select_query(query_span, select_list, vec![], None, vec![], vec![]),
        )));
    }

    if rest.tokens.first().map(|token| token.kind) != Some(FROM) {
        return Ok(None);
    }
    let (next, table) = match simple_table_reference(rest.advance(1)) {
        Some(res) => res,
        None => return Ok(None),
    };
    rest = next;

    let mut selection = None;
    if rest.tokens.first().map(|token| token.kind) == Some(WHERE) {
        let (next, expr) = match simple_where_expr(rest.advance(1)) {
            Some(res) => res,
            None => return Ok(None),
        };
        rest = next;
        selection = Some(expr);
    }

    let mut order_by = Vec::new();
    if matches!(
        (
            rest.tokens.first().map(|token| token.kind),
            rest.tokens.get(1).map(|token| token.kind),
        ),
        (Some(ORDER), Some(BY))
    ) {
        let (next, order) = match simple_order_by(rest.advance(2)) {
            Some(res) => res,
            None => return Ok(None),
        };
        rest = next;
        order_by = order;
    }

    let mut limit = Vec::new();
    if rest.tokens.first().map(|token| token.kind) == Some(LIMIT) {
        let (next, expr) = match simple_limit(rest.advance(1)) {
            Some(res) => res,
            None => return Ok(None),
        };
        rest = next;
        limit.push(expr);
    }

    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return Ok(None);
    }

    let consumed = i.tokens.len() - rest.tokens.len();
    let query_span = transform_span(&i.tokens[..consumed]);
    Ok(Some((
        rest,
        build_simple_select_query(
            query_span,
            select_list,
            vec![table],
            selection,
            order_by,
            limit,
        ),
    )))
}

fn build_simple_select_query(
    span: Span,
    select_list: Vec<SelectTarget>,
    from: Vec<TableReference>,
    selection: Option<Expr>,
    order_by: Vec<OrderByExpr>,
    limit: Vec<Expr>,
) -> Query {
    Query {
        span,
        with: None,
        body: SetExpr::Select(Box::new(SelectStmt {
            span,
            hints: None,
            distinct: false,
            top_n: None,
            select_list,
            from,
            selection,
            group_by: None,
            having: None,
            window_list: None,
            qualify: None,
        })),
        order_by,
        limit,
        offset: None,
        ignore_result: false,
    }
}

fn select_function_query(i: Input) -> std::result::Result<Option<(Input, Query)>, nom::Err<Error>> {
    let func_start = i.advance(1);
    let Some((rest, func)) = simple_function_call_fast_path(func_start)? else {
        return Ok(None);
    };
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return Ok(None);
    }

    let consumed = i.tokens.len() - rest.tokens.len();
    let func_consumed = func_start.tokens.len() - rest.tokens.len();
    let func_span = transform_span(&func_start.tokens[..func_consumed]);
    let query_span = transform_span(&i.tokens[..consumed]);
    Ok(Some((rest, Query {
        span: query_span,
        with: None,
        body: SetExpr::Select(Box::new(SelectStmt {
            span: query_span,
            hints: None,
            distinct: false,
            top_n: None,
            select_list: vec![SelectTarget::AliasedExpr {
                expr: Box::new(Expr::FunctionCall {
                    span: func_span,
                    func,
                }),
                alias: None,
            }],
            from: vec![],
            selection: None,
            group_by: None,
            having: None,
            window_list: None,
            qualify: None,
        })),
        order_by: vec![],
        limit: vec![],
        offset: None,
        ignore_result: false,
    })))
}

fn simple_select_targets(
    i: Input,
) -> std::result::Result<Option<(Input, Vec<SelectTarget>)>, nom::Err<Error>> {
    let Some((mut rest, first_target)) = simple_select_target(i)? else {
        return Ok(None);
    };
    if matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(FROM | EOI | SemiColon | FORMAT)
    ) {
        return Ok(Some((rest, vec![first_target])));
    }
    if rest.tokens.first().map(|token| token.kind) != Some(Comma) {
        return Ok(None);
    }

    let mut targets = Vec::with_capacity(2);
    targets.push(first_target);
    rest = rest.advance(1);
    loop {
        let Some((next, target)) = simple_select_target(rest)? else {
            return Ok(None);
        };
        targets.push(target);
        rest = next;
        match rest.tokens.first().map(|token| token.kind) {
            Some(Comma) => rest = rest.advance(1),
            Some(FROM | EOI | SemiColon | FORMAT) => return Ok(Some((rest, targets))),
            _ => return Ok(None),
        }
    }
}

fn simple_select_target(
    i: Input,
) -> std::result::Result<Option<(Input, SelectTarget)>, nom::Err<Error>> {
    if i.tokens.first().is_some_and(|token| token.kind == COLUMNS) {
        return Ok(None);
    }

    if let Some(star) = i.tokens.first().filter(|token| token.kind == Multiply) {
        return Ok(Some((i.advance(1), SelectTarget::StarColumns {
            qualified: vec![Indirection::Star(Some(star.span))],
            column_filter: None,
        })));
    }

    if i.tokens.first().is_some_and(|token| token.kind == COUNT)
        && i.tokens.get(1).is_some_and(|token| token.kind == LParen)
        && let Some((rest, expr)) = simple_count_expr_fast_path(i)?
    {
        let (rest, alias) = simple_select_alias(rest);
        return Ok(Some((rest, SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias,
        })));
    }

    if i.tokens.first().is_some_and(|token| token.kind == Ident)
        && i.tokens.get(1).is_some_and(|token| token.kind == LParen)
        && let Some((rest, func)) = simple_function_call_fast_path(i)?
    {
        let consumed = i.tokens.len() - rest.tokens.len();
        let span = transform_span(&i.tokens[..consumed]);
        let (rest, alias) = simple_select_alias(rest);
        return Ok(Some((rest, SelectTarget::AliasedExpr {
            expr: Box::new(Expr::FunctionCall { span, func }),
            alias,
        })));
    }

    if maybe_simple_binary_select_expr(i)
        && let Some((rest, expr)) = simple_binary_select_expr(i)?
    {
        let (rest, alias) = simple_select_alias(rest);
        return Ok(Some((rest, SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias,
        })));
    }

    if let Some((rest, expr)) = simple_column_expr(i) {
        let (rest, alias) = simple_select_alias(rest);
        return Ok(Some((rest, SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias,
        })));
    }

    if let Some((rest, expr)) = simple_expr_fast_path(i)? {
        let (rest, alias) = simple_select_alias(rest);
        return Ok(Some((rest, SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias,
        })));
    }

    let Ok((rest, expr)) = subexpr(0)(i) else {
        return Ok(None);
    };
    let (rest, alias) = simple_select_alias(rest);
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(Comma | FROM | EOI | SemiColon | FORMAT)
    ) {
        return Ok(None);
    }
    Ok(Some((rest, SelectTarget::AliasedExpr {
        expr: Box::new(expr),
        alias,
    })))
}

fn maybe_simple_binary_select_expr(i: Input) -> bool {
    matches!(
        i.tokens.get(1).map(|token| token.kind),
        Some(Plus | Minus | Multiply | Divide | IntDiv | Modulo)
    ) || matches!(
        (
            i.tokens.first().map(|token| token.kind),
            i.tokens.get(1).map(|token| token.kind),
            i.tokens.get(2).map(|token| token.kind),
            i.tokens.get(3).map(|token| token.kind),
        ),
        (
            Some(Ident),
            Some(Dot),
            Some(Ident),
            Some(Plus | Minus | Multiply | Divide | IntDiv | Modulo)
        )
    ) || matches!(
        (
            i.tokens.first().map(|token| token.kind),
            i.tokens.get(1).map(|token| token.kind),
            i.tokens.get(2).map(|token| token.kind),
            i.tokens.get(3).map(|token| token.kind),
            i.tokens.get(4).map(|token| token.kind),
            i.tokens.get(5).map(|token| token.kind),
        ),
        (
            Some(Ident),
            Some(Dot),
            Some(Ident),
            Some(Dot),
            Some(Ident),
            Some(Plus | Minus | Multiply | Divide | IntDiv | Modulo)
        )
    )
}

fn simple_binary_select_expr(
    i: Input,
) -> std::result::Result<Option<(Input, Expr)>, nom::Err<Error>> {
    let Some((rest, left)) = simple_column_expr(i).or_else(|| simple_literal_expr(i)) else {
        return Ok(None);
    };
    let Some(token) = rest.tokens.first() else {
        return Ok(None);
    };
    let op = match token.kind {
        Plus => BinaryOperator::Plus,
        Minus => BinaryOperator::Minus,
        Multiply => BinaryOperator::Multiply,
        Divide => BinaryOperator::Divide,
        IntDiv => BinaryOperator::IntDiv,
        Modulo => BinaryOperator::Modulo,
        _ => return Ok(None),
    };
    let Some((rest, right)) = simple_expr_fast_path(rest.advance(1))? else {
        return Ok(None);
    };
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(Comma | FROM | EOI | SemiColon | FORMAT | AS | Ident)
    ) {
        return Ok(None);
    }

    Ok(Some((rest, Expr::BinaryOp {
        span: Some(token.span),
        op,
        left: Box::new(left),
        right: Box::new(right),
    })))
}

fn simple_count_expr_fast_path(
    i: Input,
) -> std::result::Result<Option<(Input, Expr)>, nom::Err<Error>> {
    let Some(count_token) = i.tokens.first().filter(|token| token.kind == COUNT) else {
        return Ok(None);
    };
    if i.tokens.get(1).map(|token| token.kind) != Some(LParen) {
        return Ok(None);
    }

    if let Some(star) = i.tokens.get(2).filter(|token| token.kind == Multiply) {
        let rest = i.advance(3);
        if rest.tokens.first().map(|token| token.kind) != Some(RParen) {
            return Ok(None);
        }
        let rest = rest.advance(1);
        if !matches!(
            rest.tokens.first().map(|token| token.kind),
            Some(Comma | FROM | EOI | SemiColon | FORMAT | AS | Ident)
        ) {
            return Ok(None);
        }
        return Ok(Some((rest, Expr::CountAll {
            span: transform_span(&i.tokens[..4]),
            qualified: vec![Indirection::Star(Some(star.span))],
            window: None,
        })));
    }

    let name = Identifier {
        span: Some(count_token.span),
        name: count_token.text().to_string(),
        quote: None,
        ident_type: IdentifierType::None,
    };
    let mut rest = i.advance(2);
    let mut args = Vec::with_capacity(1);
    if rest.tokens.first().map(|token| token.kind) != Some(RParen) {
        loop {
            let Some((next, arg)) = simple_expr_fast_path(rest)? else {
                return Ok(None);
            };
            args.push(arg);
            rest = next;
            match rest.tokens.first().map(|token| token.kind) {
                Some(Comma) => rest = rest.advance(1),
                Some(RParen) => break,
                _ => return Ok(None),
            }
        }
    }

    rest = rest.advance(1);
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(Comma | FROM | EOI | SemiColon | FORMAT | AS | Ident)
    ) {
        return Ok(None);
    }
    let consumed = i.tokens.len() - rest.tokens.len();
    Ok(Some((rest, Expr::FunctionCall {
        span: transform_span(&i.tokens[..consumed]),
        func: FunctionCall {
            distinct: false,
            name,
            args,
            params: vec![],
            order_by: vec![],
            window: None,
            lambda: None,
        },
    })))
}

fn simple_select_alias(i: Input) -> (Input, Option<Identifier>) {
    if i.tokens.first().map(|token| token.kind) == Some(AS) {
        if let Some((rest, alias)) = simple_identifier(i.advance(1)) {
            return (rest, Some(alias));
        }
        return (i, None);
    }

    match i.tokens.first().map(|token| token.kind) {
        Some(Ident) => {
            let Some((rest, alias)) = simple_identifier(i) else {
                return (i, None);
            };
            (rest, Some(alias))
        }
        _ => (i, None),
    }
}

fn simple_table_reference(i: Input) -> Option<(Input, TableReference)> {
    if let Some((rest, table)) = simple_table_function_reference(i) {
        return Some((rest, table));
    }

    let (rest, table) = simple_table_ref(i)?;
    let (rest, alias) = simple_table_alias(rest)?;
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT | WHERE | ORDER | LIMIT)
    ) {
        return None;
    }
    let consumed = i.tokens.len() - rest.tokens.len();
    Some((rest, TableReference::Table {
        span: transform_span(&i.tokens[..consumed]),
        table,
        alias,
        temporal: None,
        with_options: None,
        pivot: None,
        unpivot: None,
        sample: None,
    }))
}

fn simple_table_function_reference(i: Input) -> Option<(Input, TableReference)> {
    if !matches!(
        (
            i.tokens.first().map(|token| token.kind),
            i.tokens.get(1).map(|token| token.kind),
        ),
        (Some(Ident), Some(LParen))
    ) {
        return None;
    }

    let (rest, func) = simple_function_call_fast_path(i).ok()??;
    let (rest, alias) = simple_table_alias(rest)?;
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT | WHERE | ORDER | LIMIT)
    ) {
        return None;
    }

    let consumed = i.tokens.len() - rest.tokens.len();
    Some((rest, TableReference::TableFunction {
        span: transform_span(&i.tokens[..consumed]),
        lateral: false,
        name: func.name,
        params: func.args,
        named_params: vec![],
        alias,
        sample: None,
    }))
}

fn simple_table_alias(i: Input) -> Option<(Input, Option<TableAlias>)> {
    let mut rest = i;
    if rest.tokens.first().map(|token| token.kind) == Some(AS) {
        rest = rest.advance(1);
    }

    let Some((next, name)) = simple_identifier(rest) else {
        return Some((i, None));
    };
    rest = next;

    let mut columns = Vec::new();
    if rest.tokens.first().map(|token| token.kind) == Some(LParen) {
        columns = Vec::with_capacity(4);
        rest = rest.advance(1);
        loop {
            let (next, column) = simple_identifier(rest)?;
            columns.push(column);
            rest = next;
            match rest.tokens.first().map(|token| token.kind) {
                Some(Comma) => rest = rest.advance(1),
                Some(RParen) => {
                    rest = rest.advance(1);
                    break;
                }
                _ => return None,
            }
        }
    }

    Some((
        rest,
        Some(TableAlias {
            name,
            columns,
            keep_database_name: false,
        }),
    ))
}

fn simple_table_ref(i: Input) -> Option<(Input, TableRef)> {
    let (mut rest, ident0) = simple_identifier(i)?;
    if rest.tokens.first().map(|token| token.kind) != Some(Dot) {
        return Some((rest, TableRef {
            catalog: None,
            database: None,
            table: ident0,
            branch: None,
        }));
    }
    let (next, ident1) = simple_identifier(rest.advance(1))?;
    rest = next;
    if rest.tokens.first().map(|token| token.kind) != Some(Dot) {
        return Some((rest, TableRef {
            catalog: None,
            database: Some(ident0),
            table: ident1,
            branch: None,
        }));
    }
    let (rest, ident2) = simple_identifier(rest.advance(1))?;
    Some((rest, TableRef {
        catalog: Some(ident0),
        database: Some(ident1),
        table: ident2,
        branch: None,
    }))
}

fn simple_order_by(i: Input) -> Option<(Input, Vec<OrderByExpr>)> {
    let mut rest = i;
    let mut order_by = Vec::new();
    loop {
        let (next, expr) = simple_column_expr(rest).or_else(|| simple_literal_expr(rest))?;
        rest = next;
        let asc = match rest.tokens.first().map(|token| token.kind) {
            Some(ASC) => {
                rest = rest.advance(1);
                Some(true)
            }
            Some(DESC) => {
                rest = rest.advance(1);
                Some(false)
            }
            _ => None,
        };
        order_by.push(OrderByExpr {
            expr,
            asc,
            nulls_first: None,
        });
        match rest.tokens.first().map(|token| token.kind) {
            Some(Comma) => rest = rest.advance(1),
            Some(LIMIT | EOI | SemiColon | FORMAT) => return Some((rest, order_by)),
            _ => return None,
        }
    }
}

fn simple_limit(i: Input) -> Option<(Input, Expr)> {
    simple_literal_expr(i).and_then(|(rest, expr)| {
        if matches!(
            rest.tokens.first().map(|token| token.kind),
            Some(EOI | SemiColon | FORMAT)
        ) {
            Some((rest, expr))
        } else {
            None
        }
    })
}

fn simple_where_expr(i: Input) -> Option<(Input, Expr)> {
    let start = i;
    let (mut rest, expr) = simple_column_expr(i)?;

    let token = rest.tokens.first()?;
    if let Some(not) = match (
        rest.tokens.first().map(|token| token.kind),
        rest.tokens.get(1).map(|token| token.kind),
        rest.tokens.get(2).map(|token| token.kind),
    ) {
        (Some(IS), Some(NULL), _) => {
            rest = rest.advance(2);
            Some(false)
        }
        (Some(IS), Some(NOT), Some(NULL)) => {
            rest = rest.advance(3);
            Some(true)
        }
        _ => None,
    } {
        let consumed = start.tokens.len() - rest.tokens.len();
        return Some((rest, Expr::IsNull {
            span: transform_span(&start.tokens[..consumed]),
            expr: Box::new(expr),
            not,
        }));
    }

    let (op, value_start) = match (
        rest.tokens.first().map(|token| token.kind),
        rest.tokens.get(1).map(|token| token.kind),
    ) {
        (Some(LIKE), _) => (BinaryOperator::Like(None), rest.advance(1)),
        (Some(ILIKE), _) => (BinaryOperator::ILike(None), rest.advance(1)),
        (Some(NOT), Some(LIKE)) => (BinaryOperator::NotLike(None), rest.advance(2)),
        (Some(NOT), Some(ILIKE)) => (BinaryOperator::NotILike(None), rest.advance(2)),
        (Some(Eq | DoubleEq), _) => (BinaryOperator::Eq, rest.advance(1)),
        (Some(NotEq), _) => (BinaryOperator::NotEq, rest.advance(1)),
        (Some(Lt), _) => (BinaryOperator::Lt, rest.advance(1)),
        (Some(Gt), _) => (BinaryOperator::Gt, rest.advance(1)),
        (Some(Lte), _) => (BinaryOperator::Lte, rest.advance(1)),
        (Some(Gte), _) => (BinaryOperator::Gte, rest.advance(1)),
        _ => return None,
    };
    let (rest, right) = simple_expr_fast_path(value_start).ok()??;
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(ORDER | LIMIT | EOI | SemiColon | FORMAT)
    ) {
        return None;
    }

    Some((rest, Expr::BinaryOp {
        span: Some(token.span),
        op,
        left: Box::new(expr),
        right: Box::new(right),
    }))
}

fn simple_column_expr(i: Input) -> Option<(Input, Expr)> {
    let start = i;
    let (mut rest, ident0) = simple_identifier(i)?;
    if rest.tokens.first().map(|token| token.kind) != Some(Dot) {
        return Some((rest, Expr::ColumnRef {
            span: ident0.span,
            column: ColumnRef {
                database: None,
                table: None,
                column: ColumnID::Name(ident0),
            },
        }));
    }

    let (next, ident1) = simple_identifier(rest.advance(1))?;
    rest = next;
    if rest.tokens.first().map(|token| token.kind) != Some(Dot) {
        let span = transform_span(&start.tokens[..start.tokens.len() - rest.tokens.len()]);
        return Some((rest, Expr::ColumnRef {
            span,
            column: ColumnRef {
                database: None,
                table: Some(ident0),
                column: ColumnID::Name(ident1),
            },
        }));
    }

    let (rest, ident2) = simple_identifier(rest.advance(1))?;
    let span = transform_span(&start.tokens[..start.tokens.len() - rest.tokens.len()]);
    Some((rest, Expr::ColumnRef {
        span,
        column: ColumnRef {
            database: Some(ident0),
            table: Some(ident1),
            column: ColumnID::Name(ident2),
        },
    }))
}

fn simple_literal_expr(i: Input) -> Option<(Input, Expr)> {
    let token = i.tokens.first()?;
    let span = Some(token.span);
    let value = match token.kind {
        LiteralInteger => Literal::UInt64(token.text().parse().ok()?),
        TRUE | FALSE => Literal::Boolean(token.kind == TRUE),
        NULL => Literal::Null,
        _ => return None,
    };
    Some((i.advance(1), Expr::Literal { span, value }))
}

fn simple_identifier(i: Input) -> Option<(Input, Identifier)> {
    let token = i.tokens.first().filter(|token| token.kind == Ident)?;
    Some((i.advance(1), Identifier {
        span: Some(token.span),
        name: token.text().to_string(),
        quote: None,
        ident_type: IdentifierType::None,
    }))
}

pub(crate) fn statement(
    i: Input,
) -> std::result::Result<Option<(Input, Statement)>, nom::Err<Error>> {
    match i.tokens.first().map(|token| token.kind) {
        Some(SELECT) => {
            Ok(query(i)?.map(|(rest, query)| (rest, Statement::Query(Box::new(query)))))
        }
        Some(ALTER) => Ok(simple_alter_table_stmt(i)),
        Some(CALL) => Ok(simple_call_stmt(i)),
        Some(CREATE) => Ok(simple_create_database_stmt(i)
            .or_else(|| simple_create_role_stmt(i))
            .or_else(|| simple_create_table_stmt(i))),
        Some(DELETE) => simple_delete_stmt(i),
        Some(INSERT) => simple_insert_values_stmt(i),
        Some(SET) => simple_set_stmt(i),
        Some(SHOW) => Ok(simple_show_create_table_stmt(i)),
        Some(UPDATE) => simple_update_stmt(i),
        Some(DROP) => Ok(simple_drop_database_stmt(i)
            .or_else(|| simple_drop_table_stmt(i))
            .or_else(|| simple_drop_view_stmt(i))
            .or_else(|| simple_drop_principal_stmt(i))),
        _ => Ok(None),
    }
}

pub(crate) fn statement_format_tail(i: Input) -> Option<(Input, Option<String>)> {
    let mut rest = i;
    let mut format = None;

    if rest.tokens.first().map(|token| token.kind) == Some(FORMAT) {
        let token = rest.tokens.get(1).filter(|token| token.kind == Ident)?;
        format = Some(token.text().to_string());
        rest = rest.advance(2);
    }

    if rest.tokens.first().map(|token| token.kind) == Some(SemiColon) {
        rest = rest.advance(1);
    }

    (rest.tokens.first().map(|token| token.kind) == Some(EOI)).then_some((rest, format))
}

fn simple_call_stmt(i: Input) -> Option<(Input, Statement)> {
    if i.tokens.first().map(|token| token.kind) != Some(CALL) {
        return None;
    }
    if i.tokens.get(1).map(|token| token.kind) == Some(PROCEDURE) {
        return None;
    }

    let (mut rest, name) = simple_identifier(i.advance(1))?;
    if rest.tokens.first().map(|token| token.kind) != Some(LParen) {
        return None;
    }
    rest = rest.advance(1);

    let mut args = Vec::new();
    if rest.tokens.first().map(|token| token.kind) != Some(RParen) {
        args = Vec::with_capacity(2);
        loop {
            let (next, arg) = simple_call_arg(rest)?;
            args.push(arg);
            rest = next;
            match rest.tokens.first().map(|token| token.kind) {
                Some(Comma) => rest = rest.advance(1),
                Some(RParen) => break,
                _ => return None,
            }
        }
    }
    rest = rest.advance(1);

    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return None;
    }

    Some((rest, Statement::Call(CallStmt { name, args })))
}

fn simple_call_arg(i: Input) -> Option<(Input, String)> {
    let token = i.tokens.first()?;
    let arg = match token.kind {
        LiteralString
            if token
                .text()
                .chars()
                .next()
                .is_some_and(|quote| i.dialect.is_string_quote(quote)) =>
        {
            parse_simple_string_literal(i, token).ok()?
        }
        Ident => token.text().to_string(),
        LiteralInteger => token.text().to_string(),
        TRUE | FALSE => token.text().to_ascii_lowercase(),
        _ => return None,
    };
    Some((i.advance(1), arg))
}

fn simple_set_stmt(i: Input) -> std::result::Result<Option<(Input, Statement)>, nom::Err<Error>> {
    if i.tokens.first().map(|token| token.kind) != Some(SET) {
        return Ok(None);
    }

    let mut rest = i.advance(1);
    let set_type = match rest.tokens.first().map(|token| token.kind) {
        Some(GLOBAL) => {
            rest = rest.advance(1);
            SetType::SettingsGlobal
        }
        Some(SESSION) => {
            rest = rest.advance(1);
            SetType::SettingsSession
        }
        Some(VARIABLE) => {
            rest = rest.advance(1);
            SetType::Variable
        }
        _ => SetType::SettingsSession,
    };

    let Some((next, var)) = simple_identifier(rest) else {
        return Ok(None);
    };
    rest = next;

    if rest.tokens.first().map(|token| token.kind) != Some(Eq) {
        return Ok(None);
    }

    let (next, value) = match subexpr(0)(rest.advance(1)) {
        Ok(res) => res,
        Err(nom::Err::Error(_)) => return Ok(None),
        Err(err) => return Err(err),
    };
    rest = next;

    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return Ok(None);
    }

    Ok(Some((rest, Statement::SetStmt {
        settings: Settings {
            set_type,
            identifiers: vec![var],
            values: SetValues::Expr(vec![Box::new(value)]),
        },
    })))
}

fn simple_delete_stmt(
    i: Input,
) -> std::result::Result<Option<(Input, Statement)>, nom::Err<Error>> {
    if !matches!(
        (
            i.tokens.first().map(|token| token.kind),
            i.tokens.get(1).map(|token| token.kind),
        ),
        (Some(DELETE), Some(FROM))
    ) {
        return Ok(None);
    }

    let (mut rest, (catalog, database, table)) =
        match simple_dot_separated_idents_1_to_3(i.advance(2)) {
            Some(res) => res,
            None => return Ok(None),
        };
    let (next, table_alias) = match simple_table_alias(rest) {
        Some(res) => res,
        None => return Ok(None),
    };
    rest = next;

    let mut selection = None;
    if rest.tokens.first().map(|token| token.kind) == Some(WHERE) {
        let (next, expr) = subexpr(0)(rest.advance(1))?;
        rest = next;
        selection = Some(expr);
    }

    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return Ok(None);
    }

    Ok(Some((
        rest,
        Statement::Delete(DeleteStmt {
            hints: None,
            catalog,
            database,
            table,
            table_alias,
            selection,
            with: None,
        }),
    )))
}

fn simple_update_stmt(
    i: Input,
) -> std::result::Result<Option<(Input, Statement)>, nom::Err<Error>> {
    if i.tokens.first().map(|token| token.kind) != Some(UPDATE) {
        return Ok(None);
    }

    let (mut rest, (catalog, database, table)) =
        match simple_dot_separated_idents_1_to_3(i.advance(1)) {
            Some(res) => res,
            None => return Ok(None),
        };
    let (next, table_alias) = match simple_table_alias(rest) {
        Some(res) => res,
        None => return Ok(None),
    };
    rest = next;

    if rest.tokens.first().map(|token| token.kind) != Some(SET) {
        return Ok(None);
    }
    rest = rest.advance(1);

    let mut update_list = Vec::with_capacity(2);
    loop {
        let Some((next, update_expr)) = simple_mutation_update_expr(rest)? else {
            return Ok(None);
        };
        update_list.push(update_expr);
        rest = next;
        match rest.tokens.first().map(|token| token.kind) {
            Some(Comma) => rest = rest.advance(1),
            Some(WHERE | EOI | SemiColon | FORMAT) => break,
            Some(FROM) => return Ok(None),
            _ => return Ok(None),
        }
    }

    let mut selection = None;
    if rest.tokens.first().map(|token| token.kind) == Some(WHERE) {
        let (next, expr) = subexpr(0)(rest.advance(1))?;
        rest = next;
        selection = Some(expr);
    }

    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return Ok(None);
    }

    Ok(Some((
        rest,
        Statement::Update(UpdateStmt {
            hints: None,
            catalog,
            database,
            table,
            table_alias,
            update_list,
            from: None,
            selection,
            with: None,
        }),
    )))
}

fn simple_mutation_update_expr(
    i: Input,
) -> std::result::Result<Option<(Input, MutationUpdateExpr)>, nom::Err<Error>> {
    let (mut rest, ident0) = match simple_identifier(i) {
        Some(res) => res,
        None => return Ok(None),
    };
    let (table, name) = if rest.tokens.first().map(|token| token.kind) == Some(Dot) {
        let Some((next, ident1)) = simple_identifier(rest.advance(1)) else {
            return Ok(None);
        };
        rest = next;
        (Some(ident0), ident1)
    } else {
        (None, ident0)
    };

    if rest.tokens.first().map(|token| token.kind) != Some(Eq) {
        return Ok(None);
    }
    let (rest, expr) = subexpr(0)(rest.advance(1))?;
    Ok(Some((rest, MutationUpdateExpr { table, name, expr })))
}

fn simple_create_database_stmt(i: Input) -> Option<(Input, Statement)> {
    if i.tokens.first().map(|token| token.kind) != Some(CREATE) {
        return None;
    }
    let mut rest = i.advance(1);
    let mut opt_or_replace = false;
    if matches!(
        (
            rest.tokens.first().map(|token| token.kind),
            rest.tokens.get(1).map(|token| token.kind),
        ),
        (Some(OR), Some(REPLACE))
    ) {
        rest = rest.advance(2);
        opt_or_replace = true;
    }
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(DATABASE | SCHEMA)
    ) {
        return None;
    }
    rest = rest.advance(1);

    let mut opt_if_not_exists = false;
    if matches!(
        (
            rest.tokens.first().map(|token| token.kind),
            rest.tokens.get(1).map(|token| token.kind),
            rest.tokens.get(2).map(|token| token.kind),
        ),
        (Some(IF), Some(NOT), Some(EXISTS))
    ) {
        rest = rest.advance(3);
        opt_if_not_exists = true;
    }
    if opt_or_replace && opt_if_not_exists {
        return None;
    }

    let (rest, (catalog, database)) = simple_database_ref(rest)?;
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return None;
    }

    let create_option = match (opt_or_replace, opt_if_not_exists) {
        (false, false) => CreateOption::Create,
        (true, false) => CreateOption::CreateOrReplace,
        (false, true) => CreateOption::CreateIfNotExists,
        (true, true) => unreachable!(),
    };
    Some((
        rest,
        Statement::CreateDatabase(CreateDatabaseStmt {
            create_option,
            database: DatabaseRef { catalog, database },
            engine: None,
            options: vec![],
        }),
    ))
}

fn simple_drop_database_stmt(i: Input) -> Option<(Input, Statement)> {
    if !matches!(
        (
            i.tokens.first().map(|token| token.kind),
            i.tokens.get(1).map(|token| token.kind),
        ),
        (Some(DROP), Some(DATABASE | SCHEMA))
    ) {
        return None;
    }
    let mut rest = i.advance(2);
    let mut if_exists = false;
    if matches!(
        (
            rest.tokens.first().map(|token| token.kind),
            rest.tokens.get(1).map(|token| token.kind),
        ),
        (Some(IF), Some(EXISTS))
    ) {
        rest = rest.advance(2);
        if_exists = true;
    }
    let (rest, (catalog, database)) = simple_database_ref(rest)?;
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return None;
    }
    Some((
        rest,
        Statement::DropDatabase(DropDatabaseStmt {
            if_exists,
            catalog,
            database,
        }),
    ))
}

fn simple_drop_table_stmt(i: Input) -> Option<(Input, Statement)> {
    if !matches!(
        (
            i.tokens.first().map(|token| token.kind),
            i.tokens.get(1).map(|token| token.kind),
        ),
        (Some(DROP), Some(TABLE))
    ) {
        return None;
    }
    let mut rest = i.advance(2);
    let mut if_exists = false;
    if matches!(
        (
            rest.tokens.first().map(|token| token.kind),
            rest.tokens.get(1).map(|token| token.kind),
        ),
        (Some(IF), Some(EXISTS))
    ) {
        rest = rest.advance(2);
        if_exists = true;
    }
    let (rest, (catalog, database, table)) = simple_dot_separated_idents_1_to_3(rest)?;
    let mut rest = rest;
    let mut all = false;
    if rest.tokens.first().map(|token| token.kind) == Some(ALL) {
        rest = rest.advance(1);
        all = true;
    }
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return None;
    }
    Some((
        rest,
        Statement::DropTable(DropTableStmt {
            if_exists,
            catalog,
            database,
            table,
            all,
        }),
    ))
}

fn simple_drop_view_stmt(i: Input) -> Option<(Input, Statement)> {
    if !matches!(
        (
            i.tokens.first().map(|token| token.kind),
            i.tokens.get(1).map(|token| token.kind),
        ),
        (Some(DROP), Some(VIEW))
    ) {
        return None;
    }
    let mut rest = i.advance(2);
    let mut if_exists = false;
    if matches!(
        (
            rest.tokens.first().map(|token| token.kind),
            rest.tokens.get(1).map(|token| token.kind),
        ),
        (Some(IF), Some(EXISTS))
    ) {
        rest = rest.advance(2);
        if_exists = true;
    }
    let (rest, (catalog, database, view)) = simple_dot_separated_idents_1_to_3(rest)?;
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return None;
    }
    Some((
        rest,
        Statement::DropView(DropViewStmt {
            if_exists,
            catalog,
            database,
            view,
        }),
    ))
}

fn simple_database_ref(i: Input) -> Option<(Input, (Option<Identifier>, Identifier))> {
    let (rest, (catalog, database, name)) = simple_dot_separated_idents_1_to_3(i)?;
    if catalog.is_some() {
        return None;
    }
    Some((rest, (database, name)))
}

fn simple_drop_principal_stmt(i: Input) -> Option<(Input, Statement)> {
    let kind = i.tokens.get(1).map(|token| token.kind)?;
    let mut rest = i.advance(2);
    let mut if_exists = false;
    if matches!(
        (
            rest.tokens.first().map(|token| token.kind),
            rest.tokens.get(1).map(|token| token.kind),
        ),
        (Some(IF), Some(EXISTS))
    ) {
        rest = rest.advance(2);
        if_exists = true;
    }

    match kind {
        ROLE => {
            let token = rest.tokens.first()?;
            let role_name = match token.kind {
                Ident => token.text().to_string(),
                LiteralString
                    if token
                        .text()
                        .chars()
                        .next()
                        .is_some_and(|quote| rest.dialect.is_string_quote(quote)) =>
                {
                    parse_simple_string_literal(rest, token).ok()?
                }
                _ => return None,
            };
            rest = rest.advance(1);
            if !matches!(
                rest.tokens.first().map(|token| token.kind),
                Some(EOI | SemiColon | FORMAT)
            ) {
                return None;
            }
            Some((rest, Statement::DropRole {
                if_exists,
                role_name,
            }))
        }
        USER => {
            let token = rest.tokens.first()?;
            let username = match token.kind {
                Ident => token.text().to_string(),
                LiteralString
                    if token
                        .text()
                        .chars()
                        .next()
                        .is_some_and(|quote| rest.dialect.is_string_quote(quote)) =>
                {
                    parse_simple_string_literal(rest, token).ok()?
                }
                _ => return None,
            };
            rest = rest.advance(1);
            if rest.tokens.first().map(|token| token.kind) == Some(Abs) {
                let host = rest
                    .tokens
                    .get(1)
                    .filter(|token| token.kind == LiteralString)?;
                if parse_simple_string_literal(rest, host).ok()?.as_str() != "%" {
                    return None;
                }
                rest = rest.advance(2);
            }
            if !matches!(
                rest.tokens.first().map(|token| token.kind),
                Some(EOI | SemiColon | FORMAT)
            ) {
                return None;
            }
            Some((rest, Statement::DropUser {
                if_exists,
                user: UserIdentity {
                    username,
                    hostname: "%".to_string(),
                },
            }))
        }
        _ => None,
    }
}

fn simple_create_role_stmt(i: Input) -> Option<(Input, Statement)> {
    if !matches!(
        (
            i.tokens.first().map(|token| token.kind),
            i.tokens.get(1).map(|token| token.kind),
        ),
        (Some(CREATE), Some(ROLE))
    ) {
        return None;
    }
    let token = i.tokens.get(2).filter(|token| token.kind == Ident)?;
    let rest = i.advance(3);
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return None;
    }

    Some((rest, Statement::CreateRole {
        create_option: CreateOption::Create,
        role_name: token.text().to_string(),
        comment: None,
    }))
}

fn simple_show_create_table_stmt(i: Input) -> Option<(Input, Statement)> {
    if !matches!(
        (
            i.tokens.first().map(|token| token.kind),
            i.tokens.get(1).map(|token| token.kind),
            i.tokens.get(2).map(|token| token.kind),
        ),
        (Some(SHOW), Some(CREATE), Some(TABLE))
    ) {
        return None;
    }
    let (rest, (catalog, database, table)) = simple_dot_separated_idents_1_to_3(i.advance(3))?;
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return None;
    }

    Some((
        rest,
        Statement::ShowCreateTable(ShowCreateTableStmt {
            catalog,
            database,
            table,
            with_quoted_ident: false,
        }),
    ))
}

fn simple_insert_values_stmt(
    i: Input,
) -> std::result::Result<Option<(Input, Statement)>, nom::Err<Error>> {
    let mut rest = i.advance(1);
    if rest.tokens.first().map(|token| token.kind) != Some(INTO) {
        return Ok(None);
    }
    rest = rest.advance(1);
    if rest.tokens.first().map(|token| token.kind) == Some(TABLE) {
        rest = rest.advance(1);
    }

    let (next, (catalog, database, table)) = match simple_dot_separated_idents_1_to_3(rest) {
        Some(res) => res,
        None => return Ok(None),
    };
    rest = next;
    let table = TableRef {
        catalog,
        database,
        table,
        branch: None,
    };

    let mut columns = Vec::new();
    if rest.tokens.first().map(|token| token.kind) == Some(LParen) {
        columns = Vec::with_capacity(4);
        rest = rest.advance(1);
        loop {
            let Some((next, column)) = simple_ident(rest) else {
                return Ok(None);
            };
            columns.push(column);
            rest = next;
            match rest.tokens.first().map(|token| token.kind) {
                Some(Comma) => rest = rest.advance(1),
                Some(RParen) => {
                    rest = rest.advance(1);
                    break;
                }
                _ => return Ok(None),
            }
        }
    }

    if rest.tokens.first().map(|token| token.kind) != Some(VALUES) {
        return Ok(None);
    }
    rest = rest.advance(1);
    let mut rows = Vec::with_capacity(1);
    loop {
        let Some((next, row)) = simple_insert_row(rest)? else {
            return Ok(None);
        };
        rows.push(row);
        rest = next;
        match rest.tokens.first().map(|token| token.kind) {
            Some(Comma) => rest = rest.advance(1),
            Some(EOI | SemiColon | FORMAT) => break,
            _ => return Ok(None),
        }
    }

    Ok(Some((
        rest,
        Statement::Insert(InsertStmt {
            hints: None,
            with: None,
            table,
            columns,
            source: InsertSource::Values { rows },
            overwrite: false,
        }),
    )))
}

fn simple_insert_row(i: Input) -> std::result::Result<Option<(Input, Vec<Expr>)>, nom::Err<Error>> {
    if i.tokens.first().map(|token| token.kind) != Some(LParen) {
        return Ok(None);
    }
    let mut rest = i.advance(1);
    let mut row = Vec::with_capacity(4);
    loop {
        let Some((next, expr)) = simple_insert_value(rest)? else {
            return Ok(None);
        };
        row.push(expr);
        rest = next;
        match rest.tokens.first().map(|token| token.kind) {
            Some(Comma) => rest = rest.advance(1),
            Some(RParen) => return Ok(Some((rest.advance(1), row))),
            _ => return Ok(None),
        }
    }
}

fn simple_insert_value(i: Input) -> std::result::Result<Option<(Input, Expr)>, nom::Err<Error>> {
    if i.tokens.first().is_some_and(|token| token.kind == Ident)
        && i.tokens.get(1).is_some_and(|token| token.kind == LParen)
        && let Some((rest, func)) = simple_function_call_fast_path(i)?
    {
        let consumed = i.tokens.len() - rest.tokens.len();
        return Ok(Some((rest, Expr::FunctionCall {
            span: transform_span(&i.tokens[..consumed]),
            func,
        })));
    }

    let Some(token) = i.tokens.first() else {
        return Ok(None);
    };
    let span = Some(token.span);
    let value = match token.kind {
        DEFAULT => {
            return Ok(Some((i.advance(1), Expr::ColumnRef {
                span,
                column: ColumnRef {
                    database: None,
                    table: None,
                    column: ColumnID::Name(Identifier {
                        span,
                        name: token.text().to_string(),
                        quote: None,
                        ident_type: IdentifierType::None,
                    }),
                },
            })));
        }
        LiteralString
            if token
                .text()
                .chars()
                .next()
                .is_some_and(|quote| i.dialect.is_string_quote(quote)) =>
        {
            let value = parse_simple_string_literal(i, token)?;
            Literal::String(value)
        }
        LiteralInteger => parse_uint(token.text(), 10)
            .map_err(|err| nom::Err::Failure(Error::from_error_kind(i, err)))?,
        LiteralFloat if !token.text().starts_with('.') => parse_float(token.text())
            .map_err(|err| nom::Err::Failure(Error::from_error_kind(i, err)))?,
        MySQLLiteralHex => parse_uint(&token.text()[2..], 16)
            .map_err(|err| nom::Err::Failure(Error::from_error_kind(i, err)))?,
        TRUE | FALSE => Literal::Boolean(token.kind == TRUE),
        NULL => Literal::Null,
        Minus => {
            let Some(next) = i.tokens.get(1) else {
                return Ok(None);
            };
            let span = transform_span(&i.tokens[..2]);
            let value = match next.kind {
                LiteralInteger => parse_uint(next.text(), 10)
                    .map_err(|err| nom::Err::Failure(Error::from_error_kind(i, err)))?,
                LiteralFloat if !next.text().starts_with('.') => parse_float(next.text())
                    .map_err(|err| nom::Err::Failure(Error::from_error_kind(i, err)))?,
                _ => return Ok(None),
            };
            return Ok(Some((i.advance(2), Expr::UnaryOp {
                span,
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Literal {
                    span: Some(next.span),
                    value,
                }),
            })));
        }
        _ => {
            let (rest, expr) = subexpr(0)(i)?;
            if rest
                .tokens
                .first()
                .is_some_and(|token| matches!(token.kind, Comma | RParen))
            {
                return Ok(Some((rest, expr)));
            }
            return Ok(None);
        }
    };

    Ok(Some((i.advance(1), Expr::Literal { span, value })))
}

fn simple_alter_table_stmt(i: Input) -> Option<(Input, Statement)> {
    let mut rest = i.advance(1);
    if rest.tokens.first().map(|token| token.kind) != Some(TABLE) {
        return None;
    }
    rest = rest.advance(1);

    let (next, table_reference) = simple_statement_table_reference(rest)?;
    rest = next;
    let (rest, action) = match rest.tokens.first().map(|token| token.kind)? {
        DROP => simple_alter_table_drop_action(rest)?,
        RENAME => simple_alter_table_rename_action(rest)?,
        MODIFY => simple_alter_table_modify_action(rest)?,
        _ => return None,
    };
    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return None;
    }

    Some((
        rest,
        Statement::AlterTable(AlterTableStmt {
            if_exists: false,
            table_reference,
            action,
        }),
    ))
}

fn simple_alter_table_drop_action(i: Input) -> Option<(Input, AlterTableAction)> {
    let rest = i.advance(1);
    match rest.tokens.first().map(|token| token.kind)? {
        CONSTRAINT => {
            let (rest, constraint_name) = simple_ident(rest.advance(1))?;
            Some((rest, AlterTableAction::DropConstraint { constraint_name }))
        }
        COLUMN => {
            let (rest, column) = simple_ident(rest.advance(1))?;
            Some((rest, AlterTableAction::DropColumn { column }))
        }
        Ident => {
            let (rest, column) = simple_ident(rest)?;
            Some((rest, AlterTableAction::DropColumn { column }))
        }
        _ => None,
    }
}

fn simple_alter_table_rename_action(i: Input) -> Option<(Input, AlterTableAction)> {
    let mut rest = i.advance(1);
    if rest.tokens.first().map(|token| token.kind) == Some(TO) {
        let (rest, new_table) = simple_ident(rest.advance(1))?;
        return Some((rest, AlterTableAction::RenameTable { new_table }));
    }
    if rest.tokens.first().map(|token| token.kind) == Some(COLUMN) {
        rest = rest.advance(1);
    }
    let (rest, old_column) = simple_ident(rest)?;
    if rest.tokens.first().map(|token| token.kind) != Some(TO) {
        return None;
    }
    let (rest, new_column) = simple_ident(rest.advance(1))?;
    Some((rest, AlterTableAction::RenameColumn {
        old_column,
        new_column,
    }))
}

fn simple_alter_table_modify_action(i: Input) -> Option<(Input, AlterTableAction)> {
    let mut rest = i.advance(1);
    if rest.tokens.first().map(|token| token.kind) == Some(COLUMN) {
        rest = rest.advance(1);
    }
    let mut columns = Vec::new();
    loop {
        let (next, name) = simple_ident(rest)?;
        let (next, data_type) = simple_column_type_name_with_nullable(next)?;
        columns.push(ColumnDefinition {
            name,
            data_type,
            expr: None,
            check: None,
            comment: None,
            stats_truncate_len: None,
        });
        rest = next;
        match rest.tokens.first().map(|token| token.kind) {
            Some(Comma) => {
                rest = rest.advance(1);
                if rest.tokens.first().map(|token| token.kind) == Some(COLUMN) {
                    rest = rest.advance(1);
                }
            }
            Some(EOI | SemiColon | FORMAT) => break,
            _ => return None,
        }
    }

    Some((rest, AlterTableAction::ModifyColumn {
        action: ModifyColumnAction::SetDataType(columns),
    }))
}

fn simple_create_table_stmt(i: Input) -> Option<(Input, Statement)> {
    let mut rest = i.advance(1);
    let table_type = match rest.tokens.first().map(|token| token.kind) {
        Some(TEMP) | Some(TEMPORARY) => {
            rest = rest.advance(1);
            TableType::Temporary
        }
        Some(TRANSIENT) => {
            rest = rest.advance(1);
            TableType::Transient
        }
        _ => TableType::Normal,
    };
    if rest.tokens.first().map(|token| token.kind) != Some(TABLE) {
        return None;
    }
    rest = rest.advance(1);

    let mut create_option = CreateOption::Create;
    if matches!(
        (
            rest.tokens.first().map(|token| token.kind),
            rest.tokens.get(1).map(|token| token.kind),
            rest.tokens.get(2).map(|token| token.kind),
        ),
        (Some(IF), Some(NOT), Some(EXISTS))
    ) {
        rest = rest.advance(3);
        create_option = CreateOption::CreateIfNotExists;
    }

    let (next, (catalog, database, table)) = simple_dot_separated_idents_1_to_3(rest)?;
    rest = next;
    if rest.tokens.first().map(|token| token.kind) != Some(LParen) {
        return None;
    }
    rest = rest.advance(1);

    let mut columns = Vec::new();
    loop {
        let (next, column) = simple_create_column_definition(rest).ok()??;
        columns.push(column);
        rest = next;
        match rest.tokens.first().map(|token| token.kind) {
            Some(Comma) => rest = rest.advance(1),
            Some(RParen) => {
                rest = rest.advance(1);
                break;
            }
            _ => return None,
        }
    }

    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(EOI | SemiColon | FORMAT)
    ) {
        return None;
    }

    Some((
        rest,
        Statement::CreateTable(CreateTableStmt {
            create_option,
            catalog,
            database,
            table,
            source: Some(CreateTableSource::Columns {
                columns,
                opt_table_indexes: None,
                opt_column_constraints: None,
                opt_table_constraints: None,
            }),
            engine: None,
            uri_location: None,
            cluster_by: None,
            table_options: BTreeMap::new(),
            iceberg_table_partition: None,
            table_properties: None,
            as_query: None,
            table_type,
        }),
    ))
}

fn simple_create_column_definition(
    i: Input,
) -> std::result::Result<Option<(Input, ColumnDefinition)>, nom::Err<Error>> {
    let Some((mut rest, name)) = simple_ident(i) else {
        return Ok(None);
    };
    let Some((next, mut data_type)) = simple_column_type_name_without_boundary(rest) else {
        return Ok(None);
    };
    rest = next;

    let mut expr = None;
    loop {
        match (
            rest.tokens.first().map(|token| token.kind),
            rest.tokens.get(1).map(|token| token.kind),
        ) {
            (Some(NOT), Some(NULL)) => {
                if data_type.is_nullable() {
                    return Ok(None);
                }
                data_type = data_type.wrap_not_null();
                rest = rest.advance(2);
            }
            (Some(NULL), _) => {
                if matches!(data_type, TypeName::NotNull(_)) {
                    return Ok(None);
                }
                data_type = data_type.wrap_nullable();
                rest = rest.advance(1);
            }
            (Some(DEFAULT), _) => {
                if expr.is_some() {
                    return Ok(None);
                }
                let (next, default_expr) = subexpr(0)(rest.advance(1))?;
                expr = Some(ColumnExpr::Default(Box::new(default_expr)));
                rest = next;
            }
            _ => break,
        }
    }

    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(Comma | RParen)
    ) {
        return Ok(None);
    }

    Ok(Some((rest, ColumnDefinition {
        name,
        data_type,
        expr,
        check: None,
        comment: None,
        stats_truncate_len: None,
    })))
}

fn simple_statement_table_reference(i: Input) -> Option<(Input, TableReference)> {
    let (rest, (catalog, database, table)) = simple_dot_separated_idents_1_to_3(i)?;
    let consumed = i.tokens.len() - rest.tokens.len();
    Some((rest, TableReference::Table {
        span: transform_span(&i.tokens[..consumed]),
        table: TableRef {
            catalog,
            database,
            table,
            branch: None,
        },
        alias: None,
        temporal: None,
        with_options: None,
        pivot: None,
        unpivot: None,
        sample: None,
    }))
}

type SimpleTableName = (Option<Identifier>, Option<Identifier>, Identifier);

fn simple_dot_separated_idents_1_to_3(i: Input) -> Option<(Input, SimpleTableName)> {
    let (mut rest, ident0) = simple_ident(i)?;
    if rest.tokens.first().map(|token| token.kind) != Some(Dot) {
        return Some((rest, (None, None, ident0)));
    }
    let (next, ident1) = simple_ident(rest.advance(1))?;
    rest = next;
    if rest.tokens.first().map(|token| token.kind) != Some(Dot) {
        return Some((rest, (None, Some(ident0), ident1)));
    }
    let (rest, ident2) = simple_ident(rest.advance(1))?;
    Some((rest, (Some(ident0), Some(ident1), ident2)))
}

fn simple_ident(i: Input) -> Option<(Input, Identifier)> {
    let token = i.tokens.first().filter(|token| token.kind == Ident)?;
    Some((i.advance(1), Identifier {
        span: Some(token.span),
        name: token.text().to_string(),
        quote: None,
        ident_type: IdentifierType::None,
    }))
}

fn simple_column_type_name_with_nullable(i: Input) -> Option<(Input, TypeName)> {
    let (mut rest, mut ty) = simple_column_type_name_without_boundary(i)?;
    match (
        rest.tokens.first().map(|token| token.kind),
        rest.tokens.get(1).map(|token| token.kind),
    ) {
        (Some(NOT), Some(NULL)) => {
            rest = rest.advance(2);
            ty = ty.wrap_not_null();
        }
        (Some(NULL), _) => {
            rest = rest.advance(1);
            ty = ty.wrap_nullable();
        }
        _ => {}
    }

    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(Comma | RParen | EOI | SemiColon | FORMAT)
    ) {
        return None;
    }

    Some((rest, ty))
}

fn simple_column_type_name_without_boundary(i: Input) -> Option<(Input, TypeName)> {
    let token = i.tokens.first()?;
    match token.kind {
        BOOLEAN | BOOL => Some((i.advance(1), TypeName::Boolean)),
        INT8 | TINYINT => Some((consume_simple_type_width(i.advance(1)), TypeName::Int8)),
        INT16 | SMALLINT => Some((consume_simple_type_width(i.advance(1)), TypeName::Int16)),
        INT32 | INT | INTEGER => Some((consume_simple_type_width(i.advance(1)), TypeName::Int32)),
        INT64 | SIGNED | BIGINT => Some((consume_simple_type_width(i.advance(1)), TypeName::Int64)),
        FLOAT32 | FLOAT | REAL => Some((i.advance(1), TypeName::Float32)),
        FLOAT64 | DOUBLE => {
            let rest = if i.tokens.get(1).is_some_and(|token| token.kind == PRECISION) {
                i.advance(2)
            } else {
                i.advance(1)
            };
            Some((rest, TypeName::Float64))
        }
        DECIMAL => simple_decimal_type(i),
        STRING | VARCHAR | CHAR | CHARACTER | TEXT => {
            Some((consume_simple_type_width(i.advance(1)), TypeName::String))
        }
        DATE => Some((i.advance(1), TypeName::Date)),
        DATETIME | TIMESTAMP => {
            Some((consume_simple_type_width(i.advance(1)), TypeName::Timestamp))
        }
        BINARY | VARBINARY | LONGBLOB | MEDIUMBLOB | TINYBLOB | BLOB => {
            Some((consume_simple_type_width(i.advance(1)), TypeName::Binary))
        }
        VARIANT | JSON => Some((i.advance(1), TypeName::Variant)),
        _ => None,
    }
}

fn simple_decimal_type(i: Input) -> Option<(Input, TypeName)> {
    if !matches!(
        (
            i.tokens.get(1).map(|token| token.kind),
            i.tokens.get(2).map(|token| token.kind)
        ),
        (Some(LParen), Some(LiteralInteger))
    ) {
        return Some((i.advance(1), TypeName::Decimal {
            precision: 18,
            scale: 3,
        }));
    }

    let precision = i.tokens.get(2)?.text().parse().ok()?;
    match (
        i.tokens.get(3).map(|token| token.kind),
        i.tokens.get(4).map(|token| token.kind),
        i.tokens.get(5).map(|token| token.kind),
    ) {
        (Some(RParen), _, _) => Some((i.advance(4), TypeName::Decimal {
            precision,
            scale: 0,
        })),
        (Some(Comma), Some(LiteralInteger), Some(RParen)) => {
            let scale = i.tokens.get(4)?.text().parse().ok()?;
            Some((i.advance(6), TypeName::Decimal { precision, scale }))
        }
        _ => None,
    }
}

fn consume_simple_type_width(i: Input) -> Input {
    if matches!(
        (
            i.tokens.first().map(|token| token.kind),
            i.tokens.get(1).map(|token| token.kind),
            i.tokens.get(2).map(|token| token.kind)
        ),
        (Some(LParen), Some(LiteralInteger), Some(RParen))
    ) {
        i.advance(3)
    } else {
        i
    }
}
