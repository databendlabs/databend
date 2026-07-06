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
use crate::parser::ErrorKind;
use crate::parser::common::transform_span;
use crate::parser::expr::parse_float;
use crate::parser::expr::parse_uint;
use crate::parser::expr::simple_function_call_fast_path;
use crate::parser::input::Input;
use crate::parser::token::*;

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
        let (next, expr) = match simple_where_is_null(rest.advance(1)) {
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

fn simple_select_targets(
    i: Input,
) -> std::result::Result<Option<(Input, Vec<SelectTarget>)>, nom::Err<Error>> {
    let mut rest = i;
    let mut targets = Vec::new();
    loop {
        let Some((next, target)) = simple_select_target(rest)? else {
            return Ok(None);
        };
        targets.push(target);
        rest = next;
        match rest.tokens.first().map(|token| token.kind) {
            Some(Comma) => rest = rest.advance(1),
            Some(FROM) => return Ok(Some((rest, targets))),
            _ => return Ok(None),
        }
    }
}

fn simple_select_target(
    i: Input,
) -> std::result::Result<Option<(Input, SelectTarget)>, nom::Err<Error>> {
    if let Some(star) = i.tokens.first().filter(|token| token.kind == Multiply) {
        return Ok(Some((i.advance(1), SelectTarget::StarColumns {
            qualified: vec![Indirection::Star(Some(star.span))],
            column_filter: None,
        })));
    }

    if i.tokens.first().is_some_and(|token| token.kind == Ident)
        && i.tokens.get(1).is_some_and(|token| token.kind == LParen)
        && let Some((rest, func)) = simple_function_call_fast_path(i)?
    {
        let consumed = i.tokens.len() - rest.tokens.len();
        let span = transform_span(&i.tokens[..consumed]);
        return Ok(Some((rest, SelectTarget::AliasedExpr {
            expr: Box::new(Expr::FunctionCall { span, func }),
            alias: None,
        })));
    }

    Ok(simple_column_expr(i).map(|(rest, expr)| {
        (rest, SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias: None,
        })
    }))
}

fn simple_table_reference(i: Input) -> Option<(Input, TableReference)> {
    let (rest, table) = simple_table_ref(i)?;
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
        alias: None,
        temporal: None,
        with_options: None,
        pivot: None,
        unpivot: None,
        sample: None,
    }))
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

fn simple_where_is_null(i: Input) -> Option<(Input, Expr)> {
    let start = i;
    let (mut rest, expr) = simple_column_expr(i)?;
    let not = match (
        rest.tokens.first().map(|token| token.kind),
        rest.tokens.get(1).map(|token| token.kind),
        rest.tokens.get(2).map(|token| token.kind),
    ) {
        (Some(IS), Some(NULL), _) => {
            rest = rest.advance(2);
            false
        }
        (Some(IS), Some(NOT), Some(NULL)) => {
            rest = rest.advance(3);
            true
        }
        _ => return None,
    };
    let consumed = start.tokens.len() - rest.tokens.len();
    Some((rest, Expr::IsNull {
        span: transform_span(&start.tokens[..consumed]),
        expr: Box::new(expr),
        not,
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
        Some(ALTER) => Ok(simple_alter_table_stmt(i)),
        Some(CREATE) => Ok(simple_create_role_stmt(i).or_else(|| simple_create_table_stmt(i))),
        Some(INSERT) => simple_insert_values_stmt(i),
        Some(SHOW) => Ok(simple_show_create_table_stmt(i)),
        Some(DROP) => Ok(simple_drop_principal_stmt(i)),
        _ => Ok(None),
    }
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
                    let quote::QuotedString(value, _) = token.text().parse().ok()?;
                    value
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
                    let quote::QuotedString(value, _) = token.text().parse().ok()?;
                    value
                }
                _ => return None,
            };
            rest = rest.advance(1);
            if matches!(
                (
                    rest.tokens.first().map(|token| token.kind),
                    rest.tokens.get(1).map(|token| token.kind),
                ),
                (Some(Abs), Some(LiteralString))
            ) {
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
    let mut rows = Vec::new();
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
    let mut row = Vec::new();
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
            let quote::QuotedString(value, _) = token.text().parse().map_err(|_| {
                nom::Err::Failure(Error::from_error_kind(
                    i,
                    ErrorKind::other("invalid escape or unicode"),
                ))
            })?;
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
        _ => return Ok(None),
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
        let (next, data_type) = simple_column_type_name(next)?;
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
        let (next, name) = simple_ident(rest)?;
        let (next, data_type) = simple_column_type_name(next)?;
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

fn simple_column_type_name(i: Input) -> Option<(Input, TypeName)> {
    let token = i.tokens.first()?;
    let (rest, ty) = match token.kind {
        BOOLEAN | BOOL => (i.advance(1), TypeName::Boolean),
        INT8 | TINYINT => (consume_simple_type_width(i.advance(1)), TypeName::Int8),
        INT16 | SMALLINT => (consume_simple_type_width(i.advance(1)), TypeName::Int16),
        INT32 | INT | INTEGER => (consume_simple_type_width(i.advance(1)), TypeName::Int32),
        INT64 | SIGNED | BIGINT => (consume_simple_type_width(i.advance(1)), TypeName::Int64),
        FLOAT32 | FLOAT | REAL => (i.advance(1), TypeName::Float32),
        FLOAT64 | DOUBLE => {
            let rest = if i.tokens.get(1).is_some_and(|token| token.kind == PRECISION) {
                i.advance(2)
            } else {
                i.advance(1)
            };
            (rest, TypeName::Float64)
        }
        DECIMAL => simple_decimal_type(i)?,
        STRING | VARCHAR | CHAR | CHARACTER | TEXT => {
            (consume_simple_type_width(i.advance(1)), TypeName::String)
        }
        DATE => (i.advance(1), TypeName::Date),
        DATETIME | TIMESTAMP => (consume_simple_type_width(i.advance(1)), TypeName::Timestamp),
        BINARY | VARBINARY | LONGBLOB | MEDIUMBLOB | TINYBLOB | BLOB => {
            (consume_simple_type_width(i.advance(1)), TypeName::Binary)
        }
        VARIANT | JSON => (i.advance(1), TypeName::Variant),
        _ => return None,
    };

    if !matches!(
        rest.tokens.first().map(|token| token.kind),
        Some(Comma | RParen | EOI | SemiColon | FORMAT)
    ) {
        return None;
    }

    Some((rest, ty))
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
