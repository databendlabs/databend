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

use super::visitor_mut::VisitorMut;
use crate::ast::*;

pub fn walk_expr_mut<'a, V: VisitorMut>(visitor: &mut V, expr: &mut Expr<'a>) {
    match expr {
        Expr::ColumnRef {
            span,
            database,
            table,
            column,
        } => visitor.visit_column_ref(span, database, table, column),
        Expr::IsNull { span, expr, not } => visitor.visit_is_null(span, expr, *not),
        Expr::IsDistinctFrom {
            span,
            left,
            right,
            not,
        } => visitor.visit_is_distinct_from(span, left, right, *not),
        Expr::InList {
            span,
            expr,
            list,
            not,
        } => visitor.visit_in_list(span, expr, list, *not),
        Expr::InSubquery {
            span,
            expr,
            subquery,
            not,
        } => visitor.visit_in_subquery(span, expr, subquery, *not),
        Expr::Between {
            span,
            expr,
            low,
            high,
            not,
        } => visitor.visit_between(span, expr, low, high, *not),
        Expr::BinaryOp {
            span,
            op,
            left,
            right,
        } => visitor.visit_binary_op(span, op, left, right),
        Expr::UnaryOp { span, op, expr } => visitor.visit_unary_op(span, op, expr),
        Expr::Cast {
            span,
            expr,
            target_type,
            pg_style,
        } => visitor.visit_cast(span, expr, target_type, *pg_style),
        Expr::TryCast {
            span,
            expr,
            target_type,
        } => visitor.visit_try_cast(span, expr, target_type),
        Expr::Extract { span, kind, expr } => visitor.visit_extract(span, kind, expr),
        Expr::Position {
            span,
            substr_expr,
            str_expr,
        } => visitor.visit_positon(span, substr_expr, str_expr),
        Expr::Substring {
            span,
            expr,
            substring_from,
            substring_for,
        } => visitor.visit_substring(span, expr, substring_from, substring_for),
        Expr::Trim {
            span,
            expr,
            trim_where,
        } => visitor.visit_trim(span, expr, trim_where),
        Expr::Literal { span, lit } => visitor.visit_literal(span, lit),
        Expr::CountAll { span } => visitor.visit_count_all(span),
        Expr::Tuple { span, exprs } => visitor.visit_tuple(span, exprs),
        Expr::FunctionCall {
            span,
            distinct,
            name,
            args,
            params,
        } => visitor.visit_function_call(span, *distinct, name, args, params),
        Expr::Case {
            span,
            operand,
            conditions,
            results,
            else_result,
        } => visitor.visit_case_when(span, operand, conditions, results, else_result),
        Expr::Exists {
            span,
            not,
            subquery,
        } => visitor.visit_exists(span, *not, subquery),
        Expr::Subquery {
            span,
            modifier,
            subquery,
        } => visitor.visit_subquery(span, modifier, subquery),
        Expr::MapAccess {
            span,
            expr,
            accessor,
        } => visitor.visit_map_access(span, expr, accessor),
        Expr::Array { span, exprs } => visitor.visit_array(span, exprs),
        Expr::Interval { span, expr, unit } => visitor.visit_interval(span, expr, unit),
        Expr::DateAdd {
            span,
            date,
            interval,
            unit,
        } => visitor.visit_date_add(span, date, interval, unit),
        Expr::DateSub {
            span,
            date,
            interval,
            unit,
        } => visitor.visit_date_sub(span, date, interval, unit),
        Expr::DateTrunc { span, unit, date } => visitor.visit_date_trunc(span, unit, date),
        Expr::IfNull { span, expr1, expr2 } => visitor.visit_ifnull(span, expr1, expr2),
        Expr::NullIf { span, expr1, expr2 } => visitor.visit_nullif(span, expr1, expr2),
        Expr::Coalesce { span, exprs } => visitor.visit_coalesce(span, exprs),
    }
}

pub fn walk_identifier_mut<'a, V: VisitorMut>(visitor: &mut V, ident: &mut Identifier<'a>) {
    visitor.visit_identifier(ident);
}

pub fn walk_query_mut<'a, V: VisitorMut>(visitor: &mut V, query: &mut Query<'a>) {
    let Query {
        with,
        body,
        order_by,
        limit,
        offset,
        ..
    } = query;

    if let Some(with) = with {
        visitor.visit_with(with);
    }
    visitor.visit_set_expr(body);
    for order_by in order_by {
        visitor.visit_order_by(order_by);
    }
    for limit in limit {
        visitor.visit_expr(limit);
    }
    if let Some(offset) = offset {
        visitor.visit_expr(offset);
    }
}

pub fn walk_set_expr_mut<'a, V: VisitorMut>(visitor: &mut V, set_expr: &mut SetExpr<'a>) {
    match set_expr {
        SetExpr::Select(select) => {
            visitor.visit_select_stmt(select);
        }
        SetExpr::Query(query) => {
            visitor.visit_query(query);
        }
        SetExpr::SetOperation(op) => {
            visitor.visit_set_operation(op);
        }
    }
}

pub fn walk_select_target_mut<'a, V: VisitorMut>(visitor: &mut V, target: &mut SelectTarget<'a>) {
    match target {
        SelectTarget::AliasedExpr { expr, alias } => {
            visitor.visit_expr(expr);
            if let Some(alias) = alias {
                visitor.visit_identifier(alias);
            }
        }
        SelectTarget::QualifiedName(names) => {
            for indirection in names {
                match indirection {
                    Indirection::Identifier(ident) => {
                        visitor.visit_identifier(ident);
                    }
                    Indirection::Star => {}
                }
            }
        }
    }
}

pub fn walk_table_reference_mut<'a, V: VisitorMut>(
    visitor: &mut V,
    table_ref: &mut TableReference<'a>,
) {
    match table_ref {
        TableReference::Table {
            catalog,
            database,
            table,
            alias,
            travel_point,
            ..
        } => {
            if let Some(catalog) = catalog {
                visitor.visit_identifier(catalog);
            }

            if let Some(database) = database {
                visitor.visit_identifier(database);
            }

            visitor.visit_identifier(table);

            if let Some(alias) = alias {
                visitor.visit_identifier(&mut alias.name);
            }

            if let Some(travel_point) = travel_point {
                visitor.visit_time_travel_point(travel_point);
            }
        }
        TableReference::Subquery {
            subquery, alias, ..
        } => {
            visitor.visit_query(subquery);
            if let Some(alias) = alias {
                visitor.visit_identifier(&mut alias.name);
            }
        }
        TableReference::TableFunction {
            name,
            params,
            alias,
            ..
        } => {
            visitor.visit_identifier(name);
            for param in params {
                visitor.visit_expr(param);
            }
            if let Some(alias) = alias {
                visitor.visit_identifier(&mut alias.name);
            }
        }
        TableReference::Join { join, .. } => {
            visitor.visit_join(join);
        }
    }
}

pub fn walk_time_travel_point_mut<'a, V: VisitorMut>(
    visitor: &mut V,
    time: &mut TimeTravelPoint<'a>,
) {
    match time {
        TimeTravelPoint::Snapshot(_) => {}
        TimeTravelPoint::Timestamp(expr) => visitor.visit_expr(expr),
    }
}

pub fn walk_join_condition_mut<'a, V: VisitorMut>(
    visitor: &mut V,
    join_cond: &mut JoinCondition<'a>,
) {
    match join_cond {
        JoinCondition::On(expr) => visitor.visit_expr(expr),
        JoinCondition::Using(using) => {
            for ident in using.iter_mut() {
                visitor.visit_identifier(ident);
            }
        }
        JoinCondition::Natural => {}
        JoinCondition::None => {}
    }
}

pub fn walk_cte_mut<'a, V: VisitorMut>(visitor: &mut V, cte: &mut CTE<'a>) {
    let CTE { alias, query, .. } = cte;

    visitor.visit_identifier(&mut alias.name);
    visitor.visit_query(query);
}
