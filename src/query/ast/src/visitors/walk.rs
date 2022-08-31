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

use super::Visitor;
use crate::ast::*;

pub fn walk_expr<'a, V: Visitor<'a>>(visitor: &mut V, expr: &'a Expr<'a>) {
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
        } => visitor.visit_date_add(span, unit, interval, date),
        Expr::DateSub {
            span,
            date,
            interval,
            unit,
        } => visitor.visit_date_sub(span, unit, interval, date),
        Expr::DateTrunc { span, unit, date } => visitor.visit_date_trunc(span, unit, date),
    }
}

pub fn walk_identifier<'a, V: Visitor<'a>>(visitor: &mut V, ident: &'a Identifier<'a>) {
    visitor.visit_identifier(ident);
}

pub fn walk_query<'a, V: Visitor<'a>>(visitor: &mut V, query: &'a Query<'a>) {
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

pub fn walk_set_expr<'a, V: Visitor<'a>>(visitor: &mut V, set_expr: &'a SetExpr<'a>) {
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

pub fn walk_select_target<'a, V: Visitor<'a>>(visitor: &mut V, target: &'a SelectTarget<'a>) {
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

pub fn walk_table_reference<'a, V: Visitor<'a>>(
    visitor: &mut V,
    table_ref: &'a TableReference<'a>,
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
                visitor.visit_identifier(&alias.name);
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
                visitor.visit_identifier(&alias.name);
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
                visitor.visit_identifier(&alias.name);
            }
        }
        TableReference::Join { join, .. } => {
            visitor.visit_join(join);
        }
    }
}

pub fn walk_time_travel_point<'a, V: Visitor<'a>>(visitor: &mut V, time: &'a TimeTravelPoint<'a>) {
    match time {
        TimeTravelPoint::Snapshot(_) => {}
        TimeTravelPoint::Timestamp(expr) => visitor.visit_expr(expr),
    }
}

pub fn walk_join_condition<'a, V: Visitor<'a>>(visitor: &mut V, join_cond: &'a JoinCondition<'a>) {
    match join_cond {
        JoinCondition::On(expr) => visitor.visit_expr(expr),
        JoinCondition::Using(using) => {
            for ident in using.iter() {
                visitor.visit_identifier(ident);
            }
        }
        JoinCondition::Natural => {}
        JoinCondition::None => {}
    }
}

pub fn walk_cte<'a, V: Visitor<'a>>(visitor: &mut V, cte: &'a CTE<'a>) {
    let CTE { alias, query, .. } = cte;

    visitor.visit_identifier(&alias.name);
    visitor.visit_query(query);
}

pub fn walk_statement<'a, V: Visitor<'a>>(visitor: &mut V, statement: &'a Statement<'a>) {
    match statement {
        Statement::Explain { kind, query } => visitor.visit_explain(kind, query),
        Statement::Query(query) => visitor.visit_query(query),
        Statement::Insert(insert) => visitor.visit_insert(insert),
        Statement::Delete {
            table_reference,
            selection,
            ..
        } => visitor.visit_delete(table_reference, selection),
        Statement::Copy(stmt) => visitor.visit_copy(stmt),
        Statement::ShowSettings { like } => visitor.visit_show_settings(like),
        Statement::ShowProcessList => visitor.visit_show_process_list(),
        Statement::ShowMetrics => visitor.visit_show_metrics(),
        Statement::ShowEngines => visitor.visit_show_engines(),
        Statement::ShowFunctions { limit } => visitor.visit_show_functions(limit),
        Statement::KillStmt {
            kill_target,
            object_id,
        } => visitor.visit_kill(kill_target, object_id),
        Statement::SetVariable {
            is_global,
            variable,
            value,
        } => visitor.visit_set_variable(*is_global, variable, value),
        Statement::ShowDatabases(stmt) => visitor.visit_show_databases(stmt),
        Statement::ShowCreateDatabase(stmt) => visitor.visit_show_create_databases(stmt),
        Statement::CreateDatabase(stmt) => visitor.visit_create_database(stmt),
        Statement::DropDatabase(stmt) => visitor.visit_drop_database(stmt),
        Statement::UndropDatabase(stmt) => visitor.visit_undrop_database(stmt),
        Statement::AlterDatabase(stmt) => visitor.visit_alter_database(stmt),
        Statement::UseDatabase { database } => visitor.visit_use_database(database),
        Statement::ShowTables(stmt) => visitor.visit_show_tables(stmt),
        Statement::ShowCreateTable(stmt) => visitor.visit_show_create_table(stmt),
        Statement::DescribeTable(stmt) => visitor.visit_describe_table(stmt),
        Statement::ShowTablesStatus(stmt) => visitor.visit_show_tables_status(stmt),
        Statement::CreateTable(stmt) => visitor.visit_create_table(stmt),
        Statement::DropTable(stmt) => visitor.visit_drop_table(stmt),
        Statement::UndropTable(stmt) => visitor.visit_undrop_table(stmt),
        Statement::AlterTable(stmt) => visitor.visit_alter_table(stmt),
        Statement::RenameTable(stmt) => visitor.visit_rename_table(stmt),
        Statement::TruncateTable(stmt) => visitor.visit_truncate_table(stmt),
        Statement::OptimizeTable(stmt) => visitor.visit_optimize_table(stmt),
        Statement::ExistsTable(stmt) => visitor.visit_exists_table(stmt),
        Statement::CreateView(stmt) => visitor.visit_create_view(stmt),
        Statement::AlterView(stmt) => visitor.visit_alter_view(stmt),
        Statement::DropView(stmt) => visitor.visit_drop_view(stmt),
        Statement::ShowUsers => visitor.visit_show_users(),
        Statement::ShowRoles => visitor.visit_show_roles(),
        Statement::CreateUser(stmt) => visitor.visit_create_user(stmt),
        Statement::AlterUser(stmt) => visitor.visit_alter_user(stmt),
        Statement::DropUser { if_exists, user } => visitor.visit_drop_user(*if_exists, user),
        Statement::CreateRole {
            if_not_exists,
            role_name,
        } => visitor.visit_create_role(*if_not_exists, role_name),
        Statement::DropRole {
            if_exists,
            role_name,
        } => visitor.visit_drop_role(*if_exists, role_name),
        Statement::Grant(stmt) => visitor.visit_grant(stmt),
        Statement::ShowGrants { principal } => visitor.visit_show_grant(principal),
        Statement::Revoke(stmt) => visitor.visit_revoke(stmt),
        Statement::CreateUDF {
            if_not_exists,
            udf_name,
            parameters,
            definition,
            description,
        } => visitor.visit_create_udf(
            *if_not_exists,
            udf_name,
            parameters,
            definition,
            description,
        ),
        Statement::DropUDF {
            if_exists,
            udf_name,
        } => visitor.visit_drop_udf(*if_exists, udf_name),
        Statement::AlterUDF {
            udf_name,
            parameters,
            definition,
            description,
        } => visitor.visit_alter_udf(udf_name, parameters, definition, description),
        Statement::ListStage { location, pattern } => visitor.visit_list_stage(location, pattern),
        Statement::ShowStages => visitor.visit_show_stages(),
        Statement::DropStage {
            if_exists,
            stage_name,
        } => visitor.visit_drop_stage(*if_exists, stage_name),
        Statement::CreateStage(stmt) => visitor.visit_create_stage(stmt),
        Statement::RemoveStage { location, pattern } => {
            visitor.visit_remove_stage(location, pattern)
        }
        Statement::DescribeStage { stage_name } => visitor.visit_describe_stage(stage_name),
        Statement::Call(stmt) => visitor.visit_call(stmt),
        Statement::Presign(stmt) => visitor.visit_presign(stmt),
        Statement::CreateShare(stmt) => visitor.visit_create_share(stmt),
        Statement::DropShare(stmt) => visitor.visit_drop_share(stmt),
        Statement::GrantShareObject(stmt) => visitor.visit_grant_share_object(stmt),
        Statement::RevokeShareObject(stmt) => visitor.visit_revoke_share_object(stmt),
        Statement::AlterShareTenants(stmt) => visitor.visit_alter_share_tenants(stmt),
        Statement::DescShare(stmt) => visitor.visit_desc_share(stmt),
        Statement::ShowShares(stmt) => visitor.visit_show_shares(stmt),
        Statement::ShowObjectGrantPrivileges(stmt) => {
            visitor.visit_show_object_grant_privileges(stmt)
        }
        Statement::ShowGrantsOfShare(stmt) => visitor.visit_show_grants_of_share(stmt),
    }
}
