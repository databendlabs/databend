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

use common_datavalues::IntervalKind;
use common_meta_types::PrincipalIdentity;
use common_meta_types::UserIdentity;

use super::walk::walk_cte;
use super::walk::walk_expr;
use super::walk::walk_identifier;
use super::walk::walk_join_condition;
use super::walk::walk_query;
use super::walk::walk_select_target;
use super::walk::walk_set_expr;
use super::walk::walk_statement;
use super::walk::walk_table_reference;
use super::walk_time_travel_point;
use crate::ast::*;
use crate::parser::token::Token;

pub trait Visitor<'ast>: Sized {
    fn visit_expr(&mut self, expr: &'ast Expr<'ast>) {
        walk_expr(self, expr);
    }

    fn visit_identifier(&mut self, _ident: &'ast Identifier<'ast>) {}

    fn visit_database_ref(
        &mut self,
        catalog: &'ast Option<Identifier<'ast>>,
        database: &'ast Identifier<'ast>,
    ) {
        if let Some(catalog) = catalog {
            walk_identifier(self, catalog);
        }

        walk_identifier(self, database);
    }

    fn visit_table_ref(
        &mut self,
        catalog: &'ast Option<Identifier<'ast>>,
        database: &'ast Option<Identifier<'ast>>,
        table: &'ast Identifier<'ast>,
    ) {
        if let Some(catalog) = catalog {
            walk_identifier(self, catalog);
        }

        if let Some(database) = database {
            walk_identifier(self, database);
        }

        walk_identifier(self, table);
    }

    fn visit_column_ref(
        &mut self,
        _span: &'ast [Token<'ast>],
        database: &'ast Option<Identifier<'ast>>,
        table: &'ast Option<Identifier<'ast>>,
        column: &'ast Identifier<'ast>,
    ) {
        if let Some(database) = database {
            walk_identifier(self, database);
        }

        if let Some(table) = table {
            walk_identifier(self, table);
        }

        walk_identifier(self, column);
    }

    fn visit_is_null(&mut self, _span: &'ast [Token<'ast>], expr: &'ast Expr<'ast>, _not: bool) {
        walk_expr(self, expr);
    }

    fn visit_is_distinct_from(
        &mut self,
        _span: &'ast [Token<'ast>],
        left: &'ast Expr<'ast>,
        right: &'ast Expr<'ast>,
        _not: bool,
    ) {
        walk_expr(self, left);
        walk_expr(self, right);
    }

    fn visit_in_list(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        list: &'ast [Expr<'ast>],
        _not: bool,
    ) {
        walk_expr(self, expr);
        for expr in list {
            walk_expr(self, expr);
        }
    }

    fn visit_in_subquery(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        subquery: &'ast Query<'ast>,
        _not: bool,
    ) {
        walk_expr(self, expr);
        walk_query(self, subquery);
    }

    fn visit_between(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        low: &'ast Expr<'ast>,
        high: &'ast Expr<'ast>,
        _not: bool,
    ) {
        walk_expr(self, expr);
        walk_expr(self, low);
        walk_expr(self, high);
    }

    fn visit_binary_op(
        &mut self,
        _span: &'ast [Token<'ast>],
        _op: &'ast BinaryOperator,
        left: &'ast Expr<'ast>,
        right: &'ast Expr<'ast>,
    ) {
        walk_expr(self, left);
        walk_expr(self, right);
    }

    fn visit_unary_op(
        &mut self,
        _span: &'ast [Token<'ast>],
        _op: &'ast UnaryOperator,
        expr: &'ast Expr<'ast>,
    ) {
        walk_expr(self, expr);
    }

    fn visit_cast(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        _target_type: &'ast TypeName,
        _pg_style: bool,
    ) {
        walk_expr(self, expr);
    }

    fn visit_try_cast(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        _target_type: &'ast TypeName,
    ) {
        walk_expr(self, expr);
    }

    fn visit_extract(
        &mut self,
        _span: &'ast [Token<'ast>],
        _kind: &'ast IntervalKind,
        expr: &'ast Expr<'ast>,
    ) {
        walk_expr(self, expr);
    }

    fn visit_positon(
        &mut self,
        _span: &'ast [Token<'ast>],
        substr_expr: &'ast Expr<'ast>,
        str_expr: &'ast Expr<'ast>,
    ) {
        walk_expr(self, substr_expr);
        walk_expr(self, str_expr);
    }

    fn visit_substring(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        substring_from: &'ast Option<Box<Expr<'ast>>>,
        substring_for: &'ast Option<Box<Expr<'ast>>>,
    ) {
        walk_expr(self, expr);
        if let Some(substring_from) = substring_from {
            walk_expr(self, substring_from);
        }
        if let Some(substring_for) = substring_for {
            walk_expr(self, substring_for);
        }
    }

    fn visit_trim(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        _trim_where: &'ast Option<(TrimWhere, Box<Expr<'ast>>)>,
    ) {
        walk_expr(self, expr);
    }

    fn visit_literal(&mut self, _span: &'ast [Token<'ast>], _lit: &'ast Literal) {}

    fn visit_count_all(&mut self, _span: &'ast [Token<'ast>]) {}

    fn visit_tuple(&mut self, _span: &'ast [Token<'ast>], elements: &'ast [Expr<'ast>]) {
        for element in elements {
            walk_expr(self, element);
        }
    }

    fn visit_function_call(
        &mut self,
        _span: &'ast [Token<'ast>],
        _distinct: bool,
        _name: &'ast Identifier<'ast>,
        args: &'ast [Expr<'ast>],
        _params: &'ast [Literal],
    ) {
        for arg in args {
            walk_expr(self, arg);
        }
    }

    fn visit_case_when(
        &mut self,
        _span: &'ast [Token<'ast>],
        operand: &'ast Option<Box<Expr<'ast>>>,
        conditions: &'ast [Expr<'ast>],
        results: &'ast [Expr<'ast>],
        else_result: &'ast Option<Box<Expr<'ast>>>,
    ) {
        if let Some(operand) = operand {
            walk_expr(self, operand);
        }
        for condition in conditions {
            walk_expr(self, condition);
        }
        for result in results {
            walk_expr(self, result);
        }
        if let Some(else_result) = else_result {
            walk_expr(self, else_result);
        }
    }

    fn visit_exists(
        &mut self,
        _span: &'ast [Token<'ast>],
        _not: bool,
        subquery: &'ast Query<'ast>,
    ) {
        walk_query(self, subquery);
    }

    fn visit_subquery(
        &mut self,
        _span: &'ast [Token<'ast>],
        _modifier: &'ast Option<SubqueryModifier>,
        subquery: &'ast Query<'ast>,
    ) {
        walk_query(self, subquery);
    }

    fn visit_map_access(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        _accessor: &'ast MapAccessor<'ast>,
    ) {
        walk_expr(self, expr);
    }

    fn visit_array(&mut self, _span: &'ast [Token<'ast>], exprs: &'ast [Expr<'ast>]) {
        for expr in exprs {
            walk_expr(self, expr);
        }
    }

    fn visit_interval(
        &mut self,
        _span: &'ast [Token<'ast>],
        expr: &'ast Expr<'ast>,
        _unit: &'ast IntervalKind,
    ) {
        walk_expr(self, expr);
    }

    fn visit_date_add(
        &mut self,
        _span: &'ast [Token<'ast>],
        _unit: &'ast IntervalKind,
        interval: &'ast Expr<'ast>,
        date: &'ast Expr<'ast>,
    ) {
        walk_expr(self, date);
        walk_expr(self, interval);
    }

    fn visit_date_sub(
        &mut self,
        _span: &'ast [Token<'ast>],
        _unit: &'ast IntervalKind,
        interval: &'ast Expr<'ast>,
        date: &'ast Expr<'ast>,
    ) {
        walk_expr(self, date);
        walk_expr(self, interval);
    }

    fn visit_date_trunc(
        &mut self,
        _span: &'ast [Token<'ast>],
        _unit: &'ast IntervalKind,
        date: &'ast Expr<'ast>,
    ) {
        walk_expr(self, date);
    }

    fn visit_statement(&mut self, statement: &'ast Statement<'ast>) {
        walk_statement(self, statement);
    }

    fn visit_query(&mut self, query: &'ast Query<'ast>) {
        walk_query(self, query);
    }

    fn visit_explain(&mut self, _kind: &'ast ExplainKind, _query: &'ast Statement<'ast>) {}

    fn visit_copy(&mut self, _copy: &'ast CopyStmt<'ast>) {}

    fn visit_copy_unit(&mut self, _copy_unit: &'ast CopyUnit<'ast>) {}

    fn visit_call(&mut self, _call: &'ast CallStmt) {}

    fn visit_show_settings(&mut self, _like: &'ast Option<String>) {}

    fn visit_show_process_list(&mut self) {}

    fn visit_show_metrics(&mut self) {}

    fn visit_show_engines(&mut self) {}

    fn visit_show_functions(&mut self, _limit: &'ast Option<ShowLimit<'ast>>) {}

    fn visit_show_limit(&mut self, _limit: &'ast ShowLimit<'ast>) {}

    fn visit_kill(&mut self, _kill_target: &'ast KillTarget, _object_id: &'ast str) {}

    fn visit_set_variable(
        &mut self,
        _is_global: bool,
        _variable: &'ast Identifier<'ast>,
        _value: &'ast Literal,
    ) {
    }

    fn visit_insert(&mut self, _insert: &'ast InsertStmt<'ast>) {}

    fn visit_insert_source(&mut self, _insert_source: &'ast InsertSource<'ast>) {}

    fn visit_delete(
        &mut self,
        _table_reference: &'ast TableReference<'ast>,
        _selection: &'ast Option<Expr<'ast>>,
    ) {
    }

    fn visit_show_databases(&mut self, _stmt: &'ast ShowDatabasesStmt<'ast>) {}

    fn visit_show_create_databases(&mut self, _stmt: &'ast ShowCreateDatabaseStmt<'ast>) {}

    fn visit_create_database(&mut self, _stmt: &'ast CreateDatabaseStmt<'ast>) {}

    fn visit_drop_database(&mut self, _stmt: &'ast DropDatabaseStmt<'ast>) {}

    fn visit_undrop_database(&mut self, _stmt: &'ast UndropDatabaseStmt<'ast>) {}

    fn visit_alter_database(&mut self, _stmt: &'ast AlterDatabaseStmt<'ast>) {}

    fn visit_use_database(&mut self, _database: &'ast Identifier<'ast>) {}

    fn visit_show_tables(&mut self, _stmt: &'ast ShowTablesStmt<'ast>) {}

    fn visit_show_create_table(&mut self, _stmt: &'ast ShowCreateTableStmt<'ast>) {}

    fn visit_describe_table(&mut self, _stmt: &'ast DescribeTableStmt<'ast>) {}

    fn visit_show_tables_status(&mut self, _stmt: &'ast ShowTablesStatusStmt<'ast>) {}

    fn visit_create_table(&mut self, _stmt: &'ast CreateTableStmt<'ast>) {}

    fn visit_create_table_source(&mut self, _source: &'ast CreateTableSource<'ast>) {}

    fn visit_column_definition(&mut self, _column_definition: &'ast ColumnDefinition<'ast>) {}

    fn visit_drop_table(&mut self, _stmt: &'ast DropTableStmt<'ast>) {}

    fn visit_undrop_table(&mut self, _stmt: &'ast UndropTableStmt<'ast>) {}

    fn visit_alter_table(&mut self, _stmt: &'ast AlterTableStmt<'ast>) {}

    fn visit_rename_table(&mut self, _stmt: &'ast RenameTableStmt<'ast>) {}

    fn visit_truncate_table(&mut self, _stmt: &'ast TruncateTableStmt<'ast>) {}

    fn visit_optimize_table(&mut self, _stmt: &'ast OptimizeTableStmt<'ast>) {}

    fn visit_exists_table(&mut self, _stmt: &'ast ExistsTableStmt<'ast>) {}

    fn visit_create_view(&mut self, _stmt: &'ast CreateViewStmt<'ast>) {}

    fn visit_alter_view(&mut self, _stmt: &'ast AlterViewStmt<'ast>) {}

    fn visit_drop_view(&mut self, _stmt: &'ast DropViewStmt<'ast>) {}

    fn visit_show_users(&mut self) {}

    fn visit_create_user(&mut self, _stmt: &'ast CreateUserStmt) {}

    fn visit_alter_user(&mut self, _stmt: &'ast AlterUserStmt) {}

    fn visit_drop_user(&mut self, _if_exists: bool, _user: &'ast UserIdentity) {}

    fn visit_show_roles(&mut self) {}

    fn visit_create_role(&mut self, _if_not_exists: bool, _role_name: &'ast str) {}

    fn visit_drop_role(&mut self, _if_exists: bool, _role_name: &'ast str) {}

    fn visit_grant(&mut self, _grant: &'ast GrantStmt) {}

    fn visit_show_grant(&mut self, _principal: &'ast Option<PrincipalIdentity>) {}

    fn visit_revoke(&mut self, _revoke: &'ast RevokeStmt) {}

    fn visit_create_udf(
        &mut self,
        _if_not_exists: bool,
        _udf_name: &'ast Identifier<'ast>,
        _parameters: &'ast [Identifier<'ast>],
        _definition: &'ast Expr<'ast>,
        _description: &'ast Option<String>,
    ) {
    }

    fn visit_drop_udf(&mut self, _if_exists: bool, _udf_name: &'ast Identifier<'ast>) {}

    fn visit_alter_udf(
        &mut self,
        _udf_name: &'ast Identifier<'ast>,
        _parameters: &'ast [Identifier<'ast>],
        _definition: &'ast Expr<'ast>,
        _description: &'ast Option<String>,
    ) {
    }

    fn visit_create_stage(&mut self, _stmt: &'ast CreateStageStmt) {}

    fn visit_show_stages(&mut self) {}

    fn visit_drop_stage(&mut self, _if_exists: bool, _stage_name: &'ast str) {}

    fn visit_describe_stage(&mut self, _stage_name: &'ast str) {}

    fn visit_remove_stage(&mut self, _location: &'ast str, _pattern: &'ast str) {}

    fn visit_list_stage(&mut self, _location: &'ast str, _pattern: &'ast str) {}

    fn visit_presign(&mut self, _presign: &'ast PresignStmt) {}

    fn visit_create_share(&mut self, _stmt: &'ast CreateShareStmt<'ast>) {}

    fn visit_drop_share(&mut self, _stmt: &'ast DropShareStmt<'ast>) {}

    fn visit_grant_share_object(&mut self, _stmt: &'ast GrantShareObjectStmt<'ast>) {}

    fn visit_revoke_share_object(&mut self, _stmt: &'ast RevokeShareObjectStmt<'ast>) {}

    fn visit_alter_share_tenants(&mut self, _stmt: &'ast AlterShareTenantsStmt<'ast>) {}

    fn visit_desc_share(&mut self, _stmt: &'ast DescShareStmt<'ast>) {}

    fn visit_show_shares(&mut self, _stmt: &'ast ShowSharesStmt) {}

    fn visit_show_object_grant_privileges(&mut self, _stmt: &'ast ShowObjectGrantPrivilegesStmt) {}

    fn visit_show_grants_of_share(&mut self, _stmt: &'ast ShowGrantsOfShareStmt) {}

    fn visit_with(&mut self, with: &'ast With<'ast>) {
        let With { ctes, .. } = with;
        for cte in ctes.iter() {
            walk_cte(self, cte);
        }
    }

    fn visit_set_expr(&mut self, expr: &'ast SetExpr<'ast>) {
        walk_set_expr(self, expr);
    }

    fn visit_set_operation(&mut self, op: &'ast SetOperation<'ast>) {
        let SetOperation { left, right, .. } = op;

        walk_set_expr(self, left);
        walk_set_expr(self, right);
    }

    fn visit_order_by(&mut self, order_by: &'ast OrderByExpr<'ast>) {
        let OrderByExpr { expr, .. } = order_by;
        walk_expr(self, expr);
    }

    fn visit_select_stmt(&mut self, stmt: &'ast SelectStmt<'ast>) {
        let SelectStmt {
            select_list,
            from,
            selection,
            group_by,
            having,
            ..
        } = stmt;

        for target in select_list.iter() {
            walk_select_target(self, target);
        }

        for table_ref in from.iter() {
            walk_table_reference(self, table_ref);
        }

        if let Some(selection) = selection {
            walk_expr(self, selection);
        }

        for expr in group_by.iter() {
            walk_expr(self, expr);
        }

        if let Some(having) = having {
            walk_expr(self, having);
        }
    }

    fn visit_select_target(&mut self, target: &'ast SelectTarget<'ast>) {
        walk_select_target(self, target);
    }

    fn visit_table_reference(&mut self, table: &'ast TableReference<'ast>) {
        walk_table_reference(self, table);
    }

    fn visit_time_travel_point(&mut self, time: &'ast TimeTravelPoint<'ast>) {
        walk_time_travel_point(self, time);
    }

    fn visit_join(&mut self, join: &'ast Join<'ast>) {
        let Join {
            left,
            right,
            condition,
            ..
        } = join;

        walk_table_reference(self, left);
        walk_table_reference(self, right);

        walk_join_condition(self, condition);
    }
}
