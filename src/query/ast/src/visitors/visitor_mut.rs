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

use super::walk_mut::walk_cte_mut;
use super::walk_mut::walk_expr_mut;
use super::walk_mut::walk_identifier_mut;
use super::walk_mut::walk_join_condition_mut;
use super::walk_mut::walk_query_mut;
use super::walk_mut::walk_select_target_mut;
use super::walk_mut::walk_set_expr_mut;
use super::walk_mut::walk_statement_mut;
use super::walk_mut::walk_table_reference_mut;
use super::walk_time_travel_point_mut;
use crate::ast::*;
use crate::parser::token::Token;

pub trait VisitorMut: Sized {
    fn visit_expr(&mut self, expr: &mut Expr<'_>) {
        walk_expr_mut(self, expr);
    }

    fn visit_identifier(&mut self, _ident: &mut Identifier<'_>) {}

    fn visit_database_ref(
        &mut self,
        catalog: &mut Option<Identifier<'_>>,
        database: &mut Identifier<'_>,
    ) {
        if let Some(catalog) = catalog {
            walk_identifier_mut(self, catalog);
        }

        walk_identifier_mut(self, database);
    }

    fn visit_table_ref(
        &mut self,
        catalog: &mut Option<Identifier<'_>>,
        database: &mut Option<Identifier<'_>>,
        table: &mut Identifier<'_>,
    ) {
        if let Some(catalog) = catalog {
            walk_identifier_mut(self, catalog);
        }

        if let Some(database) = database {
            walk_identifier_mut(self, database);
        }

        walk_identifier_mut(self, table);
    }

    fn visit_column_ref(
        &mut self,
        _span: &mut &[Token<'_>],
        database: &mut Option<Identifier<'_>>,
        table: &mut Option<Identifier<'_>>,
        column: &mut Identifier<'_>,
    ) {
        if let Some(database) = database {
            walk_identifier_mut(self, database);
        }

        if let Some(table) = table {
            walk_identifier_mut(self, table);
        }

        walk_identifier_mut(self, column);
    }

    fn visit_is_null(&mut self, _span: &mut &[Token<'_>], expr: &mut Expr<'_>, _not: bool) {
        walk_expr_mut(self, expr);
    }

    fn visit_is_distinct_from(
        &mut self,
        _span: &mut &[Token<'_>],
        left: &mut Expr<'_>,
        right: &mut Expr<'_>,
        _not: bool,
    ) {
        walk_expr_mut(self, left);
        walk_expr_mut(self, right);
    }

    fn visit_in_list(
        &mut self,
        _span: &mut &[Token<'_>],
        expr: &mut Expr<'_>,
        list: &mut [Expr<'_>],
        _not: bool,
    ) {
        walk_expr_mut(self, expr);
        for expr in list {
            walk_expr_mut(self, expr);
        }
    }

    fn visit_in_subquery(
        &mut self,
        _span: &mut &[Token<'_>],
        expr: &mut Expr<'_>,
        subquery: &mut Query<'_>,
        _not: bool,
    ) {
        walk_expr_mut(self, expr);
        walk_query_mut(self, subquery);
    }

    fn visit_between(
        &mut self,
        _span: &mut &[Token<'_>],
        expr: &mut Expr<'_>,
        low: &mut Expr<'_>,
        high: &mut Expr<'_>,
        _not: bool,
    ) {
        walk_expr_mut(self, expr);
        walk_expr_mut(self, low);
        walk_expr_mut(self, high);
    }

    fn visit_binary_op(
        &mut self,
        _span: &mut &[Token<'_>],
        _op: &mut BinaryOperator,
        left: &mut Expr<'_>,
        right: &mut Expr<'_>,
    ) {
        walk_expr_mut(self, left);
        walk_expr_mut(self, right);
    }

    fn visit_unary_op(
        &mut self,
        _span: &mut &[Token<'_>],
        _op: &mut UnaryOperator,
        expr: &mut Expr<'_>,
    ) {
        walk_expr_mut(self, expr);
    }

    fn visit_cast(
        &mut self,
        _span: &mut &[Token<'_>],
        expr: &mut Expr<'_>,
        _target_type: &mut TypeName,
        _pg_style: bool,
    ) {
        walk_expr_mut(self, expr);
    }

    fn visit_try_cast(
        &mut self,
        _span: &mut &[Token<'_>],
        expr: &mut Expr<'_>,
        _target_type: &mut TypeName,
    ) {
        walk_expr_mut(self, expr);
    }

    fn visit_extract(
        &mut self,
        _span: &mut &[Token<'_>],
        _kind: &mut IntervalKind,
        expr: &mut Expr<'_>,
    ) {
        walk_expr_mut(self, expr);
    }

    fn visit_positon(
        &mut self,
        _span: &mut &[Token<'_>],
        substr_expr: &mut Expr<'_>,
        str_expr: &mut Expr<'_>,
    ) {
        walk_expr_mut(self, substr_expr);
        walk_expr_mut(self, str_expr);
    }

    fn visit_substring(
        &mut self,
        _span: &mut &[Token<'_>],
        expr: &mut Expr<'_>,
        substring_from: &mut Option<Box<Expr<'_>>>,
        substring_for: &mut Option<Box<Expr<'_>>>,
    ) {
        walk_expr_mut(self, expr);

        if let Some(substring_from) = substring_from {
            walk_expr_mut(self, substring_from);
        }

        if let Some(substring_for) = substring_for {
            walk_expr_mut(self, substring_for);
        }
    }

    fn visit_trim(
        &mut self,
        _span: &mut &[Token<'_>],
        expr: &mut Expr<'_>,
        trim_where: &mut Option<(TrimWhere, Box<Expr<'_>>)>,
    ) {
        walk_expr_mut(self, expr);

        if let Some((_, trim_where_expr)) = trim_where {
            walk_expr_mut(self, trim_where_expr);
        }
    }

    fn visit_literal(&mut self, _span: &mut &[Token<'_>], _lit: &mut Literal) {}

    fn visit_count_all(&mut self, _span: &mut &[Token<'_>]) {}

    fn visit_tuple(&mut self, _span: &mut &[Token<'_>], elements: &mut [Expr<'_>]) {
        for elem in elements.iter_mut() {
            walk_expr_mut(self, elem);
        }
    }

    fn visit_function_call(
        &mut self,
        _span: &mut &[Token<'_>],
        _distinct: bool,
        _name: &mut Identifier<'_>,
        args: &mut [Expr<'_>],
        _params: &mut [Literal],
    ) {
        for arg in args.iter_mut() {
            walk_expr_mut(self, arg);
        }
    }

    fn visit_case_when(
        &mut self,
        _span: &mut &[Token<'_>],
        operand: &mut Option<Box<Expr<'_>>>,
        conditions: &mut [Expr<'_>],
        results: &mut [Expr<'_>],
        else_result: &mut Option<Box<Expr<'_>>>,
    ) {
        if let Some(operand) = operand {
            walk_expr_mut(self, operand);
        }

        for condition in conditions.iter_mut() {
            walk_expr_mut(self, condition);
        }

        for result in results.iter_mut() {
            walk_expr_mut(self, result);
        }

        if let Some(else_result) = else_result {
            walk_expr_mut(self, else_result);
        }
    }

    fn visit_exists(&mut self, _span: &mut &[Token<'_>], _not: bool, subquery: &mut Query<'_>) {
        walk_query_mut(self, subquery);
    }

    fn visit_subquery(
        &mut self,
        _span: &mut &[Token<'_>],
        _modifier: &mut Option<SubqueryModifier>,
        subquery: &mut Query<'_>,
    ) {
        walk_query_mut(self, subquery);
    }

    fn visit_map_access(
        &mut self,
        _span: &mut &[Token<'_>],
        expr: &mut Expr<'_>,
        _accessor: &mut MapAccessor<'_>,
    ) {
        walk_expr_mut(self, expr);
    }

    fn visit_array(&mut self, _span: &mut &[Token<'_>], elements: &mut [Expr<'_>]) {
        for elem in elements.iter_mut() {
            walk_expr_mut(self, elem);
        }
    }

    fn visit_interval(
        &mut self,
        _span: &mut &[Token<'_>],
        expr: &mut Expr<'_>,
        _unit: &mut IntervalKind,
    ) {
        walk_expr_mut(self, expr);
    }

    fn visit_date_add(
        &mut self,
        _span: &mut &[Token<'_>],
        _unit: &mut IntervalKind,
        interval: &mut Expr<'_>,
        date: &mut Expr<'_>,
    ) {
        walk_expr_mut(self, date);
        walk_expr_mut(self, interval);
    }

    fn visit_date_sub(
        &mut self,
        _span: &mut &[Token<'_>],
        _unit: &mut IntervalKind,
        interval: &mut Expr<'_>,
        date: &mut Expr<'_>,
    ) {
        walk_expr_mut(self, date);
        walk_expr_mut(self, interval);
    }

    fn visit_date_trunc(
        &mut self,
        _span: &mut &[Token<'_>],
        _unit: &mut IntervalKind,
        date: &mut Expr<'_>,
    ) {
        walk_expr_mut(self, date);
    }

    fn visit_statement(&mut self, statement: &mut Statement<'_>) {
        walk_statement_mut(self, statement);
    }

    fn visit_query(&mut self, query: &mut Query<'_>) {
        walk_query_mut(self, query);
    }

    fn visit_explain(&mut self, _kind: &mut ExplainKind, _query: &mut Statement<'_>) {}

    fn visit_copy(&mut self, _copy: &mut CopyStmt<'_>) {}

    fn visit_copy_unit(&mut self, _copy_unit: &mut CopyUnit<'_>) {}

    fn visit_call(&mut self, _call: &mut CallStmt) {}

    fn visit_show_settings(&mut self, _like: &mut Option<String>) {}

    fn visit_show_process_list(&mut self) {}

    fn visit_show_metrics(&mut self) {}

    fn visit_show_engines(&mut self) {}

    fn visit_show_functions(&mut self, _limit: &mut Option<ShowLimit<'_>>) {}

    fn visit_show_limit(&mut self, _limit: &mut ShowLimit<'_>) {}

    fn visit_kill(&mut self, _kill_target: &mut KillTarget, _object_id: &mut String) {}

    fn visit_set_variable(
        &mut self,
        _is_global: bool,
        _variable: &mut Identifier<'_>,
        _value: &mut Literal,
    ) {
    }

    fn visit_insert(&mut self, _insert: &mut InsertStmt<'_>) {}

    fn visit_insert_source(&mut self, _insert_source: &mut InsertSource<'_>) {}

    fn visit_delete(
        &mut self,
        _table_reference: &mut TableReference<'_>,
        _selection: &mut Option<Expr<'_>>,
    ) {
    }

    fn visit_show_databases(&mut self, _stmt: &mut ShowDatabasesStmt<'_>) {}

    fn visit_show_create_databases(&mut self, _stmt: &mut ShowCreateDatabaseStmt<'_>) {}

    fn visit_create_database(&mut self, _stmt: &mut CreateDatabaseStmt<'_>) {}

    fn visit_drop_database(&mut self, _stmt: &mut DropDatabaseStmt<'_>) {}

    fn visit_undrop_database(&mut self, _stmt: &mut UndropDatabaseStmt<'_>) {}

    fn visit_alter_database(&mut self, _stmt: &mut AlterDatabaseStmt<'_>) {}

    fn visit_use_database(&mut self, _database: &mut Identifier<'_>) {}

    fn visit_show_tables(&mut self, _stmt: &mut ShowTablesStmt<'_>) {}

    fn visit_show_create_table(&mut self, _stmt: &mut ShowCreateTableStmt<'_>) {}

    fn visit_describe_table(&mut self, _stmt: &mut DescribeTableStmt<'_>) {}

    fn visit_show_tables_status(&mut self, _stmt: &mut ShowTablesStatusStmt<'_>) {}

    fn visit_create_table(&mut self, _stmt: &mut CreateTableStmt<'_>) {}

    fn visit_create_table_source(&mut self, _source: &mut CreateTableSource<'_>) {}

    fn visit_column_definition(&mut self, _column_definition: &mut ColumnDefinition<'_>) {}

    fn visit_drop_table(&mut self, _stmt: &mut DropTableStmt<'_>) {}

    fn visit_undrop_table(&mut self, _stmt: &mut UndropTableStmt<'_>) {}

    fn visit_alter_table(&mut self, _stmt: &mut AlterTableStmt<'_>) {}

    fn visit_rename_table(&mut self, _stmt: &mut RenameTableStmt<'_>) {}

    fn visit_truncate_table(&mut self, _stmt: &mut TruncateTableStmt<'_>) {}

    fn visit_optimize_table(&mut self, _stmt: &mut OptimizeTableStmt<'_>) {}

    fn visit_exists_table(&mut self, _stmt: &mut ExistsTableStmt<'_>) {}

    fn visit_create_view(&mut self, _stmt: &mut CreateViewStmt<'_>) {}

    fn visit_alter_view(&mut self, _stmt: &mut AlterViewStmt<'_>) {}

    fn visit_drop_view(&mut self, _stmt: &mut DropViewStmt<'_>) {}

    fn visit_show_users(&mut self) {}

    fn visit_create_user(&mut self, _stmt: &mut CreateUserStmt) {}

    fn visit_alter_user(&mut self, _stmt: &mut AlterUserStmt) {}

    fn visit_drop_user(&mut self, _if_exists: bool, _user: &mut UserIdentity) {}

    fn visit_show_roles(&mut self) {}

    fn visit_create_role(&mut self, _if_not_exists: bool, _role_name: &mut String) {}

    fn visit_drop_role(&mut self, _if_exists: bool, _role_name: &mut String) {}

    fn visit_grant(&mut self, _grant: &mut GrantStmt) {}

    fn visit_show_grant(&mut self, _principal: &mut Option<PrincipalIdentity>) {}

    fn visit_revoke(&mut self, _revoke: &mut RevokeStmt) {}

    fn visit_create_udf(
        &mut self,
        _if_not_exists: bool,
        _udf_name: &mut Identifier<'_>,
        _parameters: &mut [Identifier<'_>],
        _definition: &mut Expr<'_>,
        _description: &mut Option<String>,
    ) {
    }

    fn visit_drop_udf(&mut self, _if_exists: bool, _udf_name: &mut Identifier<'_>) {}

    fn visit_alter_udf(
        &mut self,
        _udf_name: &mut Identifier<'_>,
        _parameters: &mut [Identifier<'_>],
        _definition: &mut Expr<'_>,
        _description: &mut Option<String>,
    ) {
    }

    fn visit_create_stage(&mut self, _stmt: &mut CreateStageStmt) {}

    fn visit_show_stages(&mut self) {}

    fn visit_drop_stage(&mut self, _if_exists: bool, _stage_name: &mut String) {}

    fn visit_describe_stage(&mut self, _stage_name: &mut String) {}

    fn visit_remove_stage(&mut self, _location: &mut String, _pattern: &mut String) {}

    fn visit_list_stage(&mut self, _location: &mut String, _pattern: &mut String) {}

    fn visit_presign(&mut self, _presign: &mut PresignStmt) {}

    fn visit_create_share(&mut self, _stmt: &mut CreateShareStmt<'_>) {}

    fn visit_drop_share(&mut self, _stmt: &mut DropShareStmt<'_>) {}

    fn visit_grant_share_object(&mut self, _stmt: &mut GrantShareObjectStmt<'_>) {}

    fn visit_revoke_share_object(&mut self, _stmt: &mut RevokeShareObjectStmt<'_>) {}

    fn visit_alter_share_tenants(&mut self, _stmt: &mut AlterShareTenantsStmt<'_>) {}

    fn visit_desc_share(&mut self, _stmt: &mut DescShareStmt<'_>) {}

    fn visit_show_shares(&mut self, _stmt: &mut ShowSharesStmt) {}

    fn visit_show_object_grant_privileges(&mut self, _stmt: &mut ShowObjectGrantPrivilegesStmt) {}

    fn visit_show_grants_of_share(&mut self, _stmt: &mut ShowGrantsOfShareStmt) {}

    fn visit_with(&mut self, with: &mut With<'_>) {
        let With { ctes, .. } = with;
        for cte in ctes.iter_mut() {
            walk_cte_mut(self, cte);
        }
    }

    fn visit_set_expr(&mut self, expr: &mut SetExpr<'_>) {
        walk_set_expr_mut(self, expr);
    }

    fn visit_set_operation(&mut self, op: &mut SetOperation<'_>) {
        let SetOperation { left, right, .. } = op;

        walk_set_expr_mut(self, left);
        walk_set_expr_mut(self, right);
    }

    fn visit_order_by(&mut self, order_by: &mut OrderByExpr<'_>) {
        let OrderByExpr { expr, .. } = order_by;
        walk_expr_mut(self, expr);
    }

    fn visit_select_stmt(&mut self, stmt: &mut SelectStmt<'_>) {
        let SelectStmt {
            select_list,
            from,
            selection,
            group_by,
            having,
            ..
        } = stmt;

        for target in select_list.iter_mut() {
            walk_select_target_mut(self, target);
        }

        for table_ref in from.iter_mut() {
            walk_table_reference_mut(self, table_ref);
        }

        if let Some(selection) = selection {
            walk_expr_mut(self, selection);
        }

        for expr in group_by.iter_mut() {
            walk_expr_mut(self, expr);
        }

        if let Some(having) = having {
            walk_expr_mut(self, having);
        }
    }

    fn visit_select_target(&mut self, target: &mut SelectTarget<'_>) {
        walk_select_target_mut(self, target);
    }

    fn visit_table_reference(&mut self, table: &mut TableReference<'_>) {
        walk_table_reference_mut(self, table);
    }

    fn visit_time_travel_point(&mut self, time: &mut TimeTravelPoint<'_>) {
        walk_time_travel_point_mut(self, time);
    }

    fn visit_join(&mut self, join: &mut Join<'_>) {
        let Join {
            left,
            right,
            condition,
            ..
        } = join;

        walk_table_reference_mut(self, left);
        walk_table_reference_mut(self, right);

        walk_join_condition_mut(self, condition);
    }
}
