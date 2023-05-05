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

use common_exception::Span;
use common_meta_app::principal::FileFormatOptionsAst;
use common_meta_app::principal::PrincipalIdentity;
use common_meta_app::principal::UserIdentity;

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

pub trait VisitorMut: Sized {
    fn visit_expr(&mut self, expr: &mut Expr) {
        walk_expr_mut(self, expr);
    }

    fn visit_identifier(&mut self, _ident: &mut Identifier) {}

    fn visit_database_ref(&mut self, catalog: &mut Option<Identifier>, database: &mut Identifier) {
        if let Some(catalog) = catalog {
            walk_identifier_mut(self, catalog);
        }

        walk_identifier_mut(self, database);
    }

    fn visit_table_ref(
        &mut self,
        catalog: &mut Option<Identifier>,
        database: &mut Option<Identifier>,
        table: &mut Identifier,
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
        _span: Span,
        database: &mut Option<Identifier>,
        table: &mut Option<Identifier>,
        column: &mut Identifier,
    ) {
        if let Some(database) = database {
            walk_identifier_mut(self, database);
        }

        if let Some(table) = table {
            walk_identifier_mut(self, table);
        }

        walk_identifier_mut(self, column);
    }

    fn visit_is_null(&mut self, _span: Span, expr: &mut Expr, _not: bool) {
        walk_expr_mut(self, expr);
    }

    fn visit_is_distinct_from(
        &mut self,
        _span: Span,
        left: &mut Expr,
        right: &mut Expr,
        _not: bool,
    ) {
        walk_expr_mut(self, left);
        walk_expr_mut(self, right);
    }

    fn visit_in_list(&mut self, _span: Span, expr: &mut Expr, list: &mut [Expr], _not: bool) {
        walk_expr_mut(self, expr);
        for expr in list {
            walk_expr_mut(self, expr);
        }
    }

    fn visit_in_subquery(
        &mut self,
        _span: Span,
        expr: &mut Expr,
        subquery: &mut Query,
        _not: bool,
    ) {
        walk_expr_mut(self, expr);
        walk_query_mut(self, subquery);
    }

    fn visit_between(
        &mut self,
        _span: Span,
        expr: &mut Expr,
        low: &mut Expr,
        high: &mut Expr,
        _not: bool,
    ) {
        walk_expr_mut(self, expr);
        walk_expr_mut(self, low);
        walk_expr_mut(self, high);
    }

    fn visit_binary_op(
        &mut self,
        _span: Span,
        _op: &mut BinaryOperator,
        left: &mut Expr,
        right: &mut Expr,
    ) {
        walk_expr_mut(self, left);
        walk_expr_mut(self, right);
    }

    fn visit_unary_op(&mut self, _span: Span, _op: &mut UnaryOperator, expr: &mut Expr) {
        walk_expr_mut(self, expr);
    }

    fn visit_cast(
        &mut self,
        _span: Span,
        expr: &mut Expr,
        _target_type: &mut TypeName,
        _pg_style: bool,
    ) {
        walk_expr_mut(self, expr);
    }

    fn visit_try_cast(&mut self, _span: Span, expr: &mut Expr, _target_type: &mut TypeName) {
        walk_expr_mut(self, expr);
    }

    fn visit_extract(&mut self, _span: Span, _kind: &mut IntervalKind, expr: &mut Expr) {
        walk_expr_mut(self, expr);
    }

    fn visit_position(&mut self, _span: Span, substr_expr: &mut Expr, str_expr: &mut Expr) {
        walk_expr_mut(self, substr_expr);
        walk_expr_mut(self, str_expr);
    }

    fn visit_substring(
        &mut self,
        _span: Span,
        expr: &mut Expr,
        substring_from: &mut Box<Expr>,
        substring_for: &mut Option<Box<Expr>>,
    ) {
        walk_expr_mut(self, expr);
        walk_expr_mut(self, substring_from);

        if let Some(substring_for) = substring_for {
            walk_expr_mut(self, substring_for);
        }
    }

    fn visit_trim(
        &mut self,
        _span: Span,
        expr: &mut Expr,
        trim_where: &mut Option<(TrimWhere, Box<Expr>)>,
    ) {
        walk_expr_mut(self, expr);

        if let Some((_, trim_where_expr)) = trim_where {
            walk_expr_mut(self, trim_where_expr);
        }
    }

    fn visit_literal(&mut self, _span: Span, _lit: &mut Literal) {}

    fn visit_count_all(&mut self, _span: Span) {}

    fn visit_tuple(&mut self, _span: Span, elements: &mut [Expr]) {
        for elem in elements.iter_mut() {
            walk_expr_mut(self, elem);
        }
    }

    fn visit_function_call(
        &mut self,
        _span: Span,
        _distinct: bool,
        _name: &mut Identifier,
        args: &mut [Expr],
        _params: &mut [Literal],
        over: &mut Option<Window>,
    ) {
        for arg in args.iter_mut() {
            walk_expr_mut(self, arg);
        }

        if let Some(over) = over {
            match over {
                Window::WindowReference(reference) => {
                    self.visit_identifier(&mut reference.window_name);
                }
                Window::WindowSpec(spec) => {
                    spec.partition_by
                        .iter_mut()
                        .for_each(|expr| walk_expr_mut(self, expr));
                    spec.order_by
                        .iter_mut()
                        .for_each(|expr| walk_expr_mut(self, &mut expr.expr));

                    if let Some(frame) = &mut spec.window_frame {
                        self.visit_frame_bound(&mut frame.start_bound);
                        self.visit_frame_bound(&mut frame.end_bound);
                    }
                }
            }
        }
    }

    fn visit_frame_bound(&mut self, bound: &mut WindowFrameBound) {
        match bound {
            WindowFrameBound::Preceding(Some(expr)) => walk_expr_mut(self, expr.as_mut()),
            WindowFrameBound::Following(Some(expr)) => walk_expr_mut(self, expr.as_mut()),
            _ => {}
        }
    }

    fn visit_case_when(
        &mut self,
        _span: Span,
        operand: &mut Option<Box<Expr>>,
        conditions: &mut [Expr],
        results: &mut [Expr],
        else_result: &mut Option<Box<Expr>>,
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

    fn visit_exists(&mut self, _span: Span, _not: bool, subquery: &mut Query) {
        walk_query_mut(self, subquery);
    }

    fn visit_subquery(
        &mut self,
        _span: Span,
        _modifier: &mut Option<SubqueryModifier>,
        subquery: &mut Query,
    ) {
        walk_query_mut(self, subquery);
    }

    fn visit_map_access(&mut self, _span: Span, expr: &mut Expr, _accessor: &mut MapAccessor) {
        walk_expr_mut(self, expr);
    }

    fn visit_array(&mut self, _span: Span, elements: &mut [Expr]) {
        for elem in elements.iter_mut() {
            walk_expr_mut(self, elem);
        }
    }

    fn visit_map(&mut self, _span: Span, kvs: &mut [(Expr, Expr)]) {
        for (key_expr, val_expr) in kvs {
            walk_expr_mut(self, key_expr);
            walk_expr_mut(self, val_expr);
        }
    }

    fn visit_interval(&mut self, _span: Span, expr: &mut Expr, _unit: &mut IntervalKind) {
        walk_expr_mut(self, expr);
    }

    fn visit_date_add(
        &mut self,
        _span: Span,
        _unit: &mut IntervalKind,
        interval: &mut Expr,
        date: &mut Expr,
    ) {
        walk_expr_mut(self, date);
        walk_expr_mut(self, interval);
    }

    fn visit_date_sub(
        &mut self,
        _span: Span,
        _unit: &mut IntervalKind,
        interval: &mut Expr,
        date: &mut Expr,
    ) {
        walk_expr_mut(self, date);
        walk_expr_mut(self, interval);
    }

    fn visit_date_trunc(&mut self, _span: Span, _unit: &mut IntervalKind, date: &mut Expr) {
        walk_expr_mut(self, date);
    }

    fn visit_statement(&mut self, statement: &mut Statement) {
        walk_statement_mut(self, statement);
    }

    fn visit_query(&mut self, query: &mut Query) {
        walk_query_mut(self, query);
    }

    fn visit_explain(&mut self, _kind: &mut ExplainKind, stmt: &mut Statement) {
        walk_statement_mut(self, stmt);
    }

    fn visit_copy(&mut self, _copy: &mut CopyStmt) {}

    fn visit_copy_unit(&mut self, _copy_unit: &mut CopyUnit) {}

    fn visit_call(&mut self, _call: &mut CallStmt) {}

    fn visit_show_settings(&mut self, _like: &mut Option<String>) {}

    fn visit_show_process_list(&mut self) {}

    fn visit_show_metrics(&mut self) {}

    fn visit_show_engines(&mut self) {}

    fn visit_show_functions(&mut self, _limit: &mut Option<ShowLimit>) {}

    fn visit_show_table_functions(&mut self, _limit: &mut Option<ShowLimit>) {}

    fn visit_show_limit(&mut self, _limit: &mut ShowLimit) {}

    fn visit_kill(&mut self, _kill_target: &mut KillTarget, _object_id: &mut String) {}

    fn visit_set_variable(
        &mut self,
        _is_global: bool,
        _variable: &mut Identifier,
        _value: &mut Box<Expr>,
    ) {
    }

    fn visit_unset_variable(&mut self, _stmt: &mut UnSetStmt) {}

    fn visit_set_role(&mut self, _is_default: bool, _role_name: &mut String) {}

    fn visit_insert(&mut self, _insert: &mut InsertStmt) {}
    fn visit_replace(&mut self, _replace: &mut ReplaceStmt) {}

    fn visit_insert_source(&mut self, _insert_source: &mut InsertSource) {}

    fn visit_delete(
        &mut self,
        _table_reference: &mut TableReference,
        _selection: &mut Option<Expr>,
    ) {
    }

    fn visit_update(&mut self, _update: &mut UpdateStmt) {}

    fn visit_show_catalogs(&mut self, _stmt: &mut ShowCatalogsStmt) {}

    fn visit_show_create_catalog(&mut self, _stmt: &mut ShowCreateCatalogStmt) {}

    fn visit_create_catalog(&mut self, _stmt: &mut CreateCatalogStmt) {}

    fn visit_drop_catalog(&mut self, _stmt: &mut DropCatalogStmt) {}

    fn visit_show_databases(&mut self, _stmt: &mut ShowDatabasesStmt) {}

    fn visit_show_create_databases(&mut self, _stmt: &mut ShowCreateDatabaseStmt) {}

    fn visit_create_database(&mut self, _stmt: &mut CreateDatabaseStmt) {}

    fn visit_drop_database(&mut self, _stmt: &mut DropDatabaseStmt) {}

    fn visit_undrop_database(&mut self, _stmt: &mut UndropDatabaseStmt) {}

    fn visit_alter_database(&mut self, _stmt: &mut AlterDatabaseStmt) {}

    fn visit_use_database(&mut self, _database: &mut Identifier) {}

    fn visit_show_tables(&mut self, _stmt: &mut ShowTablesStmt) {}

    fn visit_show_columns(&mut self, _stmt: &mut ShowColumnsStmt) {}

    fn visit_show_create_table(&mut self, _stmt: &mut ShowCreateTableStmt) {}

    fn visit_describe_table(&mut self, _stmt: &mut DescribeTableStmt) {}

    fn visit_show_tables_status(&mut self, _stmt: &mut ShowTablesStatusStmt) {}

    fn visit_create_table(&mut self, _stmt: &mut CreateTableStmt) {}

    fn visit_create_table_source(&mut self, _source: &mut CreateTableSource) {}

    fn visit_column_definition(&mut self, _column_definition: &mut ColumnDefinition) {}

    fn visit_drop_table(&mut self, _stmt: &mut DropTableStmt) {}

    fn visit_undrop_table(&mut self, _stmt: &mut UndropTableStmt) {}

    fn visit_alter_table(&mut self, _stmt: &mut AlterTableStmt) {}

    fn visit_rename_table(&mut self, _stmt: &mut RenameTableStmt) {}

    fn visit_truncate_table(&mut self, _stmt: &mut TruncateTableStmt) {}

    fn visit_optimize_table(&mut self, _stmt: &mut OptimizeTableStmt) {}

    fn visit_vacuum_table(&mut self, _stmt: &mut VacuumTableStmt) {}

    fn visit_analyze_table(&mut self, _stmt: &mut AnalyzeTableStmt) {}

    fn visit_exists_table(&mut self, _stmt: &mut ExistsTableStmt) {}

    fn visit_create_view(&mut self, _stmt: &mut CreateViewStmt) {}

    fn visit_alter_view(&mut self, _stmt: &mut AlterViewStmt) {}

    fn visit_drop_view(&mut self, _stmt: &mut DropViewStmt) {}

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
        _udf_name: &mut Identifier,
        _parameters: &mut [Identifier],
        _definition: &mut Expr,
        _description: &mut Option<String>,
    ) {
    }

    fn visit_drop_udf(&mut self, _if_exists: bool, _udf_name: &mut Identifier) {}

    fn visit_alter_udf(
        &mut self,
        _udf_name: &mut Identifier,
        _parameters: &mut [Identifier],
        _definition: &mut Expr,
        _description: &mut Option<String>,
    ) {
    }

    fn visit_create_stage(&mut self, _stmt: &mut CreateStageStmt) {}

    fn visit_show_stages(&mut self) {}

    fn visit_drop_stage(&mut self, _if_exists: bool, _stage_name: &mut String) {}

    fn visit_describe_stage(&mut self, _stage_name: &mut String) {}

    fn visit_remove_stage(&mut self, _location: &mut String, _pattern: &mut String) {}

    fn visit_list_stage(&mut self, _location: &mut String, _pattern: &mut String) {}

    fn visit_create_file_format(
        &mut self,
        _if_not_exists: bool,
        _name: &mut String,
        _file_format_options: &mut FileFormatOptionsAst,
    ) {
    }

    fn visit_drop_file_format(&mut self, _if_exists: bool, _name: &mut String) {}

    fn visit_show_file_formats(&mut self) {}

    fn visit_presign(&mut self, _presign: &mut PresignStmt) {}

    fn visit_create_share_endpoint(&mut self, _stmt: &mut CreateShareEndpointStmt) {}

    fn visit_show_share_endpoint(&mut self, _stmt: &mut ShowShareEndpointStmt) {}

    fn visit_drop_share_endpoint(&mut self, _stmt: &mut DropShareEndpointStmt) {}

    fn visit_create_share(&mut self, _stmt: &mut CreateShareStmt) {}

    fn visit_drop_share(&mut self, _stmt: &mut DropShareStmt) {}

    fn visit_grant_share_object(&mut self, _stmt: &mut GrantShareObjectStmt) {}

    fn visit_revoke_share_object(&mut self, _stmt: &mut RevokeShareObjectStmt) {}

    fn visit_alter_share_tenants(&mut self, _stmt: &mut AlterShareTenantsStmt) {}

    fn visit_desc_share(&mut self, _stmt: &mut DescShareStmt) {}

    fn visit_show_shares(&mut self, _stmt: &mut ShowSharesStmt) {}

    fn visit_show_object_grant_privileges(&mut self, _stmt: &mut ShowObjectGrantPrivilegesStmt) {}

    fn visit_show_grants_of_share(&mut self, _stmt: &mut ShowGrantsOfShareStmt) {}

    fn visit_with(&mut self, with: &mut With) {
        let With { ctes, .. } = with;
        for cte in ctes.iter_mut() {
            walk_cte_mut(self, cte);
        }
    }

    fn visit_set_expr(&mut self, expr: &mut SetExpr) {
        walk_set_expr_mut(self, expr);
    }

    fn visit_set_operation(&mut self, op: &mut SetOperation) {
        let SetOperation { left, right, .. } = op;

        walk_set_expr_mut(self, left);
        walk_set_expr_mut(self, right);
    }

    fn visit_order_by(&mut self, order_by: &mut OrderByExpr) {
        let OrderByExpr { expr, .. } = order_by;
        walk_expr_mut(self, expr);
    }

    fn visit_select_stmt(&mut self, stmt: &mut SelectStmt) {
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

        match group_by {
            Some(GroupBy::Normal(exprs)) => {
                for expr in exprs {
                    walk_expr_mut(self, expr);
                }
            }
            Some(GroupBy::GroupingSets(sets)) => {
                for set in sets {
                    for expr in set {
                        walk_expr_mut(self, expr);
                    }
                }
            }
            _ => {}
        }

        if let Some(having) = having {
            walk_expr_mut(self, having);
        }
    }

    fn visit_select_target(&mut self, target: &mut SelectTarget) {
        walk_select_target_mut(self, target);
    }

    fn visit_table_reference(&mut self, table: &mut TableReference) {
        walk_table_reference_mut(self, table);
    }

    fn visit_time_travel_point(&mut self, time: &mut TimeTravelPoint) {
        walk_time_travel_point_mut(self, time);
    }

    fn visit_join(&mut self, join: &mut Join) {
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
