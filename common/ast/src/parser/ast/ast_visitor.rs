// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;

use crate::parser::ast::BinaryOperator;
use crate::parser::ast::ColumnDefinition;
use crate::parser::ast::Expr;
use crate::parser::ast::Identifier;
use crate::parser::ast::Indirection;
use crate::parser::ast::Join;
use crate::parser::ast::JoinCondition;
use crate::parser::ast::Literal;
use crate::parser::ast::OrderByExpr;
use crate::parser::ast::Query;
use crate::parser::ast::SQLProperty;
use crate::parser::ast::SelectStmt;
use crate::parser::ast::SelectTarget;
use crate::parser::ast::SetExpr;
use crate::parser::ast::SetOperator;
use crate::parser::ast::Statement;
use crate::parser::ast::TableAlias;
use crate::parser::ast::TableReference;
use crate::parser::ast::TypeName;
use crate::parser::ast::UnaryOperator;

pub trait AstVisitor {
    fn visit_expr(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::ColumnRef { .. } => self.visit_column_ref(),
            Expr::IsNull { expr, not } => self.visit_is_null(expr, not),
            Expr::InList { expr, list, not } => self.visit_in_list(expr, list, not),
            Expr::InSubquery {
                expr,
                subquery,
                not,
            } => self.visit_in_subquery(expr, subquery, not),
            Expr::Between {
                expr,
                low,
                high,
                not,
            } => self.visit_between(expr, low, high, not),
            Expr::BinaryOp { op, left, right } => self.visit_binary_op(op, left, right),
            Expr::UnaryOp { op, expr } => self.visit_unary_op(op, expr),
            Expr::Cast { expr, target_type } => self.visit_cast(expr, target_type),
            Expr::Literal(_) => self.visit_literal(),
            Expr::CountAll => self.visit_count_all(),
            Expr::FunctionCall {
                distinct,
                name,
                args,
                params,
            } => self.visit_function_call(distinct, name, args, params),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => self.visit_case(operand, conditions, results, else_result),
            Expr::Exists(query) => self.visit_exists(query),
            Expr::Subquery(query) => self.visit_query(query),
        }
    }

    fn visit_is_null(&mut self, expr: &Expr, _not: &bool) -> Result<()> {
        self.visit_expr(expr)
    }

    fn visit_in_list(&mut self, expr: &Expr, exprs: &[Expr], _not: &bool) -> Result<()> {
        self.visit_expr(expr)?;
        self.visit_exprs(exprs)
    }

    fn visit_in_subquery(&mut self, expr: &Expr, query: &Query, _not: &bool) -> Result<()> {
        self.visit_expr(expr)?;
        self.visit_query(query)
    }

    fn visit_column_ref(&mut self) -> Result<()> {
        Ok(())
    }

    fn visit_literal(&mut self) -> Result<()> {
        Ok(())
    }

    fn visit_count_all(&mut self) -> Result<()> {
        Ok(())
    }

    fn visit_binary_op(&mut self, _: &BinaryOperator, left: &Expr, right: &Expr) -> Result<()> {
        self.visit_expr(left)?;
        self.visit_expr(right)
    }

    fn visit_between(&mut self, expr: &Expr, low: &Expr, high: &Expr, _not: &bool) -> Result<()> {
        self.visit_expr(expr)?;
        self.visit_expr(low)?;
        self.visit_expr(high)
    }

    fn visit_unary_op(&mut self, _: &UnaryOperator, expr: &Expr) -> Result<()> {
        self.visit_expr(expr)
    }

    fn visit_cast(&mut self, expr: &Expr, _type_name: &TypeName) -> Result<()> {
        self.visit_expr(expr)
    }

    fn visit_function_call(
        &mut self,
        _: &bool,
        _: &str,
        exprs: &[Expr],
        _: &[Literal],
    ) -> Result<()> {
        self.visit_exprs(exprs)
    }

    fn visit_case(
        &mut self,
        operand: &Option<Box<Expr>>,
        conditions: &[Expr],
        results: &[Expr],
        else_result: &Option<Box<Expr>>,
    ) -> Result<()> {
        self.visit_expr(operand.as_ref().unwrap())?;
        self.visit_exprs(conditions)?;
        self.visit_exprs(results)?;
        self.visit_expr(else_result.as_ref().unwrap())
    }

    fn visit_exists(&mut self, query: &Query) -> Result<()> {
        self.visit_query(query)
    }

    fn visit_exprs(&mut self, exprs: &[Expr]) -> Result<()> {
        for expr in exprs {
            self.visit_expr(expr)?
        }
        Ok(())
    }

    fn visit_query(&mut self, query: &Query) -> Result<()> {
        self.visit_set_expr(&query.body)?;
        self.visit_order_by_exprs(&query.order_by)?;
        self.visit_expr(query.limit.as_ref().unwrap())
    }

    fn visit_set_expr(&mut self, set_expr: &SetExpr) -> Result<()> {
        match set_expr {
            SetExpr::Select(select_stmt) => self.visit_select_stmt(select_stmt),
            SetExpr::Query(query) => self.visit_query(query),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => self.visit_set_operation(op, all, left, right),
        }
    }

    fn visit_order_by_expr(&mut self, order_by_expr: &OrderByExpr) -> Result<()> {
        self.visit_expr(&order_by_expr.expr)
    }

    fn visit_order_by_exprs(&mut self, exprs: &[OrderByExpr]) -> Result<()> {
        for expr in exprs {
            self.visit_order_by_expr(expr)?;
        }
        Ok(())
    }

    fn visit_set_operation(
        &mut self,
        _: &SetOperator,
        _: &bool,
        left: &SetExpr,
        right: &SetExpr,
    ) -> Result<()> {
        self.visit_set_expr(left)?;
        self.visit_set_expr(right)
    }

    fn visit_select_stmt(&mut self, select_stmt: &SelectStmt) -> Result<()> {
        self.visit_select_targets(&select_stmt.select_list)?;
        self.visit_table_reference(&select_stmt.from)?;
        self.visit_expr(select_stmt.selection.as_ref().unwrap())?;
        self.visit_exprs(&select_stmt.group_by)?;
        self.visit_expr(select_stmt.having.as_ref().unwrap())
    }

    fn visit_select_target(&mut self, select_target: &SelectTarget) -> Result<()> {
        match select_target {
            SelectTarget::Projection { expr, alias } => self.visit_projection(expr, alias),
            SelectTarget::Indirections(indirections) => self.visit_indirections(indirections),
        }
    }

    fn visit_projection(&mut self, expr: &Expr, _: &Option<Identifier>) -> Result<()> {
        self.visit_expr(expr)
    }

    fn visit_indirections(&mut self, _: &[Indirection]) -> Result<()> {
        Ok(())
    }

    fn visit_select_targets(&mut self, select_targets: &[SelectTarget]) -> Result<()> {
        for select_target in select_targets {
            self.visit_select_target(select_target)?;
        }
        Ok(())
    }

    fn visit_table_reference(&mut self, table_reference: &TableReference) -> Result<()> {
        match table_reference {
            TableReference::Table {
                database,
                table,
                alias,
            } => self.visit_table(database, table, alias),
            TableReference::Subquery { subquery, alias } => {
                self.visit_table_subquery(subquery, alias)
            }
            TableReference::TableFunction { expr, alias } => self.visit_table_function(expr, alias),
            TableReference::Join(join) => self.visit_join(join),
        }
    }

    fn visit_table(
        &mut self,
        _: &Option<Identifier>,
        _: &Identifier,
        alias: &Option<TableAlias>,
    ) -> Result<()> {
        self.visit_table_alias(alias.as_ref().unwrap())
    }

    fn visit_table_subquery(&mut self, subquery: &Query, alias: &Option<TableAlias>) -> Result<()> {
        self.visit_query(subquery)?;
        self.visit_table_alias(alias.as_ref().unwrap())
    }

    fn visit_table_function(&mut self, expr: &Expr, alias: &Option<TableAlias>) -> Result<()> {
        self.visit_expr(expr)?;
        self.visit_table_alias(alias.as_ref().unwrap())
    }

    fn visit_join(&mut self, join: &Join) -> Result<()> {
        self.visit_join_condition(&join.condition)?;
        self.visit_table_reference(&join.left)?;
        self.visit_table_reference(&join.right)
    }

    fn visit_join_condition(&mut self, join_condition: &JoinCondition) -> Result<()> {
        match join_condition {
            JoinCondition::On(expr) => self.visit_expr(expr),
            JoinCondition::Using(_) => Ok(()),
            JoinCondition::Natural => Ok(()),
            JoinCondition::None => Ok(()),
        }
    }

    fn visit_table_alias(&mut self, _: &TableAlias) -> Result<()> {
        Ok(())
    }

    // statement
    fn visit_statement(&mut self, statement: &Statement) -> Result<()> {
        match statement {
            Statement::Explain { analyze, query } => self.visit_explain(analyze, query),
            Statement::Select(query) => self.visit_select(query),
            Statement::ShowTables => self.visit_show_tables(),
            Statement::ShowDatabases => self.visit_show_databases(),
            Statement::ShowSettings => self.visit_show_settings(),
            Statement::ShowProcessList => self.visit_show_process_list(),
            Statement::ShowCreateTable { database, table } => {
                self.visit_show_create_table(database, table)
            }
            Statement::CreateTable {
                if_not_exists,
                database,
                table,
                columns,
                engine,
                options,
                ..
            } => self.visit_create_table(if_not_exists, database, table, columns, engine, options),
            Statement::Describe { database, table } => self.visit_describe(database, table),
            Statement::DropTable {
                if_exists,
                database,
                table,
            } => self.visit_drop_table(if_exists, database, table),
            Statement::TruncateTable { database, table } => {
                self.visit_truncate_table(database, table)
            }
            Statement::CreateDatabase {
                if_not_exists,
                name,
                engine,
                options,
            } => self.visit_create_database(if_not_exists, name, engine, options),
            Statement::UseDatabase { name } => self.visit_use_database(name),
            Statement::KillStmt { object_id } => self.visit_kill_stmt(object_id),
        }
    }

    fn visit_explain(&mut self, _: &bool, query: &Statement) -> Result<()> {
        self.visit_statement(query)
    }

    fn visit_select(&mut self, query: &Query) -> Result<()> {
        self.visit_query(query)
    }

    fn visit_show_tables(&mut self) -> Result<()> {
        Ok(())
    }

    fn visit_show_databases(&mut self) -> Result<()> {
        Ok(())
    }

    fn visit_show_settings(&mut self) -> Result<()> {
        Ok(())
    }

    fn visit_show_process_list(&mut self) -> Result<()> {
        Ok(())
    }

    fn visit_show_create_table(&mut self, _: &Option<Identifier>, _: &Identifier) -> Result<()> {
        Ok(())
    }

    fn visit_create_table(
        &mut self,
        _: &bool,
        _: &Option<Identifier>,
        _: &Identifier,
        _: &[ColumnDefinition],
        _: &str,
        _: &[SQLProperty],
    ) -> Result<()> {
        Ok(())
    }

    fn visit_describe(&mut self, _: &Option<Identifier>, _: &Identifier) -> Result<()> {
        Ok(())
    }

    fn visit_drop_table(&mut self, _: &bool, _: &Option<Identifier>, _: &Identifier) -> Result<()> {
        Ok(())
    }

    fn visit_truncate_table(&mut self, _: &Option<Identifier>, _: &Identifier) -> Result<()> {
        Ok(())
    }

    fn visit_create_database(
        &mut self,
        _: &bool,
        _: &Identifier,
        _: &str,
        _: &[SQLProperty],
    ) -> Result<()> {
        Ok(())
    }

    fn visit_use_database(&mut self, _: &Identifier) -> Result<()> {
        Ok(())
    }

    fn visit_kill_stmt(&mut self, _: &Identifier) -> Result<()> {
        Ok(())
    }
}
