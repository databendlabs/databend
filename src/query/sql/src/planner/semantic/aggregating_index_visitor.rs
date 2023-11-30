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

use common_ast::ast::ColumnID;
use common_ast::ast::Expr;
use common_ast::ast::GroupBy;
use common_ast::ast::Identifier;
use common_ast::ast::Lambda;
use common_ast::ast::Literal;
use common_ast::ast::Query;
use common_ast::ast::SelectStmt;
use common_ast::ast::SelectTarget;
use common_ast::ast::SetExpr;
use common_ast::ast::TableReference;
use common_ast::ast::Window;
use common_ast::walk_expr;
use common_ast::walk_select_target;
use common_ast::walk_select_target_mut;
use common_ast::Visitor;
use common_ast::VisitorMut;
use common_exception::Span;
use common_expression::BLOCK_NAME_COL_NAME;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::BUILTIN_FUNCTIONS;

use crate::planner::SUPPORTED_AGGREGATING_INDEX_FUNCTIONS;

#[derive(Debug, Clone, Default)]
pub struct AggregatingIndexRewriter {
    pub user_defined_block_name: bool,
    has_agg_function: bool,
}

#[derive(Debug, Clone, Default)]
pub struct AggregatingIndexChecker {
    not_support: bool,
}

impl AggregatingIndexChecker {
    pub fn is_supported(&self) -> bool {
        !self.not_support
    }
}

impl VisitorMut for AggregatingIndexRewriter {
    fn visit_expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::FunctionCall {
                distinct,
                name,
                args,
                window,
                ..
            } if !*distinct
                && args.len() == 1
                && SUPPORTED_AGGREGATING_INDEX_FUNCTIONS
                    .contains(&&*name.name.to_ascii_lowercase().to_lowercase())
                && window.is_none() =>
            {
                self.has_agg_function = true;
                name.name = format!("{}_STATE", name.name);
            }
            Expr::CountAll { window, .. } if window.is_none() => {
                self.has_agg_function = true;
                *expr = Expr::FunctionCall {
                    span: None,
                    distinct: false,
                    name: Identifier {
                        name: "COUNT_STATE".to_string(),
                        quote: None,
                        span: None,
                    },
                    args: vec![],
                    params: vec![],
                    window: None,
                    lambda: None,
                };
            }
            _ => {}
        }
    }

    fn visit_select_stmt(&mut self, stmt: &mut SelectStmt) {
        let SelectStmt {
            select_list,
            from,
            group_by,
            ..
        } = stmt;

        for target in select_list.iter_mut() {
            walk_select_target_mut(self, target);
        }

        let table = {
            let table_ref = from.first().unwrap();
            match table_ref {
                TableReference::Table { table, .. } => table.clone(),
                _ => unreachable!(),
            }
        };

        let block_name_expr = Expr::ColumnRef {
            span: None,
            database: None,
            table: Some(table),
            column: ColumnID::Name(Identifier::from_name(BLOCK_NAME_COL_NAME)),
        };

        // if select list already contains `BLOCK_NAME_COL_NAME`
        if select_list.iter().any(|target| match target {
            SelectTarget::AliasedExpr { expr, .. } => match (*expr).clone().as_ref() {
                Expr::ColumnRef { column, .. } => {
                    column.name().eq_ignore_ascii_case(BLOCK_NAME_COL_NAME)
                }

                _ => false,
            },
            SelectTarget::StarColumns { .. } => false,
        }) {
            self.user_defined_block_name = true;
        } else {
            select_list.extend_one(SelectTarget::AliasedExpr {
                expr: Box::new(block_name_expr.clone()),
                alias: None,
            });
        }

        match group_by {
            Some(group_by) => match group_by {
                GroupBy::Normal(groups) => {
                    if !groups.iter().any(|expr| match (*expr).clone() {
                        Expr::ColumnRef { column, .. } => {
                            column.name().eq_ignore_ascii_case(BLOCK_NAME_COL_NAME)
                        }
                        _ => false,
                    }) {
                        groups.extend_one(block_name_expr)
                    }
                }
                _ => unreachable!(),
            },
            None if self.has_agg_function => {
                let groups = vec![block_name_expr];
                *group_by = Some(GroupBy::Normal(groups));
            }
            _ => {}
        }
    }
}

impl<'ast> Visitor<'ast> for AggregatingIndexChecker {
    fn visit_function_call(
        &mut self,
        _span: Span,
        _distinct: bool,
        name: &'ast Identifier,
        args: &'ast [Expr],
        _params: &'ast [Literal],
        _over: &'ast Option<Window>,
        _lambda: &'ast Option<Lambda>,
    ) {
        if self.not_support {
            return;
        }

        // is agg func but not support now.
        if AggregateFunctionFactory::instance().contains(&name.name)
            && !SUPPORTED_AGGREGATING_INDEX_FUNCTIONS.contains(&&*name.name.to_lowercase())
        {
            self.not_support = true;
            return;
        }

        self.not_support = BUILTIN_FUNCTIONS
            .get_property(&name.name)
            .map(|p| p.non_deterministic)
            .unwrap_or(false);

        for arg in args {
            walk_expr(self, arg);
        }
    }

    fn visit_select_stmt(&mut self, stmt: &'ast SelectStmt) {
        if self.not_support {
            return;
        }
        if stmt.having.is_some() || stmt.window_list.is_some() {
            self.not_support = true;
            return;
        }
        if let Some(selection) = &stmt.selection {
            walk_expr(self, selection);
        }
        match &stmt.group_by {
            None => {}
            Some(group_by) => match group_by {
                GroupBy::Normal(exprs) => {
                    for expr in exprs {
                        walk_expr(self, expr);
                    }
                }
                _ => {
                    self.not_support = true;
                    return;
                }
            },
        }
        for target in &stmt.select_list {
            if target.has_window() {
                self.not_support = true;
                return;
            }
            walk_select_target(self, target);
        }
    }

    fn visit_query(&mut self, query: &'ast Query) {
        if self.not_support {
            return;
        }
        if query.with.is_some() || !query.order_by.is_empty() || !query.limit.is_empty() {
            self.not_support = true;
            return;
        }

        if !matches!(&query.body, SetExpr::Select(_)) {
            self.not_support = true;
            return;
        }

        self.visit_set_expr(&query.body);

        for order_by in &query.order_by {
            self.visit_order_by(order_by);
        }
    }
}
