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

use std::collections::HashSet;

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::TableReference;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_expression::BLOCK_NAME_COL_NAME;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::BUILTIN_FUNCTIONS;
use derive_visitor::DriveMut;
use derive_visitor::Visitor;
use derive_visitor::VisitorMut;
use itertools::Itertools;

use crate::planner::SUPPORTED_AGGREGATING_INDEX_FUNCTIONS;

#[derive(Debug, Clone, VisitorMut)]
#[visitor(Expr(exit), SelectStmt(enter))]
pub struct AggregatingIndexRewriter {
    pub sql_dialect: Dialect,
    extracted_aggs: HashSet<String>,
    current_position: Option<usize>,
    agg_func_positions: HashSet<usize>,
}

impl AggregatingIndexRewriter {
    fn exit_expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::FunctionCall {
                func:
                    FunctionCall {
                        distinct,
                        name,
                        args,
                        window,
                        lambda,
                        ..
                    },
                ..
            } if !*distinct
                && SUPPORTED_AGGREGATING_INDEX_FUNCTIONS
                    .contains(&&*name.name.to_ascii_lowercase().to_lowercase())
                && window.is_none()
                && lambda.is_none() =>
            {
                self.agg_func_positions
                    .insert(self.current_position.unwrap());
                if name.name.eq_ignore_ascii_case("avg") {
                    self.extract_avg(args);
                } else if name.name.eq_ignore_ascii_case("count") {
                    self.extract_count(args);
                } else {
                    let agg = format!(
                        "{}({})",
                        name.name.to_ascii_uppercase(),
                        args.iter().map(|arg| arg.to_string()).join(",")
                    );
                    self.extracted_aggs.insert(agg);
                }
            }
            Expr::CountAll { window, .. } if window.is_none() => {
                self.agg_func_positions
                    .insert(self.current_position.unwrap());
                self.extracted_aggs.insert("COUNT()".to_string());
            }
            _ => {}
        };
    }

    fn enter_select_stmt(&mut self, stmt: &mut SelectStmt) {
        let SelectStmt {
            select_list,
            group_by,
            ..
        } = stmt;

        // we save the targets' expr to a hashset, so if the group by
        // items not in targets, we will add to target.
        let mut select_list_exprs: HashSet<String> = HashSet::new();
        select_list.iter().for_each(|target| {
            if let SelectTarget::AliasedExpr { expr, alias } = target {
                select_list_exprs.insert(expr.to_string());
                if let Some(alias) = alias {
                    select_list_exprs.insert(alias.to_string());
                }
            }
        });
        let mut new_select_list: Vec<SelectTarget> = vec![];
        for (position, target) in select_list.iter_mut().enumerate() {
            // if target has agg function, we will extract the func to a hashset
            // see `exit_expr` above for detail.
            // we save the position of target that has agg function here,
            // so that we can skip this target after and replace this skipped
            // target with extracted agg function.
            self.current_position = Some(position);
            target.drive_mut(self);

            // add targets that not have agg function to new select list.
            if !self.agg_func_positions.contains(&position) {
                new_select_list.push(target.clone());
            }
        }

        // add agg functions that extracted from target to new select list.
        // here we sort the `extracted_aggs` for explain test stable.
        self.extracted_aggs.iter().sorted().for_each(|agg| {
            if let Ok(tokens) = tokenize_sql(agg) {
                if let Ok(new_expr) = parse_expr(&tokens, self.sql_dialect) {
                    let target = SelectTarget::AliasedExpr {
                        expr: Box::new(new_expr),
                        alias: None,
                    };
                    new_select_list.push(target);
                }
            }
        });

        match group_by {
            Some(group_by) => match group_by {
                GroupBy::Normal(groups) => {
                    groups.iter().for_each(|expr| {
                        // if group by item not in targets, we will add it in.
                        if !select_list_exprs.contains(&expr.to_string()) {
                            let target = SelectTarget::AliasedExpr {
                                expr: Box::new(expr.clone()),
                                alias: None,
                            };
                            new_select_list.push(target);
                        }
                    });
                }
                _ => unreachable!(),
            },
            None => {}
        }

        // replace the select list with our rewritten new select list.
        *select_list = new_select_list;
    }
}

impl AggregatingIndexRewriter {
    pub fn new(sql_dialect: Dialect) -> Self {
        Self {
            sql_dialect,
            extracted_aggs: Default::default(),
            current_position: None,
            agg_func_positions: Default::default(),
        }
    }

    // If `arg[0]` data type is *NOT* nullable, the optimizer will rewrite `count(arg)` to `count()`,
    // this happened in `NormalizeAggregateOptimizer`.
    // But here we cannot get the arg[0] type, so we provide both `count(arg)` and `count()`.
    pub fn extract_avg(&mut self, args: &[Expr]) {
        let sum = format!("SUM({})", args[0]);
        let count = format!("COUNT({})", args[0]);
        let count_without_column = "COUNT()".to_string();
        self.extracted_aggs.insert(sum);
        self.extracted_aggs.insert(count);
        self.extracted_aggs.insert(count_without_column);
    }

    pub fn extract_count(&mut self, args: &[Expr]) {
        let count_without_column = "COUNT()".to_string();
        self.extracted_aggs.insert(count_without_column);
        if !args.is_empty() {
            let count = format!("COUNT({})", args[0]);
            self.extracted_aggs.insert(count);
        }
    }
}

#[derive(Debug, Clone, Default, Visitor)]
#[visitor(FunctionCall(enter), SelectStmt(enter), Query(enter))]
pub struct AggregatingIndexChecker {
    not_support: bool,
}

impl AggregatingIndexChecker {
    pub fn is_supported(&self) -> bool {
        !self.not_support
    }

    fn enter_function_call(&mut self, func: &FunctionCall) {
        let FunctionCall {
            distinct: _,
            name,
            params: _,
            args: _,
            window: _,
            lambda: _,
        } = func;

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
    }

    fn enter_select_stmt(&mut self, stmt: &SelectStmt) {
        if self.not_support {
            return;
        }
        if stmt.having.is_some() || stmt.window_list.is_some() || stmt.qualify.is_some() {
            self.not_support = true;
            return;
        }
        match &stmt.group_by {
            None => {}
            Some(GroupBy::Normal(_)) => {}
            _ => {
                self.not_support = true;
                return;
            }
        }
        for target in &stmt.select_list {
            if target.has_window() {
                self.not_support = true;
                return;
            }
        }
    }

    fn enter_query(&mut self, query: &Query) {
        if query.with.is_some()
            || !query.order_by.is_empty()
            || !query.limit.is_empty()
            || !matches!(&query.body, SetExpr::Select(_))
        {
            self.not_support = true;
        }
    }
}
#[derive(Debug, Clone, Default, VisitorMut)]
#[visitor(Expr(exit), SelectStmt(exit))]
pub struct RefreshAggregatingIndexRewriter {
    pub user_defined_block_name: bool,
    has_agg_function: bool,
}

impl RefreshAggregatingIndexRewriter {
    fn exit_expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::FunctionCall {
                func:
                    FunctionCall {
                        distinct,
                        name,
                        window,
                        ..
                    },
                ..
            } if !*distinct
                && SUPPORTED_AGGREGATING_INDEX_FUNCTIONS
                    .contains(&&*name.name.to_ascii_lowercase().to_lowercase())
                && window.is_none() =>
            {
                self.has_agg_function = true;
                name.name = format!("{}_STATE", name.name);
            }
            Expr::CountAll { span, window } if window.is_none() => {
                self.has_agg_function = true;
                *expr = Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(*span, "COUNT_STATE"),
                        args: vec![],
                        params: vec![],
                        window: None,
                        lambda: None,
                    },
                };
            }
            _ => {}
        }
    }

    fn exit_select_stmt(&mut self, stmt: &mut SelectStmt) {
        let SelectStmt {
            select_list,
            from,
            group_by,
            ..
        } = stmt;

        let table = {
            let table_ref = from.first().unwrap();
            match table_ref {
                TableReference::Table { table, .. } => table.clone(),
                _ => unreachable!(),
            }
        };

        let block_name_expr = Expr::ColumnRef {
            span: None,
            column: ColumnRef {
                database: None,
                table: Some(table),
                column: ColumnID::Name(Identifier::from_name(stmt.span, BLOCK_NAME_COL_NAME)),
            },
        };

        // if select list already contains `BLOCK_NAME_COL_NAME`
        if select_list.iter().any(|target| match target {
            SelectTarget::AliasedExpr { expr, .. } => match (*expr).clone().as_ref() {
                Expr::ColumnRef { column, .. } => column
                    .column
                    .name()
                    .eq_ignore_ascii_case(BLOCK_NAME_COL_NAME),

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
                        Expr::ColumnRef { column, .. } => column
                            .column
                            .name()
                            .eq_ignore_ascii_case(BLOCK_NAME_COL_NAME),
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
