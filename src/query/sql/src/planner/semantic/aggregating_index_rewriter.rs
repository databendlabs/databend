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
use common_ast::ast::SelectStmt;
use common_ast::ast::SelectTarget;
use common_ast::ast::TableReference;
use common_ast::walk_select_target_mut;
use common_ast::Dialect;
use common_ast::VisitorMut;
use common_expression::BLOCK_NAME_COL_NAME;

use crate::planner::SUPPORTED_AGGREGATING_INDEX_FUNCTIONS;

#[derive(Debug, Clone, Default)]
pub struct AggregatingIndexRewriter {
    pub sql_dialect: Dialect,
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
                name.name = format!("{}_STATE", name.name);
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
            let table_ref = from.get(0).unwrap();
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
                    if column.name().eq_ignore_ascii_case(BLOCK_NAME_COL_NAME) {
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            },
            SelectTarget::QualifiedName { .. } => false,
        }) {
            // check group by also has `BLOCK_NAME_COL_NAME`
            if let Some(group_by) = group_by {
                match group_by {
                    GroupBy::Normal(groups) => {
                        if !groups.iter().any(|expr| match (*expr).clone() {
                            Expr::ColumnRef { column, .. } => {
                                if column.name().eq_ignore_ascii_case(BLOCK_NAME_COL_NAME) {
                                    true
                                } else {
                                    false
                                }
                            }
                            _ => false,
                        }) {
                            groups.extend_one(block_name_expr)
                        }
                    }
                    _ => unreachable!(),
                }
            }
        } else {
            select_list.extend_one(SelectTarget::AliasedExpr {
                expr: Box::new(block_name_expr.clone()),
                alias: None,
            });

            if let Some(group_by) = group_by {
                match group_by {
                    GroupBy::Normal(groups) => groups.extend_one(block_name_expr),
                    _ => unreachable!(),
                }
            }
        }
    }
}
