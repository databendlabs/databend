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
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableReference;
use derive_visitor::VisitorMut;

#[derive(Debug, Clone, Default, VisitorMut)]
#[visitor(SelectStmt(enter))]
pub struct DistinctToGroupBy {}

impl DistinctToGroupBy {
    fn enter_select_stmt(&mut self, stmt: &mut SelectStmt) {
        let SelectStmt {
            select_list,
            from,
            selection,
            group_by,
            having,
            window_list,
            qualify,
            ..
        } = stmt;

        if group_by.is_none() && select_list.len() == 1 && from.len() == 1 {
            if let databend_common_ast::ast::SelectTarget::AliasedExpr {
                expr:
                    box Expr::FunctionCall {
                        span,
                        func:
                            FunctionCall {
                                distinct,
                                name,
                                args,
                                ..
                            },
                    },
                alias,
            } = &select_list[0]
            {
                let sub_query_name = "_distinct_group_by_subquery";
                if ((name.name.to_ascii_lowercase() == "count" && *distinct)
                    || name.name.to_ascii_lowercase() == "count_distinct")
                    && args.iter().all(|arg| !matches!(arg, Expr::Literal { .. }))
                {
                    let subquery = Query {
                        span: None,
                        with: None,
                        body: SetExpr::Select(Box::new(SelectStmt {
                            span: None,
                            hints: None,
                            distinct: false,
                            top_n: None,
                            select_list: args
                                .iter()
                                .map(|arg| SelectTarget::AliasedExpr {
                                    expr: Box::new(arg.clone()),
                                    alias: None,
                                })
                                .collect(),
                            from: from.clone(),
                            selection: selection.clone(),
                            group_by: Some(GroupBy::Normal(args.clone())),
                            having: None,
                            window_list: None,
                            qualify: None,
                        })),
                        order_by: vec![],
                        limit: vec![],
                        offset: None,
                        ignore_result: false,
                    };

                    let new_stmt = SelectStmt {
                        span: None,
                        hints: None,
                        top_n: None,
                        distinct: false,
                        select_list: vec![databend_common_ast::ast::SelectTarget::AliasedExpr {
                            expr: Box::new(Expr::FunctionCall {
                                span: None,
                                func: FunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(*span, "count"),
                                    args: vec![Expr::ColumnRef {
                                        span: None,
                                        column: ColumnRef {
                                            database: None,
                                            table: None,
                                            column: ColumnID::Name(Identifier::from_name(
                                                None, "_1",
                                            )),
                                        },
                                    }],
                                    params: vec![],
                                    window_ignore_null: None,
                                    window: None,
                                    lambda: None,
                                },
                            }),
                            alias: alias.clone(),
                        }],
                        from: vec![TableReference::Subquery {
                            span: None,
                            lateral: false,
                            subquery: Box::new(subquery),
                            alias: Some(TableAlias {
                                name: Identifier::from_name(None, sub_query_name),
                                columns: vec![Identifier::from_name(None, "_1")],
                            }),
                        }],
                        selection: None,
                        group_by: None,
                        having: having.clone(),
                        window_list: window_list.clone(),
                        qualify: qualify.clone(),
                    };

                    *stmt = new_stmt;
                }
            }
        }
    }
}
