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

use common_ast::ast::Expr;
use common_ast::ast::GroupBy;
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_ast::ast::Query;
use common_ast::ast::SelectStmt;
use common_ast::ast::SelectTarget;
use common_ast::ast::SetExpr;
use common_ast::ast::TableReference;
use common_ast::ast::TypeName;
use common_expression::types::DataType;
use rand::Rng;

use crate::sql_gen::Column;
use crate::sql_gen::SqlGenerator;
use crate::sql_gen::Table;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_query(&mut self) -> Query {
        let body = self.gen_set_expr();
        let limit = self.gen_limit();
        let offset = self.gen_offset(limit.len());
        Query {
            span: None,
            // TODO
            with: None,
            body,
            // TODO
            order_by: vec![],
            limit,
            offset,
            ignore_result: false,
        }
    }

    fn gen_set_expr(&mut self) -> SetExpr {
        match self.rng.gen_range(0..=9) {
            0..=9 => {
                let select = self.gen_select();
                SetExpr::Select(Box::new(select))
            }
            // TODO
            _ => unreachable!(),
        }
    }

    fn flip_coin(&mut self) -> bool {
        self.rng.gen_bool(0.5)
    }

    fn gen_limit(&mut self) -> Vec<Expr> {
        let mut res = Vec::new();
        if self.flip_coin() {
            let limit = Expr::Literal {
                span: None,
                lit: Literal::UInt64(self.rng.gen_range(0..=100)),
            };
            res.push(limit);
            if self.flip_coin() {
                let offset = Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(5..=10)),
                };
                res.push(offset);
            }
        }
        res
    }

    fn gen_offset(&mut self, limit_len: usize) -> Option<Expr> {
        if self.flip_coin() && limit_len != 2 {
            return Some(Expr::Literal {
                span: None,
                lit: Literal::UInt64(self.rng.gen_range(0..=10)),
            });
        }
        None
    }

    fn gen_select(&mut self) -> SelectStmt {
        let from = self.gen_from();
        let group_by = self.gen_group_by();
        let select_list = self.gen_select_list(&group_by);
        let selection = self.gen_selection();
        SelectStmt {
            span: None,
            // TODO
            hints: None,
            // TODO
            distinct: false,
            select_list,
            from,
            selection,
            group_by,
            // TODO
            having: None,
            // TODO
            window_list: None,
        }
    }

    fn gen_group_by(&mut self) -> Option<GroupBy> {
        let mode = self.rng.gen_range(0..=5);
        let group_cap = self.rng.gen_range(1..=5);
        let mut groupby_items = Vec::with_capacity(group_cap);

        for _ in 0..group_cap {
            let ty = self.gen_data_type();
            let expr = self.gen_expr(&ty);
            let groupby_item = match expr {
                Expr::Literal {
                    lit: Literal::UInt64(_),
                    ..
                } => Expr::Cast {
                    span: None,
                    expr: Box::new(expr),
                    target_type: TypeName::UInt64,
                    pg_style: false,
                },
                Expr::Literal {
                    lit:
                        Literal::Decimal256 {
                            scale, precision, ..
                        },
                    ..
                } => {
                    if scale == 0 {
                        Expr::Cast {
                            span: None,
                            expr: Box::new(expr),
                            target_type: TypeName::Decimal { precision, scale },
                            pg_style: false,
                        }
                    } else {
                        expr
                    }
                }
                _ => expr,
            };
            groupby_items.push(groupby_item);
        }

        match mode {
            0 => None,
            1 => Some(GroupBy::Normal(groupby_items)),
            2 => Some(GroupBy::All),
            3 => Some(GroupBy::GroupingSets(vec![groupby_items])),
            4 => Some(GroupBy::Cube(groupby_items)),
            5 => Some(GroupBy::Rollup(groupby_items)),
            _ => unreachable!(),
        }
    }

    fn gen_select_list(&mut self, group_by: &Option<GroupBy>) -> Vec<SelectTarget> {
        let mut targets = vec![];
        if let Some(group_by) = group_by {
            match group_by {
                GroupBy::Normal(group_by) => {
                    targets.extend(group_by.iter().map(|expr| SelectTarget::AliasedExpr {
                        expr: Box::new(expr.clone()),
                        alias: None,
                    }));
                }
                GroupBy::All => {
                    let select_num = self.rng.gen_range(1..=5);
                    for _ in 0..select_num {
                        let target = match self.rng.gen_range(0..=9) {
                            0..=9 => {
                                let ty = self.gen_data_type();
                                let expr = self.gen_expr(&ty);
                                SelectTarget::AliasedExpr {
                                    expr: Box::new(expr),
                                    // TODO
                                    alias: None,
                                }
                            }
                            // TODO
                            _ => unreachable!(),
                        };
                        targets.push(target)
                    }
                }
                GroupBy::GroupingSets(group_by) => {
                    targets.extend(group_by[0].iter().map(|expr| SelectTarget::AliasedExpr {
                        expr: Box::new(expr.clone()),
                        alias: None,
                    }));
                }
                GroupBy::Cube(group_by) => {
                    targets.extend(group_by.iter().map(|expr| SelectTarget::AliasedExpr {
                        expr: Box::new(expr.clone()),
                        alias: None,
                    }));
                }
                GroupBy::Rollup(group_by) => {
                    targets.extend(group_by.iter().map(|expr| SelectTarget::AliasedExpr {
                        expr: Box::new(expr.clone()),
                        alias: None,
                    }));
                }
            }
        } else {
            let select_num = self.rng.gen_range(1..=5);
            for _ in 0..select_num {
                let target = match self.rng.gen_range(0..=9) {
                    0..=9 => {
                        let ty = self.gen_data_type();
                        let expr = self.gen_expr(&ty);
                        SelectTarget::AliasedExpr {
                            expr: Box::new(expr),
                            // TODO
                            alias: None,
                        }
                    }
                    // TODO
                    _ => unreachable!(),
                };
                targets.push(target)
            }
        }
        targets
    }

    fn gen_from(&mut self) -> Vec<TableReference> {
        match self.rng.gen_range(0..=9) {
            0..=9 => {
                let i = self.rng.gen_range(0..self.tables.len());
                self.bound_table(self.tables[i].clone());

                let table_name = Identifier::from_name(self.tables[i].name.clone());

                let table_ref = TableReference::Table {
                    span: None,
                    // TODO
                    catalog: None,
                    // TODO
                    database: None,
                    table: table_name,
                    // TODO
                    alias: None,
                    // TODO
                    travel_point: None,
                    // TODO
                    pivot: None,
                    // TODO
                    unpivot: None,
                };
                vec![table_ref]
            }
            // TODO
            _ => unreachable!(),
        }
    }

    fn gen_selection(&mut self) -> Option<Expr> {
        match self.rng.gen_range(0..=9) {
            0..=5 => Some(self.gen_expr(&DataType::Boolean)),
            6 => {
                let ty = self.gen_simple_data_type();
                Some(self.gen_expr(&ty))
            }
            _ => None,
        }
    }

    fn bound_table(&mut self, table: Table) {
        for (i, field) in table.schema.fields().iter().enumerate() {
            let column = Column {
                table_name: table.name.clone(),
                name: field.name.clone(),
                index: i + 1,
                data_type: DataType::from(&field.data_type),
            };
            self.bound_columns.push(column);
        }
        self.bound_tables.push(table);
    }
}
