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
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_ast::ast::Query;
use common_ast::ast::SelectStmt;
use common_ast::ast::SelectTarget;
use common_ast::ast::SetExpr;
use common_ast::ast::TableReference;
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
        let select_list = self.gen_select_list();
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
            // TODO
            group_by: None,
            // TODO
            having: None,
            // TODO
            window_list: None,
        }
    }

    fn gen_select_list(&mut self) -> Vec<SelectTarget> {
        let select_num = self.rng.gen_range(1..=5);
        let mut targets = Vec::with_capacity(select_num);
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
