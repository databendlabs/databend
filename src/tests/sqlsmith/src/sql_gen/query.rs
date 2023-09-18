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
use common_ast::ast::Join;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator;
use common_ast::ast::Literal;
use common_ast::ast::OrderByExpr;
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
        self.bound_columns.clear();
        self.bound_tables.clear();
        self.is_join = false;

        let body = self.gen_set_expr();
        let limit = self.gen_limit();
        let offset = self.gen_offset(limit.len());

        let order_by = self.gen_order_by(self.group_by.clone());
        Query {
            span: None,
            // TODO
            with: None,
            body,
            order_by,
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

    fn gen_order_by(&mut self, group_by: Option<GroupBy>) -> Vec<OrderByExpr> {
        let order_nums = self.rng.gen_range(1..5);
        let mut orders = Vec::with_capacity(order_nums);
        if self.flip_coin() {
            if let Some(group_by) = group_by {
                match group_by {
                    GroupBy::GroupingSets(group_by) => {
                        orders.extend(group_by[0].iter().map(|expr| OrderByExpr {
                            expr: expr.clone(),
                            asc: Some(self.flip_coin()),
                            nulls_first: Some(self.flip_coin()),
                        }))
                    }
                    GroupBy::Rollup(group_by)
                    | GroupBy::Cube(group_by)
                    | GroupBy::Normal(group_by) => {
                        orders.extend(group_by.iter().map(|expr| OrderByExpr {
                            expr: expr.clone(),
                            asc: Some(self.flip_coin()),
                            nulls_first: Some(self.flip_coin()),
                        }))
                    }
                    GroupBy::All => {
                        for _ in 0..order_nums {
                            let ty = self.gen_data_type();
                            let expr = self.gen_expr(&ty);
                            let order_by_expr = if self.rng.gen_bool(0.2) {
                                OrderByExpr {
                                    expr,
                                    asc: None,
                                    nulls_first: None,
                                }
                            } else {
                                OrderByExpr {
                                    expr,
                                    asc: Some(self.flip_coin()),
                                    nulls_first: Some(self.flip_coin()),
                                }
                            };
                            orders.push(order_by_expr);
                        }
                    }
                }
            } else {
                for _ in 0..order_nums {
                    let ty = self.gen_data_type();
                    let expr = self.gen_expr(&ty);
                    let order_by_expr = if self.rng.gen_bool(0.2) {
                        OrderByExpr {
                            expr,
                            asc: None,
                            nulls_first: None,
                        }
                    } else {
                        OrderByExpr {
                            expr,
                            asc: Some(self.flip_coin()),
                            nulls_first: Some(self.flip_coin()),
                        }
                    };
                    orders.push(order_by_expr);
                }
            }
        }
        orders
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
        self.group_by = group_by.clone();
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
        if self.rng.gen_bool(0.8) {
            return None;
        }
        let group_cap = self.rng.gen_range(1..=5);
        let mut groupby_items = Vec::with_capacity(group_cap);

        for _ in 0..group_cap {
            let ty = self.gen_data_type();
            let groupby_item = self.gen_expr(&ty);
            let groupby_item = self.rewrite_position_expr(groupby_item);
            groupby_items.push(groupby_item);
        }

        match self.rng.gen_range(0..=4) {
            0 => Some(GroupBy::Normal(groupby_items)),
            1 => Some(GroupBy::All),
            2 => Some(GroupBy::GroupingSets(vec![groupby_items])),
            3 => Some(GroupBy::Cube(groupby_items)),
            4 => Some(GroupBy::Rollup(groupby_items)),
            _ => unreachable!(),
        }
    }

    fn gen_select_list(&mut self, group_by: &Option<GroupBy>) -> Vec<SelectTarget> {
        let mut targets = Vec::with_capacity(5);

        let generate_target = |expr: Expr| SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias: None,
        };

        match group_by {
            Some(GroupBy::Normal(group_by))
            | Some(GroupBy::Cube(group_by))
            | Some(GroupBy::Rollup(group_by)) => {
                let ty = self.gen_data_type();
                let agg_expr = self.gen_agg_func(&ty);
                targets.push(SelectTarget::AliasedExpr {
                    expr: Box::new(agg_expr),
                    alias: None,
                });
                targets.extend(group_by.iter().map(|expr| SelectTarget::AliasedExpr {
                    expr: Box::new(expr.clone()),
                    alias: None,
                }));
            }
            Some(GroupBy::All) => {
                let select_num = self.rng.gen_range(1..=5);
                for _ in 0..select_num {
                    let ty = self.gen_data_type();
                    let expr = if self.rng.gen_bool(0.8) {
                        self.gen_agg_func(&ty)
                    } else {
                        self.gen_expr(&ty)
                    };
                    let target = generate_target(expr);
                    targets.push(target);
                }
            }
            Some(GroupBy::GroupingSets(group_by)) => {
                let ty = self.gen_data_type();
                let agg_expr = self.gen_agg_func(&ty);
                targets.push(SelectTarget::AliasedExpr {
                    expr: Box::new(agg_expr),
                    alias: None,
                });
                targets.extend(group_by[0].iter().map(|expr| SelectTarget::AliasedExpr {
                    expr: Box::new(expr.clone()),
                    alias: None,
                }));
            }
            None => {
                let select_num = self.rng.gen_range(1..=7);
                for _ in 0..select_num {
                    let ty = self.gen_data_type();
                    let expr = self.gen_expr(&ty);
                    let target = generate_target(expr);
                    targets.push(target);
                }
            }
        }

        targets
    }

    fn gen_from(&mut self) -> Vec<TableReference> {
        match self.rng.gen_range(0..=9) {
            0..=7 => {
                let i = self.rng.gen_range(0..self.tables.len());
                let table_ref = self.gen_table_ref(self.tables[i].clone());
                vec![table_ref]
            }
            // join
            8..=9 => {
                self.is_join = true;
                let join = self.gen_join_table_ref();
                vec![join]
            }
            // TODO
            _ => unreachable!(),
        }
    }

    fn gen_table_ref(&mut self, table: Table) -> TableReference {
        let table_name = Identifier::from_name(table.name.clone());

        self.bound_table(table);

        TableReference::Table {
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
        }
    }

    fn gen_join_table_ref(&mut self) -> TableReference {
        let i = self.rng.gen_range(0..self.tables.len());
        let j = if i == self.tables.len() - 1 { 0 } else { i + 1 };
        let left_table = self.gen_table_ref(self.tables[i].clone());
        let right_table = self.gen_table_ref(self.tables[j].clone());

        let op = match self.rng.gen_range(0..=8) {
            0 => JoinOperator::Inner,
            1 => JoinOperator::LeftOuter,
            2 => JoinOperator::RightOuter,
            3 => JoinOperator::FullOuter,
            4 => JoinOperator::LeftSemi,
            5 => JoinOperator::LeftAnti,
            6 => JoinOperator::RightSemi,
            7 => JoinOperator::RightAnti,
            8 => JoinOperator::CrossJoin,
            _ => unreachable!(),
        };

        let condition = match self.rng.gen_range(0..=2) {
            0 => {
                let ty = self.gen_data_type();
                let expr = self.gen_expr(&ty);
                JoinCondition::On(Box::new(expr))
            }
            1 => {
                let left_fields = self.tables[i].schema.fields();
                let right_fields = self.tables[j].schema.fields();

                let mut names = Vec::new();
                for left_field in left_fields {
                    for right_field in right_fields {
                        if left_field.name == right_field.name {
                            names.push(left_field.name.clone());
                        }
                    }
                }
                if names.is_empty() {
                    JoinCondition::Natural
                } else {
                    let mut num = self.rng.gen_range(1..=3);
                    if num > names.len() {
                        num = names.len();
                    }
                    let mut idents = Vec::with_capacity(num);
                    for _ in 0..num {
                        let idx = self.rng.gen_range(0..names.len());
                        idents.push(Identifier::from_name(names[idx].clone()));
                    }
                    JoinCondition::Using(idents)
                }
            }
            2 => JoinCondition::Natural,
            _ => unreachable!(),
        };

        let condition = match op {
            // Outer joins can not work with `JoinCondition::None`
            JoinOperator::LeftOuter | JoinOperator::RightOuter | JoinOperator::FullOuter => {
                condition
            }
            // CrossJoin can only work with `JoinCondition::None`
            JoinOperator::CrossJoin => JoinCondition::None,
            _ => {
                if self.rng.gen_bool(0.2) {
                    JoinCondition::None
                } else {
                    condition
                }
            }
        };

        let join = Join {
            op,
            condition,
            left: Box::new(left_table),
            right: Box::new(right_table),
        };
        TableReference::Join { span: None, join }
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

    // rewrite position expr in group by and order by,
    // avoiding `GROUP BY position n is not in select list` errors
    fn rewrite_position_expr(&mut self, expr: Expr) -> Expr {
        if let Expr::Literal {
            lit: Literal::UInt64(n),
            ..
        } = expr
        {
            let pos = n % 3 + 1;
            Expr::Literal {
                span: None,
                lit: Literal::UInt64(pos),
            }
        } else {
            expr
        }
    }
}
