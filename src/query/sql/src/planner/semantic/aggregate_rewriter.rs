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

use common_ast::ast::BinaryOperator;
use common_ast::ast::Expr;
use common_ast::parser::parse_expr;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_ast::VisitorMut;

#[derive(Debug, Clone, Default)]
pub struct AggregateRewriter {
    pub sql_dialect: Dialect,
}

impl VisitorMut for AggregateRewriter {
    fn visit_expr(&mut self, expr: &mut Expr) {
        match expr {
            Expr::FunctionCall {
                distinct,
                name,
                args,
                ..
            } if !*distinct
                && args.len() == 1
                && name.name.to_ascii_lowercase().to_lowercase() == "sum" =>
            {
                match &args[0] {
                    Expr::BinaryOp {
                        op, left, right, ..
                    } if matches!(op, BinaryOperator::Minus | BinaryOperator::Plus) => {
                        match (left.as_ref(), right.as_ref()) {
                            (l @ Expr::Literal { .. }, other) => {
                                let text = format!("{} * count() {op} sum({})", l, other);

                                if let Ok(new_expr) = tokenize_sql(&text)
                                    .and_then(|tokens| parse_expr(&tokens, self.sql_dialect))
                                {
                                    *expr = new_expr;
                                }
                            }
                            (other, l @ Expr::Literal { .. }) => {
                                let text = format!("sum({}) {op}{} * count() ", other, l);
                                if let Ok(new_expr) = tokenize_sql(&text)
                                    .and_then(|tokens| parse_expr(&tokens, self.sql_dialect))
                                {
                                    *expr = new_expr;
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
}
