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

use databend_common_ast::ast::walk_expr_mut;
use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::VisitorMut;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;

#[derive(Debug, Clone, Default)]
pub struct AggregateRewriter {
    pub sql_dialect: Dialect,
}

impl VisitorMut for AggregateRewriter {
    fn visit_expr(&mut self, expr: &mut Expr) {
        // rewrite children
        walk_expr_mut(self, expr);

        let new_expr = match expr {
            Expr::FunctionCall {
                distinct,
                name,
                args,
                window,
                ..
            } if !*distinct && args.len() == 1 && window.is_none() => {
                match name.name.to_ascii_lowercase().to_lowercase().as_str() {
                    "sum" => self.rewrite_sum(args),
                    "avg" => self.rewrite_avg(args),
                    _ => None,
                }
            }
            _ => None,
        };

        if let Some(new_expr) = new_expr {
            *expr = new_expr;
        }
    }
}

impl AggregateRewriter {
    fn rewrite_sum(&self, args: &[Expr]) -> Option<Expr> {
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
                            return Some(new_expr);
                        }
                    }
                    (other, l @ Expr::Literal { .. }) => {
                        let text = format!("sum({}) {op}{} * count() ", other, l);
                        if let Ok(new_expr) = tokenize_sql(&text)
                            .and_then(|tokens| parse_expr(&tokens, self.sql_dialect))
                        {
                            return Some(new_expr);
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        None
    }

    fn rewrite_avg(&self, args: &[Expr]) -> Option<Expr> {
        let text = format!(
            "sum({}) / if(count({}) = 0, 1, count({}))",
            args[0], args[0], args[0]
        );

        if let Ok(tokens) = tokenize_sql(&text) {
            if let Ok(new_expr) = parse_expr(&tokens, self.sql_dialect) {
                return Some(new_expr);
            }
        }

        None
    }
}
