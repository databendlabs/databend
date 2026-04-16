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

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::VisitorMut;

#[derive(Debug, Clone)]
pub struct AggregateRewriter {
    // aggr_expr_ptr is used for skipping the rewrite of Pivot.aggregate.
    // It only skips the aggregate itself, while the arguments of the aggregate will still be rewritten.
    // Here, it is assumed that the arguments of the aggregate do not contain nested Pivot.
    aggr_expr_ptr: *const Expr,
}

impl Default for AggregateRewriter {
    fn default() -> Self {
        Self {
            aggr_expr_ptr: std::ptr::null(),
        }
    }
}

impl VisitorMut for AggregateRewriter {
    fn visit_table_reference(
        &mut self,
        table_ref: &mut databend_common_ast::ast::TableReference,
    ) -> Result<VisitControl, !> {
        if let databend_common_ast::ast::TableReference::Table {
            pivot: Some(pivot), ..
        }
        | databend_common_ast::ast::TableReference::Subquery {
            pivot: Some(pivot), ..
        } = table_ref
        {
            self.aggr_expr_ptr = &pivot.aggregate;
        }

        Ok(VisitControl::Continue)
    }

    fn visit_expr(&mut self, expr: &mut Expr) -> Result<VisitControl, !> {
        if self.aggr_expr_ptr == expr {
            return Ok(VisitControl::SkipChildren);
        }

        self.rewrite_expr(expr);
        Ok(VisitControl::Continue)
    }
}

impl AggregateRewriter {
    fn rewrite_expr(&mut self, expr: &mut Expr) {
        if self.aggr_expr_ptr == expr {
            return;
        }

        let new_expr = match expr {
            Expr::FunctionCall {
                func:
                    FunctionCall {
                        distinct,
                        name,
                        args,
                        window,
                        ..
                    },
                ..
            } if !*distinct && args.len() == 1 && window.is_none() => {
                match name.name.to_ascii_lowercase().to_lowercase().as_str() {
                    "sum" => self.rewrite_sum(args),
                    "avg" => Some(self.rewrite_avg(args)),
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
    // sum(c + expr) --> c * count(expr) + sum(expr)
    fn rewrite_sum(&self, args: &[Expr]) -> Option<Expr> {
        match &args[0] {
            Expr::BinaryOp {
                span,
                op,
                left,
                right,
            } if matches!(op, BinaryOperator::Minus | BinaryOperator::Plus) => {
                match (left.as_ref(), right.as_ref()) {
                    (l @ Expr::Literal { .. }, other) => {
                        // "{l} * count() {op} sum({other})"
                        let expr = Expr::BinaryOp {
                            span: *span,
                            op: op.clone(),
                            left: Box::new(Expr::BinaryOp {
                                span: l.span(),
                                op: BinaryOperator::Multiply,
                                left: Box::new(l.clone()),
                                right: Box::new(Expr::FunctionCall {
                                    span: l.span(),
                                    func: FunctionCall {
                                        distinct: false,
                                        name: Identifier::from_name(l.span(), "count"),
                                        args: vec![other.clone()],
                                        params: vec![],
                                        order_by: vec![],
                                        window: None,
                                        lambda: None,
                                    },
                                }),
                            }),
                            right: Box::new(Expr::FunctionCall {
                                span: other.span(),
                                func: FunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(other.span(), "sum"),
                                    args: vec![other.clone()],
                                    params: vec![],
                                    order_by: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            }),
                        };
                        return Some(expr);
                    }
                    (other, l @ Expr::Literal { .. }) => {
                        // "sum({other}) {op} {l} * count()"
                        let expr = Expr::BinaryOp {
                            span: *span,
                            op: op.clone(),
                            left: Box::new(Expr::FunctionCall {
                                span: other.span(),
                                func: FunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(other.span(), "sum"),
                                    args: vec![other.clone()],
                                    params: vec![],
                                    order_by: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            }),
                            right: Box::new(Expr::BinaryOp {
                                span: l.span(),
                                op: BinaryOperator::Multiply,
                                left: Box::new(l.clone()),
                                right: Box::new(Expr::FunctionCall {
                                    span: l.span(),
                                    func: FunctionCall {
                                        distinct: false,
                                        name: Identifier::from_name(l.span(), "count"),
                                        args: vec![other.clone()],
                                        params: vec![],
                                        order_by: vec![],
                                        window: None,
                                        lambda: None,
                                    },
                                }),
                            }),
                        };
                        return Some(expr);
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        None
    }

    // avg(arg) --> "sum({args[0]}) / if(count({args[0]}) = 0, 1, count({args[0]}))"
    fn rewrite_avg(&self, args: &[Expr]) -> Expr {
        Expr::BinaryOp {
            span: args[0].span(),
            op: BinaryOperator::Divide,
            left: Box::new(Expr::FunctionCall {
                span: args[0].span(),
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(args[0].span(), "sum"),
                    args: vec![args[0].clone()],
                    params: vec![],
                    order_by: vec![],
                    window: None,
                    lambda: None,
                },
            }),
            right: Box::new(Expr::FunctionCall {
                span: args[0].span(),
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(args[0].span(), "if"),
                    args: vec![
                        Expr::BinaryOp {
                            span: args[0].span(),
                            op: BinaryOperator::Eq,
                            left: Box::new(Expr::FunctionCall {
                                span: args[0].span(),
                                func: FunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(args[0].span(), "count"),
                                    args: vec![args[0].clone()],
                                    params: vec![],
                                    order_by: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            }),
                            right: Box::new(Expr::Literal {
                                span: args[0].span(),
                                value: Literal::UInt64(0),
                            }),
                        },
                        Expr::Literal {
                            span: args[0].span(),
                            value: Literal::UInt64(1),
                        },
                        Expr::FunctionCall {
                            span: args[0].span(),
                            func: FunctionCall {
                                distinct: false,
                                name: Identifier::from_name(args[0].span(), "count"),
                                args: vec![args[0].clone()],
                                params: vec![],
                                order_by: vec![],
                                window: None,
                                lambda: None,
                            },
                        },
                    ],
                    params: vec![],
                    order_by: vec![],
                    window: None,
                    lambda: None,
                },
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_ast::ast::Statement;
    use databend_common_ast::parser::Dialect;
    use databend_common_ast::parser::parse_sql;
    use databend_common_ast::parser::tokenize_sql;
    use databend_common_ast::visit::WalkMut;

    use super::AggregateRewriter;

    fn parse_stmt(sql: &str) -> Statement {
        let tokens = tokenize_sql(sql).unwrap();
        let (stmt, _) = parse_sql(&tokens, Dialect::Experimental).unwrap();
        stmt
    }

    #[test]
    fn test_aggregate_rewriter_skips_table_pivot_aggregate() {
        let mut stmt = parse_stmt("SELECT * FROM t PIVOT(sum(x + 1) FOR y IN (1))");

        let _ = stmt.walk_mut(&mut AggregateRewriter::default());

        assert_eq!(
            stmt.to_string(),
            "SELECT * FROM t PIVOT(sum(x + 1) FOR y IN (1))"
        );
    }

    #[test]
    fn test_aggregate_rewriter_skips_subquery_pivot_aggregate() {
        let mut stmt = parse_stmt("SELECT * FROM (SELECT * FROM t) PIVOT(sum(x + 1) FOR y IN (1))");

        let _ = stmt.walk_mut(&mut AggregateRewriter::default());

        assert_eq!(
            stmt.to_string(),
            "SELECT * FROM (SELECT * FROM t) PIVOT(sum(x + 1) FOR y IN (1))"
        );
    }
}
