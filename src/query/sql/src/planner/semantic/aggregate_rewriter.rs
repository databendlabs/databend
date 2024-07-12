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
use derive_visitor::VisitorMut;

#[derive(Debug, Clone, Default, VisitorMut)]
#[visitor(Expr(exit))]
pub struct AggregateRewriter;

impl AggregateRewriter {
    fn exit_expr(&mut self, expr: &mut Expr) {
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
                                window: None,
                                lambda: None,
                            },
                        },
                    ],
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            }),
        }
    }
}
