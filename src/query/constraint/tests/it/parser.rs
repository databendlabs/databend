// Copyright 2023 Datafuse Labs.
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

use std::collections::HashMap;

use databend_common_ast::ast::BinaryOperator as ASTBinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr as ASTExpr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::UnaryOperator as ASTUnaryOperator;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_constraint::mir::MirBinaryOperator;
use databend_common_constraint::mir::MirConstant;
use databend_common_constraint::mir::MirDataType;
use databend_common_constraint::mir::MirExpr;
use databend_common_constraint::mir::MirUnaryOperator;

pub fn parse_mir_expr(text: &str, variables: &HashMap<String, MirDataType>) -> MirExpr {
    let tokens = tokenize_sql(text).unwrap();
    let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
    sql_ast_to_mir(expr, variables).unwrap()
}

pub fn pretty_mir_expr(expr: &MirExpr) -> String {
    let ast = mir_to_sql_ast(expr);
    ast.to_string()
}

fn sql_ast_to_mir(ast: ASTExpr, variables: &HashMap<String, MirDataType>) -> Option<MirExpr> {
    match ast {
        ASTExpr::Literal { value, .. } => {
            let constant = match value {
                Literal::Boolean(x) => MirConstant::Bool(x),
                Literal::UInt64(x) => MirConstant::Int(x as i64),
                Literal::Null => MirConstant::Null,
                _ => return None,
            };
            Some(MirExpr::Constant(constant))
        }
        ASTExpr::ColumnRef {
            column:
                ColumnRef {
                    database: None,
                    table: None,
                    column:
                        ColumnID::Name(Identifier {
                            name, quote: None, ..
                        }),
                },
            ..
        } => Some(MirExpr::Variable {
            data_type: variables[&name],
            name,
        }),
        ASTExpr::IsNull {
            expr, not: false, ..
        } => Some(MirExpr::UnaryOperator {
            op: MirUnaryOperator::IsNull,
            arg: Box::new(sql_ast_to_mir(*expr, variables)?),
        }),
        ASTExpr::IsNull {
            expr, not: true, ..
        } => Some(MirExpr::UnaryOperator {
            op: MirUnaryOperator::Not,
            arg: Box::new(MirExpr::UnaryOperator {
                op: MirUnaryOperator::IsNull,
                arg: Box::new(sql_ast_to_mir(*expr, variables)?),
            }),
        }),
        ASTExpr::BinaryOp {
            op, left, right, ..
        } => {
            let op = match op {
                ASTBinaryOperator::Plus => MirBinaryOperator::Plus,
                ASTBinaryOperator::Minus => MirBinaryOperator::Minus,
                ASTBinaryOperator::Multiply => MirBinaryOperator::Multiply,
                ASTBinaryOperator::And => MirBinaryOperator::And,
                ASTBinaryOperator::Or => MirBinaryOperator::Or,
                ASTBinaryOperator::Eq => MirBinaryOperator::Eq,
                ASTBinaryOperator::Lt => MirBinaryOperator::Lt,
                ASTBinaryOperator::Lte => MirBinaryOperator::Lte,
                ASTBinaryOperator::Gt => MirBinaryOperator::Gt,
                ASTBinaryOperator::Gte => MirBinaryOperator::Gte,
                ASTBinaryOperator::NotEq => {
                    return sql_ast_to_mir(
                        ASTExpr::UnaryOp {
                            span: None,
                            op: ASTUnaryOperator::Not,
                            expr: Box::new(ASTExpr::BinaryOp {
                                span: None,
                                op: ASTBinaryOperator::Eq,
                                left,
                                right,
                            }),
                        },
                        variables,
                    );
                }
                _ => return None,
            };
            let left = sql_ast_to_mir(*left, variables)?;
            let right = sql_ast_to_mir(*right, variables)?;
            Some(MirExpr::BinaryOperator {
                op,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        ASTExpr::UnaryOp { op, expr, .. } => {
            let op = match op {
                ASTUnaryOperator::Minus => MirUnaryOperator::Minus,
                ASTUnaryOperator::Not => MirUnaryOperator::Not,
                _ => return None,
            };
            let expr = sql_ast_to_mir(*expr, variables)?;
            Some(MirExpr::UnaryOperator {
                op,
                arg: Box::new(expr),
            })
        }
        _ => None,
    }
}

fn mir_to_sql_ast(expr: &MirExpr) -> ASTExpr {
    match expr {
        MirExpr::Constant(constant) => {
            let value = match constant {
                MirConstant::Bool(x) => Literal::Boolean(*x),
                MirConstant::Int(x) if *x >= 0 => Literal::UInt64(*x as u64),
                MirConstant::Int(x) => {
                    return ASTExpr::UnaryOp {
                        span: None,
                        op: ASTUnaryOperator::Minus,
                        expr: Box::new(ASTExpr::Literal {
                            span: None,
                            value: Literal::UInt64(x.wrapping_neg() as u64),
                        }),
                    };
                }
                MirConstant::Null => Literal::Null,
            };
            ASTExpr::Literal { span: None, value }
        }
        MirExpr::Variable { name, .. } => ASTExpr::ColumnRef {
            span: None,
            column: ColumnRef {
                database: None,
                table: None,
                column: ColumnID::Name(Identifier::from_name(None, name.clone())),
            },
        },
        MirExpr::UnaryOperator { op, arg } => {
            let op = match op {
                MirUnaryOperator::Minus => ASTUnaryOperator::Minus,
                MirUnaryOperator::Not => {
                    if let MirExpr::UnaryOperator {
                        op: MirUnaryOperator::IsNull,
                        arg,
                    } = &**arg
                    {
                        return ASTExpr::IsNull {
                            span: None,
                            expr: Box::new(mir_to_sql_ast(arg)),
                            not: true,
                        };
                    };
                    ASTUnaryOperator::Not
                }
                MirUnaryOperator::IsNull => {
                    return ASTExpr::IsNull {
                        span: None,
                        expr: Box::new(mir_to_sql_ast(arg)),
                        not: false,
                    };
                }
                MirUnaryOperator::RemoveNullable => {
                    return ASTExpr::FunctionCall {
                        span: None,
                        func: FunctionCall {
                            distinct: false,
                            name: Identifier::from_name(None, "remove_nullable"),
                            args: vec![mir_to_sql_ast(arg)],
                            params: vec![],
                            window: None,
                            lambda: None,
                        },
                    };
                }
            };
            ASTExpr::UnaryOp {
                span: None,
                op,
                expr: Box::new(mir_to_sql_ast(arg)),
            }
        }
        MirExpr::BinaryOperator { op, left, right } => {
            let op = match op {
                MirBinaryOperator::Plus => ASTBinaryOperator::Plus,
                MirBinaryOperator::Minus => ASTBinaryOperator::Minus,
                MirBinaryOperator::Multiply => ASTBinaryOperator::Multiply,
                MirBinaryOperator::And => ASTBinaryOperator::And,
                MirBinaryOperator::Or => ASTBinaryOperator::Or,
                MirBinaryOperator::Eq => ASTBinaryOperator::Eq,
                MirBinaryOperator::Lt => ASTBinaryOperator::Lt,
                MirBinaryOperator::Lte => ASTBinaryOperator::Lte,
                MirBinaryOperator::Gt => ASTBinaryOperator::Gt,
                MirBinaryOperator::Gte => ASTBinaryOperator::Gte,
            };
            ASTExpr::BinaryOp {
                span: None,
                op,
                left: Box::new(mir_to_sql_ast(left)),
                right: Box::new(mir_to_sql_ast(right)),
            }
        }
    }
}
