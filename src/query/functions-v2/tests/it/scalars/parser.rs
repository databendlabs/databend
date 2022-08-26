// Copyright 2022 Datafuse Labs.
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
use common_ast::ast::Literal as ASTLiteral;
use common_ast::ast::UnaryOperator;
use common_ast::parser::parse_expr;
use common_ast::parser::token::Token;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_expression::types::DataType;
use common_expression::Literal;
use common_expression::RawExpr;
use common_expression::Span;

pub fn parse_raw_expr(text: &str, columns: &[(&str, DataType)]) -> RawExpr {
    let backtrace = Backtrace::new();
    let tokens = tokenize_sql(text).unwrap();
    let expr = parse_expr(&tokens, Dialect::PostgreSQL, &backtrace).unwrap();
    transform_expr(expr, columns)
}

pub fn transform_expr(ast: common_ast::ast::Expr, columns: &[(&str, DataType)]) -> RawExpr {
    match ast {
        common_ast::ast::Expr::Literal { span, lit } => RawExpr::Literal {
            span: transform_span(span),
            lit: transform_literal(lit),
        },
        common_ast::ast::Expr::ColumnRef {
            span,
            database: None,
            table: None,
            column,
        } => {
            let col_id = columns
                .iter()
                .position(|(col_name, _)| *col_name == column.name)
                .unwrap_or_else(|| panic!("expected column {}", column.name));
            RawExpr::ColumnRef {
                span: transform_span(span),
                id: col_id,
                data_type: columns[col_id].1.clone(),
            }
        }
        common_ast::ast::Expr::Cast {
            span,
            expr,
            target_type,
            ..
        } => RawExpr::Cast {
            span: transform_span(span),
            expr: Box::new(transform_expr(*expr, columns)),
            dest_type: transform_data_type(target_type),
        },
        common_ast::ast::Expr::TryCast {
            span,
            expr,
            target_type,
            ..
        } => RawExpr::TryCast {
            span: transform_span(span),
            expr: Box::new(transform_expr(*expr, columns)),
            dest_type: transform_data_type(target_type),
        },
        common_ast::ast::Expr::FunctionCall {
            span,
            name,
            args,
            params,
            ..
        } => RawExpr::FunctionCall {
            span: transform_span(span),
            name: name.name,
            args: args
                .into_iter()
                .map(|arg| transform_expr(arg, columns))
                .collect(),
            params: params
                .into_iter()
                .map(|param| match param {
                    ASTLiteral::Integer(u) => u as usize,
                    _ => unimplemented!(),
                })
                .collect(),
        },
        common_ast::ast::Expr::UnaryOp { span, op, expr } => RawExpr::FunctionCall {
            span: transform_span(span),
            name: transform_unary_op(op),
            params: vec![],
            args: vec![transform_expr(*expr, columns)],
        },
        common_ast::ast::Expr::BinaryOp {
            span,
            op,
            left,
            right,
        } => RawExpr::FunctionCall {
            span: transform_span(span),
            name: transform_binary_op(op),
            params: vec![],
            args: vec![
                transform_expr(*left, columns),
                transform_expr(*right, columns),
            ],
        },
        common_ast::ast::Expr::Trim {
            span,
            expr,
            trim_where,
        } => {
            if let Some(inner) = trim_where {
                match inner.0 {
                    common_ast::ast::TrimWhere::Both => RawExpr::FunctionCall {
                        span: transform_span(span),
                        name: "trim_both".to_string(),
                        params: vec![],
                        args: vec![
                            transform_expr(*expr, columns),
                            transform_expr(*inner.1, columns),
                        ],
                    },
                    common_ast::ast::TrimWhere::Leading => RawExpr::FunctionCall {
                        span: transform_span(span),
                        name: "trim_leading".to_string(),
                        params: vec![],
                        args: vec![
                            transform_expr(*expr, columns),
                            transform_expr(*inner.1, columns),
                        ],
                    },
                    common_ast::ast::TrimWhere::Trailing => RawExpr::FunctionCall {
                        span: transform_span(span),
                        name: "trim_trailing".to_string(),
                        params: vec![],
                        args: vec![
                            transform_expr(*expr, columns),
                            transform_expr(*inner.1, columns),
                        ],
                    },
                }
            } else {
                RawExpr::FunctionCall {
                    span: transform_span(span),
                    name: "trim".to_string(),
                    params: vec![],
                    args: vec![transform_expr(*expr, columns)],
                }
            }
        }
        _ => unimplemented!(),
    }
}

fn transform_unary_op(op: UnaryOperator) -> String {
    format!("{op:?}").to_lowercase()
}

fn transform_binary_op(op: BinaryOperator) -> String {
    format!("{op:?}").to_lowercase()
}

fn transform_data_type(target_type: common_ast::ast::TypeName) -> DataType {
    match target_type {
        common_ast::ast::TypeName::Boolean => DataType::Boolean,
        common_ast::ast::TypeName::UInt8 => DataType::UInt8,
        common_ast::ast::TypeName::UInt16 => DataType::UInt16,
        common_ast::ast::TypeName::UInt32 => DataType::UInt32,
        common_ast::ast::TypeName::UInt64 => DataType::UInt64,
        common_ast::ast::TypeName::Int8 => DataType::Int8,
        common_ast::ast::TypeName::Int16 => DataType::Int16,
        common_ast::ast::TypeName::Int32 => DataType::Int32,
        common_ast::ast::TypeName::Int64 => DataType::Int64,
        common_ast::ast::TypeName::Float32 => DataType::Float32,
        common_ast::ast::TypeName::Float64 => DataType::Float64,
        common_ast::ast::TypeName::String => DataType::String,
        common_ast::ast::TypeName::Array {
            item_type: Some(item_type),
        } => DataType::Array(Box::new(transform_data_type(*item_type))),
        common_ast::ast::TypeName::Tuple { fields_type, .. } => {
            DataType::Tuple(fields_type.into_iter().map(transform_data_type).collect())
        }
        common_ast::ast::TypeName::Nullable(inner_type) => {
            DataType::Nullable(Box::new(transform_data_type(*inner_type)))
        }
        _ => unimplemented!(),
    }
}

pub fn transform_literal(lit: ASTLiteral) -> Literal {
    match lit {
        ASTLiteral::Integer(u) => {
            if u < u8::MAX as u64 {
                Literal::UInt8(u as u8)
            } else if u < u16::MAX as u64 {
                Literal::UInt16(u as u16)
            } else {
                unimplemented!()
            }
        }
        ASTLiteral::String(s) => Literal::String(s.as_bytes().to_vec()),
        ASTLiteral::Boolean(b) => Literal::Boolean(b),
        ASTLiteral::Null => Literal::Null,
        ASTLiteral::Float(f) => Literal::Float64(f),
        _ => unimplemented!("{lit}"),
    }
}

pub fn transform_span(span: &[Token]) -> Span {
    let start = span.first().unwrap().span.start;
    let end = span.last().unwrap().span.end;
    Some(start..end)
}
