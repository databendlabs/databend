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

use common_ast::ast::Literal as ASTLiteral;
use common_ast::parser::parse_expr;
use common_ast::parser::token::Token;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_expression::expression::Literal;
use common_expression::expression::RawExpr;
use common_expression::expression::Span;
use common_expression::types::DataType;

pub fn parse_raw_expr(text: &str, columns: &[(&str, DataType)]) -> RawExpr {
    let backtrace = Backtrace::new();
    let tokens = tokenize_sql(text).unwrap();
    let expr = parse_expr(&tokens, &backtrace).unwrap();
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
                .unwrap();
            RawExpr::ColumnRef {
                span: transform_span(span),
                id: col_id,
                data_type: columns[col_id].1.clone(),
            }
        }
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
        _ => unimplemented!(),
    }
}

pub fn transform_span(span: &[Token]) -> Span {
    let start = span.first().unwrap().span.start;
    let end = span.last().unwrap().span.end;
    Some(start..end)
}
