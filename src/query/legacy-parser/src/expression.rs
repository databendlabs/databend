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

use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::token::Token;
use common_ast::parser::token::Tokenizer;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;

/// unchecked_expressions_analyze will analyzer given expr str into `Expression`.
///
/// `unchecked` means there is no correction checks for expr:
///
/// - We don't if the expr is **safe**.
/// - We don't if the expr is **existed**.
/// - We don't if the expr is **valid**.
///
/// We just convert the given expr into Expression, and hoping everything works.
///
/// This function should only be used by very limited use-cases like fuse engines
/// `cluster_keys` which already checked by our parsers.
///
/// **FBI WARNING: DON'T USE THIS FUNCTION UNLESS YOU KNOW WHAT I MEAN**
///
/// Any usage to this function should be reviewed carefully.
///
/// # Future Plan
///
/// This function should be removed once our new expression framework is ready.
pub fn unchecked_expressions_analyze(expr: &str) -> Result<Vec<Expression>> {
    let tokens: Vec<Token> = Tokenizer::new(expr).collect::<Result<Vec<_>>>()?;
    let backtrace = Backtrace::new();
    let exprs = parse_comma_separated_exprs(&tokens, Dialect::MySQL, &backtrace)?;

    exprs
        .into_iter()
        .map(unchecked_expression_analyze)
        .collect()
}

fn unchecked_expression_analyze(expr: Expr) -> Result<Expression> {
    match expr {
        Expr::ColumnRef {
            database,
            table,
            column,
            ..
        } => {
            if database.is_none() && table.is_none() {
                Ok(Expression::Column(column.to_string()))
            } else if database.is_none() {
                Ok(Expression::QualifiedColumn(vec![
                    table.unwrap().to_string(),
                    column.to_string(),
                ]))
            } else {
                Ok(Expression::QualifiedColumn(vec![
                    database.unwrap().to_string(),
                    table.unwrap().to_string(),
                    column.to_string(),
                ]))
            }
        }
        Expr::BinaryOp {
            op, left, right, ..
        } => Ok(Expression::BinaryExpression {
            left: Box::new(unchecked_expression_analyze(*left)?),
            op: op.to_string(),
            right: Box::new(unchecked_expression_analyze(*right)?),
        }),
        Expr::Literal { lit, .. } => {
            let value = match lit {
                Literal::Integer(uint) => DataValue::UInt64(uint),
                Literal::Float(float) => DataValue::Float64(float),
                Literal::String(string) => DataValue::String(string.as_bytes().to_vec()),
                Literal::Boolean(boolean) => DataValue::Boolean(boolean),
                Literal::Null => DataValue::Null,
                _ => Err(ErrorCode::SemanticError(format!(
                    "Unsupported literal value: {lit}"
                )))?,
            };

            let data_type = value.data_type();

            Ok(Expression::Literal {
                value,
                column_name: None,
                data_type,
            })
        }
        _ => Err(ErrorCode::LogicalError(format!(
            "Logical error: can't analyze {:?}",
            expr
        ))),
    }
}
