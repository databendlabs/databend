// Copyright 2021 Datafuse Labs.
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

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::DataType;
use sqlparser::ast::Expr;
use sqlparser::ast::Function;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::FunctionArgExpr;
use sqlparser::ast::Ident;
use sqlparser::ast::Query;
use sqlparser::ast::UnaryOperator;
use sqlparser::ast::Value;

pub struct ExprTraverser;

impl ExprTraverser {
    pub async fn accept<V: ExprVisitor>(expr: &Expr, visitor: &mut V) -> Result<()> {
        let expr = visitor.pre_visit(expr).await?;
        visitor.visit(&expr).await?;
        visitor.post_visit(&expr).await
    }
}

#[async_trait]
pub trait ExprVisitor: Sized + Send {
    async fn pre_visit(&mut self, expr: &Expr) -> Result<Expr> {
        Ok(expr.clone())
    }

    async fn visit(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Nested(expr) => ExprTraverser::accept(expr, self).await,
            Expr::Value(value) => self.visit_value(value),
            Expr::Identifier(ident) => self.visit_identifier(ident),
            Expr::CompoundIdentifier(idents) => self.visit_identifiers(idents),
            Expr::IsNull(expr) => self.visit_simple_function(expr, "isnull").await,
            Expr::IsNotNull(expr) => self.visit_simple_function(expr, "isnotnull").await,
            Expr::UnaryOp { op, expr } => self.visit_unary_expr(op, expr).await,
            Expr::BinaryOp { left, op, right } => self.visit_binary_expr(left, op, right).await,
            Expr::Exists(subquery) => self.visit_exists(subquery),
            Expr::Subquery(subquery) => self.visit_subquery(subquery),
            Expr::Function(function) => self.visit_function(function).await,
            Expr::Cast { expr, data_type } => self.visit_cast(expr, data_type).await,
            Expr::TypedString { data_type, value } => self.visit_typed_string(data_type, value),
            Expr::Position {
                substr_expr,
                str_expr,
            } => self.visit_position(substr_expr, str_expr).await,
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                self.visit_substring(expr, substring_from, substring_for)
                    .await
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => self.visit_between(expr, negated, low, high).await,
            Expr::Tuple(exprs) => self.visit_tuple(exprs).await,
            Expr::InList { expr, list, .. } => self.visit_inlist(expr, list).await,
            other => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported expression: {}, type: {:?}",
                expr, other
            ))),
        }
    }

    async fn post_visit(&mut self, _expr: &Expr) -> Result<()> {
        Ok(())
    }

    async fn visit_inlist(&mut self, expr: &Expr, list: &[Expr]) -> Result<()> {
        ExprTraverser::accept(expr, self).await?;
        for expr in list {
            ExprTraverser::accept(expr, self).await?;
        }
        Ok(())
    }

    async fn visit_tuple(&mut self, exprs: &[Expr]) -> Result<()> {
        match exprs.len() {
            0 => Err(ErrorCode::SyntaxException(
                "Tuple must have at least one element.",
            )),
            1 => ExprTraverser::accept(&exprs[0], self).await,
            _ => {
                for expr in exprs {
                    ExprTraverser::accept(expr, self).await?;
                }

                Ok(())
            }
        }
    }

    fn visit_wildcard(&mut self) -> Result<()> {
        Ok(())
    }

    fn visit_value(&mut self, _value: &Value) -> Result<()> {
        Ok(())
    }

    fn visit_identifier(&mut self, _ident: &Ident) -> Result<()> {
        Ok(())
    }

    fn visit_identifiers(&mut self, _idents: &[Ident]) -> Result<()> {
        Ok(())
    }

    fn visit_exists(&mut self, _subquery: &Query) -> Result<()> {
        Ok(())
    }

    fn visit_subquery(&mut self, _subquery: &Query) -> Result<()> {
        Ok(())
    }

    async fn visit_function_arg(&mut self, arg_expr: &FunctionArgExpr) -> Result<()> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => ExprTraverser::accept(expr, self).await,
            FunctionArgExpr::Wildcard => self.visit_wildcard(),
            FunctionArgExpr::QualifiedWildcard(_) => Err(ErrorCode::SyntaxException(std::format!(
                "Unsupported QualifiedWildcard: {}",
                arg_expr
            ))),
        }
    }

    async fn visit_function(&mut self, function: &Function) -> Result<()> {
        for function_arg in &function.args {
            match function_arg {
                FunctionArg::Named { arg, .. } => self.visit_function_arg(arg).await?,
                FunctionArg::Unnamed(arg) => self.visit_function_arg(arg).await?,
            };
        }

        Ok(())
    }

    async fn visit_cast(&mut self, expr: &Expr, _data_type: &DataType) -> Result<()> {
        ExprTraverser::accept(expr, self).await
    }

    fn visit_typed_string(&mut self, _data_type: &DataType, _value: &str) -> Result<()> {
        Ok(())
    }

    async fn visit_simple_function(&mut self, expr: &Expr, _name: &str) -> Result<()> {
        ExprTraverser::accept(expr, self).await
    }

    async fn visit_unary_expr(&mut self, _op: &UnaryOperator, expr: &Expr) -> Result<()> {
        ExprTraverser::accept(expr, self).await
    }

    async fn visit_binary_expr(
        &mut self,
        left: &Expr,
        _op: &BinaryOperator,
        right: &Expr,
    ) -> Result<()> {
        ExprTraverser::accept(left, self).await?;
        ExprTraverser::accept(right, self).await
    }

    async fn visit_between(
        &mut self,
        expr: &Expr,
        _negated: &bool,
        low: &Expr,
        high: &Expr,
    ) -> Result<()> {
        ExprTraverser::accept(expr, self).await?;
        ExprTraverser::accept(low, self).await?;
        ExprTraverser::accept(high, self).await
    }

    async fn visit_position(&mut self, substr_expr: &Expr, str_expr: &Expr) -> Result<()> {
        ExprTraverser::accept(substr_expr, self).await?;
        ExprTraverser::accept(str_expr, self).await
    }

    async fn visit_substring(
        &mut self,
        expr: &Expr,
        from: &Option<Box<Expr>>,
        length: &Option<Box<Expr>>,
    ) -> Result<()> {
        ExprTraverser::accept(expr, self).await?;

        if let Some(from) = from {
            ExprTraverser::accept(from, self).await?;
        }

        if let Some(length) = length {
            ExprTraverser::accept(length, self).await?;
        }

        Ok(())
    }
}
