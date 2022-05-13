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
use sqlparser::ast::DateTimeField;
use sqlparser::ast::Expr;
use sqlparser::ast::Function;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::FunctionArgExpr;
use sqlparser::ast::Ident;
use sqlparser::ast::Query;
use sqlparser::ast::TrimWhereField;
use sqlparser::ast::UnaryOperator;
use sqlparser::ast::Value;

pub struct UDFExprTraverser;

impl UDFExprTraverser {
    pub fn accept<V: UDFExprVisitor>(expr: &Expr, visitor: &mut V) -> Result<()> {
        let expr = visitor.pre_visit(expr)?;
        visitor.visit(&expr)?;
        visitor.post_visit(&expr)
    }
}

#[async_trait]
pub trait UDFExprVisitor: Sized + Send {
    fn pre_visit(&mut self, expr: &Expr) -> Result<Expr> {
        Ok(expr.clone())
    }

    fn visit(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Nested(expr) => UDFExprTraverser::accept(expr, self),
            Expr::Value(value) => self.visit_value(value),
            Expr::Identifier(ident) => self.visit_identifier(ident),
            Expr::CompoundIdentifier(idents) => self.visit_identifiers(idents),
            Expr::IsNull(expr) => self.visit_simple_function(expr, "is_null"),
            Expr::IsNotNull(expr) => self.visit_simple_function(expr, "is_not_null"),
            Expr::UnaryOp { op, expr } => self.visit_unary_expr(op, expr),
            Expr::BinaryOp { left, op, right } => self.visit_binary_expr(left, op, right),
            Expr::Exists(subquery) => self.visit_exists(subquery),
            Expr::Subquery(subquery) => self.visit_subquery(subquery),
            Expr::Function(function) => self.visit_function(function),
            Expr::TryCast { expr, data_type } => self.visit_try_cast(expr, data_type),
            Expr::Cast {
                expr,
                data_type,
                pg_style,
            } => self.visit_cast(expr, data_type, pg_style),
            Expr::TypedString { data_type, value } => self.visit_typed_string(data_type, value),
            Expr::Position {
                substr_expr,
                str_expr,
            } => self.visit_position(substr_expr, str_expr),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => self.visit_substring(expr, substring_from, substring_for),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => self.visit_between(expr, negated, low, high),
            Expr::Tuple(exprs) => self.visit_tuple(exprs),
            Expr::InList { expr, list, .. } => self.visit_inlist(expr, list),
            Expr::Extract { field, expr } => self.visit_extract(field, expr),
            Expr::MapAccess { column, keys } => self.visit_map_access(column, keys),
            Expr::Trim { expr, trim_where } => self.visit_trim(expr, trim_where),
            Expr::Array(exprs) => self.visit_array(exprs),
            other => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported expression: {}, type: {:?}",
                expr, other
            ))),
        }
    }

    fn post_visit(&mut self, _expr: &Expr) -> Result<()> {
        Ok(())
    }

    fn visit_inlist(&mut self, expr: &Expr, list: &[Expr]) -> Result<()> {
        UDFExprTraverser::accept(expr, self)?;
        for expr in list {
            UDFExprTraverser::accept(expr, self)?;
        }
        Ok(())
    }

    fn visit_tuple(&mut self, exprs: &[Expr]) -> Result<()> {
        match exprs.len() {
            0 => Err(ErrorCode::SyntaxException(
                "Tuple must have at least one element.",
            )),
            1 => UDFExprTraverser::accept(&exprs[0], self),
            _ => {
                for expr in exprs {
                    UDFExprTraverser::accept(expr, self)?;
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

    fn visit_function_arg(&mut self, arg_expr: &FunctionArgExpr) -> Result<()> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => UDFExprTraverser::accept(expr, self),
            FunctionArgExpr::Wildcard => self.visit_wildcard(),
            FunctionArgExpr::QualifiedWildcard(_) => Err(ErrorCode::SyntaxException(std::format!(
                "Unsupported QualifiedWildcard: {}",
                arg_expr
            ))),
        }
    }

    fn visit_function(&mut self, function: &Function) -> Result<()> {
        for function_arg in &function.args {
            match function_arg {
                FunctionArg::Named { arg, .. } => self.visit_function_arg(arg)?,
                FunctionArg::Unnamed(arg) => self.visit_function_arg(arg)?,
            };
        }

        Ok(())
    }

    fn visit_cast(&mut self, expr: &Expr, _data_type: &DataType, _pg_style: &bool) -> Result<()> {
        UDFExprTraverser::accept(expr, self)
    }

    fn visit_try_cast(&mut self, expr: &Expr, _data_type: &DataType) -> Result<()> {
        UDFExprTraverser::accept(expr, self)
    }

    fn visit_typed_string(&mut self, _data_type: &DataType, _value: &str) -> Result<()> {
        Ok(())
    }

    fn visit_simple_function(&mut self, expr: &Expr, _name: &str) -> Result<()> {
        UDFExprTraverser::accept(expr, self)
    }

    fn visit_unary_expr(&mut self, _op: &UnaryOperator, expr: &Expr) -> Result<()> {
        UDFExprTraverser::accept(expr, self)
    }

    fn visit_binary_expr(&mut self, left: &Expr, _op: &BinaryOperator, right: &Expr) -> Result<()> {
        UDFExprTraverser::accept(left, self)?;
        UDFExprTraverser::accept(right, self)
    }

    fn visit_between(
        &mut self,
        expr: &Expr,
        _negated: &bool,
        low: &Expr,
        high: &Expr,
    ) -> Result<()> {
        UDFExprTraverser::accept(expr, self)?;
        UDFExprTraverser::accept(low, self)?;
        UDFExprTraverser::accept(high, self)
    }

    fn visit_position(&mut self, substr_expr: &Expr, str_expr: &Expr) -> Result<()> {
        UDFExprTraverser::accept(substr_expr, self)?;
        UDFExprTraverser::accept(str_expr, self)
    }

    fn visit_substring(
        &mut self,
        expr: &Expr,
        from: &Option<Box<Expr>>,
        length: &Option<Box<Expr>>,
    ) -> Result<()> {
        UDFExprTraverser::accept(expr, self)?;

        if let Some(from) = from {
            UDFExprTraverser::accept(from, self)?;
        }

        if let Some(length) = length {
            UDFExprTraverser::accept(length, self)?;
        }

        Ok(())
    }

    fn visit_extract(&mut self, _field: &DateTimeField, expr: &Expr) -> Result<()> {
        UDFExprTraverser::accept(expr, self)
    }

    fn visit_map_access(&mut self, expr: &Expr, _keys: &[Value]) -> Result<()> {
        UDFExprTraverser::accept(expr, self)
    }

    fn visit_trim(
        &mut self,
        expr: &Expr,
        trim_where: &Option<(TrimWhereField, Box<Expr>)>,
    ) -> Result<()> {
        UDFExprTraverser::accept(expr, self)?;

        if let Some(trim_where) = trim_where {
            UDFExprTraverser::accept(&trim_where.1, self)?;
        }
        Ok(())
    }

    fn visit_array(&mut self, exprs: &[Expr]) -> Result<()> {
        match exprs.len() {
            0 => Err(ErrorCode::SyntaxException(
                "Array must have at least one element.",
            )),
            _ => {
                for expr in exprs {
                    UDFExprTraverser::accept(expr, self)?;
                }
                Ok(())
            }
        }
    }
}
