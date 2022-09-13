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
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::DataType as AstDataType;
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
use sqlparser::ast::WindowSpec;

use crate::sql_common::SQLCommon;

pub enum OperatorKind {
    Unary,
    Binary,
    Other,
}

pub struct FunctionExprInfo {
    pub name: String,
    pub distinct: bool,
    pub args_count: usize,
    pub kind: OperatorKind,
    pub parameters: Vec<Value>,
    pub over: Option<WindowSpec>,
}

pub struct InListInfo {
    pub list_size: usize,
    pub negated: bool,
}

pub enum ExprRPNItem {
    Value(Value),
    Identifier(Ident),
    QualifiedIdentifier(Vec<Ident>),
    Function(FunctionExprInfo),
    Wildcard,
    Exists(Box<Query>),
    Subquery(Box<Query>),
    Cast(DataTypeImpl, bool),
    Between(bool),
    InList(InListInfo),
    MapAccess(Vec<Value>),
    Array(usize),
}

impl ExprRPNItem {
    pub fn function(name: String, args_count: usize) -> ExprRPNItem {
        ExprRPNItem::Function(FunctionExprInfo {
            name,
            distinct: false,
            args_count,
            kind: OperatorKind::Other,
            parameters: Vec::new(),
            over: None,
        })
    }

    pub fn binary_operator(name: String) -> ExprRPNItem {
        ExprRPNItem::Function(FunctionExprInfo {
            name,
            distinct: false,
            args_count: 2,
            kind: OperatorKind::Binary,
            parameters: Vec::new(),
            over: None,
        })
    }

    pub fn unary_operator(name: String) -> ExprRPNItem {
        ExprRPNItem::Function(FunctionExprInfo {
            name,
            distinct: false,
            args_count: 1,
            kind: OperatorKind::Unary,
            parameters: Vec::new(),
            over: None,
        })
    }
}

pub struct ExprRPNBuilder {
    rpn: Vec<ExprRPNItem>,
}

impl ExprRPNBuilder {
    pub fn build(expr: &Expr) -> Result<Vec<ExprRPNItem>> {
        let mut builder = ExprRPNBuilder { rpn: Vec::new() };
        ExprTraverser::accept(expr, &mut builder)?;
        Ok(builder.rpn)
    }

    fn process_expr(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Value(value) => {
                self.rpn.push(ExprRPNItem::Value(value.clone()));
            }
            Expr::Identifier(ident) => {
                self.rpn.push(ExprRPNItem::Identifier(ident.clone()));
            }
            Expr::CompoundIdentifier(idents) => {
                self.rpn
                    .push(ExprRPNItem::QualifiedIdentifier(idents.to_vec()));
            }
            Expr::IsNull(_) => {
                self.rpn
                    .push(ExprRPNItem::function(String::from("is_null"), 1));
            }
            Expr::IsNotNull(_) => {
                self.rpn
                    .push(ExprRPNItem::function(String::from("is_not_null"), 1));
            }
            Expr::UnaryOp { op, .. } => {
                match op {
                    UnaryOperator::Plus => {}
                    // In order to distinguish it from binary addition.
                    UnaryOperator::Minus => self
                        .rpn
                        .push(ExprRPNItem::unary_operator("NEGATE".to_string())),
                    _ => self.rpn.push(ExprRPNItem::unary_operator(op.to_string())),
                }
            }
            Expr::BinaryOp { op, .. } => {
                self.rpn.push(ExprRPNItem::binary_operator(op.to_string()));
            }
            Expr::Exists(subquery) => {
                self.rpn.push(ExprRPNItem::Exists(subquery.clone()));
            }
            Expr::Subquery(subquery) => {
                self.rpn.push(ExprRPNItem::Subquery(subquery.clone()));
            }
            Expr::Function(function) => {
                self.rpn.push(ExprRPNItem::Function(FunctionExprInfo {
                    name: function.name.to_string(),
                    args_count: function.args.len(),
                    distinct: function.distinct,
                    kind: OperatorKind::Other,
                    parameters: function.params.to_owned(),
                    over: function.over.clone(),
                }));
            }
            Expr::Cast {
                data_type,
                pg_style,
                ..
            } => {
                self.rpn.push(ExprRPNItem::Cast(
                    SQLCommon::make_data_type(data_type)?,
                    *pg_style,
                ));
            }
            Expr::TryCast { data_type, .. } => {
                let mut ty = SQLCommon::make_data_type(data_type)?;
                if ty.can_inside_nullable() {
                    ty = NullableType::new_impl(ty)
                }
                self.rpn.push(ExprRPNItem::Cast(ty, false));
            }
            Expr::TypedString { data_type, value } => {
                self.rpn.push(ExprRPNItem::Value(Value::SingleQuotedString(
                    value.to_string(),
                )));
                self.rpn.push(ExprRPNItem::Cast(
                    SQLCommon::make_data_type(data_type)?,
                    false,
                ));
            }
            Expr::Position { .. } => {
                let name = String::from("position");
                self.rpn.push(ExprRPNItem::function(name, 2));
            }
            Expr::Substring {
                substring_from,
                substring_for,
                ..
            } => {
                if substring_from.is_none() {
                    self.rpn
                        .push(ExprRPNItem::Value(Value::Number(String::from("1"), false)));
                }

                let name = String::from("substring");
                match substring_for {
                    None => self.rpn.push(ExprRPNItem::function(name, 2)),
                    Some(_) => {
                        self.rpn.push(ExprRPNItem::function(name, 3));
                    }
                }
            }
            Expr::Between { negated, .. } => {
                self.rpn.push(ExprRPNItem::Between(*negated));
            }
            Expr::Tuple(exprs) => {
                let len = exprs.len();

                if len > 1 {
                    self.rpn
                        .push(ExprRPNItem::function(String::from("tuple"), len));
                }
            }
            Expr::InList {
                expr: _,
                list,
                negated,
            } => self.rpn.push(ExprRPNItem::InList(InListInfo {
                list_size: list.len(),
                negated: *negated,
            })),
            Expr::Extract { field, .. } => match field {
                DateTimeField::Year => self
                    .rpn
                    .push(ExprRPNItem::function(String::from("to_year"), 1)),
                DateTimeField::Month => self
                    .rpn
                    .push(ExprRPNItem::function(String::from("to_month"), 1)),
                DateTimeField::Day => self
                    .rpn
                    .push(ExprRPNItem::function(String::from("to_day_of_month"), 1)),
                DateTimeField::Hour => self
                    .rpn
                    .push(ExprRPNItem::function(String::from("to_hour"), 1)),
                DateTimeField::Minute => self
                    .rpn
                    .push(ExprRPNItem::function(String::from("to_minute"), 1)),
                DateTimeField::Second => self
                    .rpn
                    .push(ExprRPNItem::function(String::from("to_second"), 1)),
            },
            Expr::MapAccess { keys, .. } => {
                self.rpn.push(ExprRPNItem::MapAccess(keys.to_owned()));
            }
            Expr::Trim { trim_where, .. } => match trim_where {
                None => self
                    .rpn
                    .push(ExprRPNItem::function(String::from("trim"), 1)),
                Some(_) => {
                    self.rpn
                        .push(ExprRPNItem::function(String::from("trim"), 2));
                }
            },
            Expr::Array(exprs) => {
                self.rpn.push(ExprRPNItem::Array(exprs.len()));
            }
            _ => (),
        }

        Ok(())
    }
}

pub struct ExprTraverser;

impl ExprTraverser {
    pub fn accept<V: ExprVisitor>(expr: &Expr, visitor: &mut V) -> Result<()> {
        let expr = visitor.pre_visit(expr)?;
        visitor.visit(&expr)?;
        visitor.post_visit(&expr)
    }
}

#[async_trait]
pub trait ExprVisitor: Sized + Send {
    fn pre_visit(&mut self, expr: &Expr) -> Result<Expr> {
        Ok(expr.clone())
    }

    fn visit(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Nested(expr) => ExprTraverser::accept(expr, self),
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
        ExprTraverser::accept(expr, self)?;
        for expr in list {
            ExprTraverser::accept(expr, self)?;
        }
        Ok(())
    }

    fn visit_tuple(&mut self, exprs: &[Expr]) -> Result<()> {
        match exprs.len() {
            0 => Err(ErrorCode::SyntaxException(
                "Tuple must have at least one element.",
            )),
            1 => ExprTraverser::accept(&exprs[0], self),
            _ => {
                for expr in exprs {
                    ExprTraverser::accept(expr, self)?;
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
            FunctionArgExpr::Expr(expr) => ExprTraverser::accept(expr, self),
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

        if let Some(over) = &function.over {
            for partition_by in &over.partition_by {
                ExprTraverser::accept(partition_by, self)?;
            }
            for order_by in &over.order_by {
                ExprTraverser::accept(&order_by.expr, self)?;
            }
        }

        Ok(())
    }

    fn visit_cast(
        &mut self,
        expr: &Expr,
        _data_type: &AstDataType,
        _pg_style: &bool,
    ) -> Result<()> {
        ExprTraverser::accept(expr, self)
    }

    fn visit_try_cast(&mut self, expr: &Expr, _data_type: &AstDataType) -> Result<()> {
        ExprTraverser::accept(expr, self)
    }

    fn visit_typed_string(&mut self, _data_type: &AstDataType, _value: &str) -> Result<()> {
        Ok(())
    }

    fn visit_simple_function(&mut self, expr: &Expr, _name: &str) -> Result<()> {
        ExprTraverser::accept(expr, self)
    }

    fn visit_unary_expr(&mut self, _op: &UnaryOperator, expr: &Expr) -> Result<()> {
        ExprTraverser::accept(expr, self)
    }

    fn visit_binary_expr(&mut self, left: &Expr, _op: &BinaryOperator, right: &Expr) -> Result<()> {
        ExprTraverser::accept(left, self)?;
        ExprTraverser::accept(right, self)
    }

    fn visit_between(
        &mut self,
        expr: &Expr,
        _negated: &bool,
        low: &Expr,
        high: &Expr,
    ) -> Result<()> {
        ExprTraverser::accept(expr, self)?;
        ExprTraverser::accept(low, self)?;
        ExprTraverser::accept(high, self)
    }

    fn visit_position(&mut self, substr_expr: &Expr, str_expr: &Expr) -> Result<()> {
        ExprTraverser::accept(substr_expr, self)?;
        ExprTraverser::accept(str_expr, self)
    }

    fn visit_substring(
        &mut self,
        expr: &Expr,
        from: &Option<Box<Expr>>,
        length: &Option<Box<Expr>>,
    ) -> Result<()> {
        ExprTraverser::accept(expr, self)?;

        if let Some(from) = from {
            ExprTraverser::accept(from, self)?;
        }

        if let Some(length) = length {
            ExprTraverser::accept(length, self)?;
        }

        Ok(())
    }

    fn visit_extract(&mut self, _field: &DateTimeField, expr: &Expr) -> Result<()> {
        ExprTraverser::accept(expr, self)
    }

    fn visit_map_access(&mut self, expr: &Expr, _keys: &[Value]) -> Result<()> {
        ExprTraverser::accept(expr, self)
    }

    fn visit_trim(
        &mut self,
        expr: &Expr,
        trim_where: &Option<(TrimWhereField, Box<Expr>)>,
    ) -> Result<()> {
        ExprTraverser::accept(expr, self)?;

        if let Some(trim_where) = trim_where {
            ExprTraverser::accept(&trim_where.1, self)?;
        }
        Ok(())
    }

    fn visit_array(&mut self, exprs: &[Expr]) -> Result<()> {
        for expr in exprs {
            ExprTraverser::accept(expr, self)?;
        }
        Ok(())
    }
}

#[async_trait]
impl ExprVisitor for ExprRPNBuilder {
    fn pre_visit(&mut self, expr: &Expr) -> Result<Expr> {
        Ok(expr.clone())
    }

    fn post_visit(&mut self, expr: &Expr) -> Result<()> {
        self.process_expr(expr)
    }

    fn visit_wildcard(&mut self) -> Result<()> {
        self.rpn.push(ExprRPNItem::Wildcard);
        Ok(())
    }
}
