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
use common_datavalues::type_coercion::merge_types;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_planners::Expression;
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

use crate::analyzer_value_expr::ValueExprAnalyzer;
use crate::sql_common::SQLCommon;
use crate::sql_dialect::SQLDialect;

#[derive(Clone)]
pub struct ExpressionSyncAnalyzer {}

impl ExpressionSyncAnalyzer {
    pub fn create() -> ExpressionSyncAnalyzer {
        ExpressionSyncAnalyzer {}
    }

    pub fn analyze(&self, expr: &Expr) -> Result<Expression> {
        let mut stack = Vec::new();

        // Build RPN for expr. Because async function unsupported recursion
        for rpn_item in &ExprRPNBuilder::build(expr)? {
            match rpn_item {
                ExprRPNItem::Value(v) => Self::analyze_value(v, &mut stack, SQLDialect::MySQL)?,
                ExprRPNItem::Identifier(v) => self.analyze_identifier(v, &mut stack)?,
                ExprRPNItem::QualifiedIdentifier(v) => self.analyze_identifiers(v, &mut stack)?,
                ExprRPNItem::Function(v) => self.analyze_function(v, &mut stack)?,
                ExprRPNItem::Cast(v, pg_style) => self.analyze_cast(v, *pg_style, &mut stack)?,
                ExprRPNItem::Between(negated) => self.analyze_between(*negated, &mut stack)?,
                ExprRPNItem::InList(v) => self.analyze_inlist(v, &mut stack)?,
                ExprRPNItem::MapAccess(v) => self.analyze_map_access(v, &mut stack)?,
                ExprRPNItem::Array(v) => self.analyze_array(*v, &mut stack)?,

                _ => {
                    return Err(ErrorCode::LogicalError(format!(
                        "Logical error: can't analyze {:?} in sync mode, it's a bug",
                        expr
                    )));
                }
            }
        }

        match stack.len() {
            1 => Ok(stack.remove(0)),
            _ => Err(ErrorCode::LogicalError(
                "Logical error: this is expr rpn bug.",
            )),
        }
    }

    pub fn analyze_function_arg(&self, arg_expr: &FunctionArgExpr) -> Result<Expression> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => self.analyze(expr),
            FunctionArgExpr::Wildcard => Ok(Expression::Wildcard),
            FunctionArgExpr::QualifiedWildcard(_) => Err(ErrorCode::SyntaxException(std::format!(
                "Unsupported arg statement: {}",
                arg_expr
            ))),
        }
    }

    fn analyze_value(
        value: &Value,
        args: &mut Vec<Expression>,
        typ: impl Into<SQLDialect>,
    ) -> Result<()> {
        args.push(ValueExprAnalyzer::analyze(value, typ)?);
        Ok(())
    }

    fn analyze_inlist(&self, info: &InListInfo, args: &mut Vec<Expression>) -> Result<()> {
        let mut list = Vec::with_capacity(info.list_size);
        for _ in 0..info.list_size {
            match args.pop() {
                None => {
                    return Err(ErrorCode::LogicalError("It's a bug."));
                }
                Some(arg) => {
                    list.insert(0, arg);
                }
            }
        }

        let expr = args
            .pop()
            .ok_or_else(|| ErrorCode::LogicalError("It's a bug."))?;
        list.insert(0, expr);

        let op = if info.negated {
            "NOT_IN".to_string()
        } else {
            "IN".to_string()
        };

        args.push(Expression::ScalarFunction { op, args: list });
        Ok(())
    }

    fn analyze_function(&self, info: &FunctionExprInfo, args: &mut Vec<Expression>) -> Result<()> {
        let mut arguments = Vec::with_capacity(info.args_count);
        for _ in 0..info.args_count {
            match args.pop() {
                None => {
                    return Err(ErrorCode::LogicalError("It's a bug."));
                }
                Some(arg) => {
                    arguments.insert(0, arg);
                }
            }
        }

        args.push(
            match AggregateFunctionFactory::instance().check(&info.name) {
                true => {
                    return Err(ErrorCode::LogicalError(
                        "Unsupport aggregate function, it's a bug.",
                    ));
                }
                false => match info.kind {
                    OperatorKind::Unary => Self::unary_function(info, &arguments),
                    OperatorKind::Binary => Self::binary_function(info, &arguments),
                    OperatorKind::Other => Self::other_function(info, &arguments),
                },
            }?,
        );
        Ok(())
    }

    fn other_function(info: &FunctionExprInfo, args: &[Expression]) -> Result<Expression> {
        let op = info.name.clone();
        let arguments = args.to_owned();
        Ok(Expression::ScalarFunction {
            op,
            args: arguments,
        })
    }

    fn unary_function(info: &FunctionExprInfo, args: &[Expression]) -> Result<Expression> {
        match args.is_empty() {
            true => Err(ErrorCode::LogicalError("Unary operator must be one child.")),
            false => Ok(Expression::UnaryExpression {
                op: info.name.clone(),
                expr: Box::new(args[0].to_owned()),
            }),
        }
    }

    fn binary_function(info: &FunctionExprInfo, args: &[Expression]) -> Result<Expression> {
        let op = info.name.clone();
        match args.len() < 2 {
            true => Err(ErrorCode::LogicalError(
                "Binary operator must be two children.",
            )),
            false => Ok(Expression::BinaryExpression {
                op,
                left: Box::new(args[0].to_owned()),
                right: Box::new(args[1].to_owned()),
            }),
        }
    }

    fn analyze_identifier(&self, ident: &Ident, arguments: &mut Vec<Expression>) -> Result<()> {
        let column_name = ident.clone().value;
        arguments.push(Expression::Column(column_name));
        Ok(())
    }

    fn analyze_identifiers(&self, idents: &[Ident], arguments: &mut Vec<Expression>) -> Result<()> {
        let mut names = Vec::with_capacity(idents.len());

        for ident in idents {
            names.push(ident.clone().value);
        }

        arguments.push(Expression::QualifiedColumn(names));
        Ok(())
    }

    fn analyze_cast(
        &self,
        data_type: &DataTypeImpl,
        pg_style: bool,
        args: &mut Vec<Expression>,
    ) -> Result<()> {
        match args.pop() {
            None => Err(ErrorCode::LogicalError(
                "Cast operator must be one children.",
            )),
            Some(inner_expr) => {
                args.push(Expression::Cast {
                    expr: Box::new(inner_expr),
                    data_type: data_type.clone(),
                    pg_style,
                });
                Ok(())
            }
        }
    }

    fn analyze_between(&self, negated: bool, args: &mut Vec<Expression>) -> Result<()> {
        if args.len() < 3 {
            return Err(ErrorCode::SyntaxException(
                "Between must be a ternary expression.",
            ));
        }

        let s_args = args.split_off(args.len() - 3);
        let expression = s_args[0].clone();
        let low_expression = s_args[1].clone();
        let high_expression = s_args[2].clone();

        match negated {
            false => args.push(
                expression
                    .gt_eq(low_expression)
                    .and(expression.lt_eq(high_expression)),
            ),
            true => args.push(
                expression
                    .lt(low_expression)
                    .or(expression.gt(high_expression)),
            ),
        };

        Ok(())
    }

    fn analyze_map_access(&self, keys: &[Value], args: &mut Vec<Expression>) -> Result<()> {
        match args.pop() {
            None => Err(ErrorCode::LogicalError(
                "MapAccess operator must be one children.",
            )),
            Some(inner_expr) => {
                let path_name: String = keys
                    .iter()
                    .enumerate()
                    .map(|(i, k)| match k {
                        k @ Value::Number(_, _) => format!("[{}]", k),
                        Value::SingleQuotedString(s) => format!("[\"{}\"]", s),
                        Value::ColonString(s) => {
                            if i == 0 {
                                s.to_string()
                            } else {
                                format!(":{}", s)
                            }
                        }
                        Value::PeriodString(s) => format!(".{}", s),
                        _ => format!("[{}]", k),
                    })
                    .collect();

                let name = match keys[0] {
                    Value::ColonString(_) => format!("{}:{}", inner_expr.column_name(), path_name),
                    _ => format!("{}{}", inner_expr.column_name(), path_name),
                };
                let path =
                    Expression::create_literal(DataValue::String(path_name.as_bytes().to_vec()));
                let arguments = vec![inner_expr, path];

                args.push(Expression::MapAccess {
                    name,
                    args: arguments,
                });
                Ok(())
            }
        }
    }

    fn analyze_array(&self, nums: usize, args: &mut Vec<Expression>) -> Result<()> {
        let mut values = Vec::with_capacity(nums);
        let mut types = Vec::with_capacity(nums);
        for _ in 0..nums {
            match args.pop() {
                None => {
                    break;
                }
                Some(inner_expr) => {
                    if let Expression::Literal {
                        value, data_type, ..
                    } = inner_expr
                    {
                        values.push(value);
                        types.push(data_type);
                    }
                }
            };
        }
        if values.len() != nums {
            return Err(ErrorCode::LogicalError(format!(
                "Array must have {} children.",
                nums
            )));
        }
        let inner_type = if types.is_empty() {
            NullType::new_impl()
        } else {
            types
                .iter()
                .fold(Ok(types[0].clone()), |acc, v| merge_types(&acc?, v))
                .map_err(|e| ErrorCode::LogicalError(e.message()))?
        };
        values.reverse();

        let array_value = Expression::create_literal_with_type(
            DataValue::Array(values),
            ArrayType::new_impl(inner_type),
        );
        args.push(array_value);
        Ok(())
    }
}

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
