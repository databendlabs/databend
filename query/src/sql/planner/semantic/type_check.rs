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
use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_ast::ast::UnaryOperator;
use common_datavalues::BooleanType;
use common_datavalues::DataField;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionFactory;

use crate::sql::plans::AggregateFunction;
use crate::sql::plans::AndExpr;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::ComparisonOp;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::FunctionCall;
use crate::sql::plans::OrExpr;
use crate::sql::plans::Scalar;
use crate::sql::BindContext;

/// A helper for type checking.
///
/// `TypeChecker::resolve` will resolve types of `Expr` and transform `Expr` into
/// a typed expression `Scalar`. At the same time, name resolution will be performed,
/// which check validity of unbound `ColumnRef` and try to replace it with qualified
/// `BoundColumnRef`.
///
/// If failed, a `SemanticError` will be raised. This may caused by incompitible
/// argument types of expressions, or inresolvable columns.
pub struct TypeChecker<'a> {
    bind_context: &'a BindContext,
}

impl<'a> TypeChecker<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    /// Resolve types of `expr` with given `required_type`.
    /// If `required_type` is None, then there is no requirement of return type.
    ///
    /// TODO(leiysky): choose correct overloads of functions with given required_type and arguments
    pub fn resolve(
        &self,
        expr: &Expr,
        required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        match expr {
            Expr::ColumnRef {
                database: _,
                table,
                column,
            } => {
                let column = self
                    .bind_context
                    .resolve_column(table.clone().map(|ident| ident.name), column.name.clone())?;
                let data_type = column.data_type.clone();

                Ok((BoundColumnRef { column }.into(), data_type))
            }

            Expr::IsNull { expr, not } => {
                let func_name = if *not {
                    "is_not_null".to_string()
                } else {
                    "is_null".to_string()
                };

                self.resolve_function(func_name.as_str(), &[&**expr], required_type)
            }

            Expr::InList { expr, list, not } => {
                let func_name = if *not {
                    "not_in".to_string()
                } else {
                    "in".to_string()
                };
                let mut args = Vec::with_capacity(list.len() + 1);
                args.push(&**expr);
                for expr in list.iter() {
                    args.push(expr);
                }
                self.resolve_function(func_name.as_str(), &args, required_type)
            }

            Expr::Between {
                expr,
                low,
                high,
                not,
            } => {
                if !*not {
                    // Rewrite `expr BETWEEN low AND high`
                    // into `expr >= low AND expr <= high`
                    let (ge_func, _) = self.resolve_function(
                        ">=",
                        &[&**expr, &**low],
                        Some(BooleanType::new_impl()),
                    )?;
                    let (le_func, _) = self.resolve_function(
                        "<=",
                        &[&**expr, &**high],
                        Some(BooleanType::new_impl()),
                    )?;
                    Ok((
                        AndExpr {
                            left: Box::new(ge_func),
                            right: Box::new(le_func),
                        }
                        .into(),
                        BooleanType::new_impl(),
                    ))
                } else {
                    // Rewrite `expr NOT BETWEEN low AND high`
                    // into `expr < low OR expr > high`
                    let (lt_func, _) = self.resolve_function(
                        "<",
                        &[&**expr, &**low],
                        Some(BooleanType::new_impl()),
                    )?;
                    let (gt_func, _) = self.resolve_function(
                        ">",
                        &[&**expr, &**high],
                        Some(BooleanType::new_impl()),
                    )?;
                    Ok((
                        OrExpr {
                            left: Box::new(lt_func),
                            right: Box::new(gt_func),
                        }
                        .into(),
                        BooleanType::new_impl(),
                    ))
                }
            }

            Expr::BinaryOp { op, left, right } => {
                self.resolve_binary_op(op, &**left, &**right, required_type)
            }

            Expr::UnaryOp { op, expr } => self.resolve_unary_op(op, &**expr, required_type),

            Expr::Cast {
                expr, target_type, ..
            } => {
                let (scalar, data_type) = self.resolve(expr, required_type)?;
                let cast_func = CastFunction::create_try(
                    "",
                    target_type.to_string().as_str(),
                    data_type.clone(),
                )?;
                Ok((
                    CastExpr {
                        argument: Box::new(scalar),
                        from_type: data_type,
                        target_type: cast_func.return_type(),
                    }
                    .into(),
                    cast_func.return_type(),
                ))
            }

            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                let mut arguments = vec![&**expr];
                match (substring_from, substring_for) {
                    (Some(from_expr), None) => {
                        arguments.push(&**from_expr);
                    }
                    (Some(from_expr), Some(for_expr)) => {
                        arguments.push(&**from_expr);
                        arguments.push(&**for_expr);
                    }
                    _ => return Err(ErrorCode::SemanticError("Invalid arguments of SUBSTRING")),
                }

                self.resolve_function("substring", &arguments, required_type)
            }

            Expr::Literal(literal) => {
                let value = self.parse_literal(literal, required_type)?;
                let data_type = value.data_type();
                Ok((ConstantExpr { value }.into(), data_type))
            }

            Expr::FunctionCall {
                distinct,
                name,
                args,
                params,
            } => {
                let args: Vec<&Expr> = args.iter().collect();
                let func_name = name.name.as_str();

                if AggregateFunctionFactory::instance().check(func_name) {
                    // Check aggregate function
                    let params = params
                        .iter()
                        .map(|literal| self.parse_literal(literal, None))
                        .collect::<Result<Vec<DataValue>>>()?;
                    let arguments = args
                        .iter()
                        .map(|arg| self.resolve(arg, None))
                        .collect::<Result<Vec<_>>>()?;

                    let data_fields = arguments
                        .iter()
                        .map(|(_, data_type)| DataField::new("", data_type.clone()))
                        .collect();
                    let agg_func = AggregateFunctionFactory::instance().get(
                        func_name,
                        params.clone(),
                        data_fields,
                    )?;

                    Ok((
                        AggregateFunction {
                            func_name: agg_func.name().to_string(),
                            distinct: *distinct,
                            params,
                            args: arguments.into_iter().map(|arg| arg.0).collect(),
                            return_type: agg_func.return_type()?,
                        }
                        .into(),
                        agg_func.return_type()?,
                    ))
                } else {
                    // Scalar function
                    self.resolve_function(func_name, &args, required_type)
                }
            }

            Expr::CountAll => {
                let agg_func = AggregateFunctionFactory::instance().get("count", vec![], vec![])?;

                Ok((
                    AggregateFunction {
                        func_name: "count".to_string(),
                        distinct: false,
                        params: vec![],
                        args: vec![],
                        return_type: agg_func.return_type()?,
                    }
                    .into(),
                    agg_func.return_type()?,
                ))
            }

            _ => Err(ErrorCode::UnImplement(format!(
                "Unsupported expr: {:?}",
                expr
            ))),
        }
    }

    /// Resolve function call.
    pub fn resolve_function(
        &self,
        func_name: &str,
        arguments: &[&Expr],
        required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        let (args, arg_types): (Vec<Scalar>, Vec<DataTypeImpl>) = arguments
            .iter()
            .map(|expr| self.resolve(expr, required_type.clone()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        let arg_types_ref: Vec<&DataTypeImpl> = arg_types.iter().collect();

        let func = FunctionFactory::instance().get(func_name, &arg_types_ref)?;
        Ok((
            FunctionCall {
                arguments: args,
                func_name: func_name.to_string(),
                arg_types: arg_types.to_vec(),
                return_type: func.return_type(),
            }
            .into(),
            func.return_type(),
        ))
    }

    /// Resolve binary expressions. Most of the binary expressions
    /// would be transformed into `FunctionCall`, except comparison
    /// expressions, conjunction(`AND`) and disjunction(`OR`).
    pub fn resolve_binary_op(
        &self,
        op: &BinaryOperator,
        left: &Expr,
        right: &Expr,
        required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Div
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
            | BinaryOperator::StringConcat
            | BinaryOperator::Like
            | BinaryOperator::NotLike
            | BinaryOperator::Regexp
            | BinaryOperator::RLike
            | BinaryOperator::NotRegexp
            | BinaryOperator::NotRLike
            | BinaryOperator::BitwiseOr
            | BinaryOperator::BitwiseAnd
            | BinaryOperator::BitwiseXor
            | BinaryOperator::Xor => {
                self.resolve_function(op.to_string().as_str(), &[left, right], required_type)
            }
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::Gte
            | BinaryOperator::Lte
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                let op = ComparisonOp::try_from(op)?;
                let (left, _) = self.resolve(left, None)?;
                let (right, _) = self.resolve(right, None)?;

                Ok((
                    ComparisonExpr {
                        op,
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    BooleanType::new_impl(),
                ))
            }
            BinaryOperator::And => {
                let (left, _) = self.resolve(left, Some(BooleanType::new_impl()))?;
                let (right, _) = self.resolve(right, Some(BooleanType::new_impl()))?;

                Ok((
                    AndExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    BooleanType::new_impl(),
                ))
            }
            BinaryOperator::Or => {
                let (left, _) = self.resolve(left, Some(BooleanType::new_impl()))?;
                let (right, _) = self.resolve(right, Some(BooleanType::new_impl()))?;

                Ok((
                    OrExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    BooleanType::new_impl(),
                ))
            }
        }
    }

    /// Resolve unary expressions.
    pub fn resolve_unary_op(
        &self,
        op: &UnaryOperator,
        child: &Expr,
        required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        self.resolve_function(op.to_string().as_str(), &[child], required_type)
    }

    /// Resolve literal values.
    fn parse_literal(
        &self,
        literal: &Literal,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<DataValue> {
        // TODO(leiysky): try cast value to required type
        let value = match literal {
            Literal::Number(string) => DataValue::try_from_literal(string, None)?,
            Literal::String(string) => DataValue::String(string.as_bytes().to_vec()),
            Literal::Boolean(boolean) => DataValue::Boolean(*boolean),
            Literal::Null => DataValue::Null,
            _ => Err(ErrorCode::SemanticError(format!(
                "Unsupported literal value: {literal}"
            )))?,
        };

        Ok(value)
    }
}
