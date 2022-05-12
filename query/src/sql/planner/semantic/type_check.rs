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

use std::sync::Arc;

use common_ast::ast::BinaryOperator;
use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_ast::ast::Query;
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

use crate::sessions::QueryContext;
use crate::sql::binder::Binder;
use crate::sql::planner::metadata::optimize_remove_count_args;
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
use crate::sql::plans::SubqueryExpr;
use crate::sql::BindContext;

/// A helper for type checking.
///
/// `TypeChecker::resolve` will resolve types of `Expr` and transform `Expr` into
/// a typed expression `Scalar`. At the same time, name resolution will be performed,
/// which check validity of unbound `ColumnRef` and try to replace it with qualified
/// `BoundColumnRef`.
///
/// If failed, a `SemanticError` will be raised. This may caused by incompatible
/// argument types of expressions, or unresolvable columns.
pub struct TypeChecker<'a> {
    bind_context: &'a BindContext,
    ctx: Arc<QueryContext>,
}

impl<'a> TypeChecker<'a> {
    pub fn new(bind_context: &'a BindContext, ctx: Arc<QueryContext>) -> Self {
        Self { bind_context, ctx }
    }

    /// Resolve types of `expr` with given `required_type`.
    /// If `required_type` is None, then there is no requirement of return type.
    ///
    /// TODO(leiysky): choose correct overloads of functions with given required_type and arguments
    #[async_recursion::async_recursion]
    pub async fn resolve(
        &self,
        expr: &Expr<'a>,
        required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        match expr {
            Expr::ColumnRef {
                database: _,
                table,
                column,
                ..
            } => {
                let column = self
                    .bind_context
                    .resolve_column(table.clone().map(|ident| ident.name), column)?;
                let data_type = column.data_type.clone();

                Ok((BoundColumnRef { column }.into(), data_type))
            }

            Expr::IsNull { expr, not, .. } => {
                let func_name = if *not {
                    "is_not_null".to_string()
                } else {
                    "is_null".to_string()
                };

                self.resolve_function(func_name.as_str(), &[&**expr], required_type)
                    .await
            }

            Expr::InList {
                expr, list, not, ..
            } => {
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
                    .await
            }

            Expr::Between {
                expr,
                low,
                high,
                not,
                ..
            } => {
                if !*not {
                    // Rewrite `expr BETWEEN low AND high`
                    // into `expr >= low AND expr <= high`
                    let (ge_func, _) = self
                        .resolve_function(">=", &[&**expr, &**low], Some(BooleanType::new_impl()))
                        .await?;
                    let (le_func, _) = self
                        .resolve_function("<=", &[&**expr, &**high], Some(BooleanType::new_impl()))
                        .await?;
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
                    let (lt_func, _) = self
                        .resolve_function("<", &[&**expr, &**low], Some(BooleanType::new_impl()))
                        .await?;
                    let (gt_func, _) = self
                        .resolve_function(">", &[&**expr, &**high], Some(BooleanType::new_impl()))
                        .await?;
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

            Expr::BinaryOp {
                op, left, right, ..
            } => {
                self.resolve_binary_op(op, &**left, &**right, required_type)
                    .await
            }

            Expr::UnaryOp { op, expr, .. } => {
                self.resolve_unary_op(op, &**expr, required_type).await
            }

            Expr::Cast {
                expr, target_type, ..
            } => {
                let (scalar, data_type) = self.resolve(expr, required_type).await?;
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
                ..
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
                    .await
            }

            Expr::Literal { lit, .. } => {
                let value = self.parse_literal(lit, required_type)?;
                let data_type = value.data_type();
                Ok((ConstantExpr { value }.into(), data_type))
            }

            Expr::FunctionCall {
                distinct,
                name,
                args,
                params,
                ..
            } => {
                let args: Vec<&Expr> = args.iter().collect();
                let func_name = name.name.as_str();

                if AggregateFunctionFactory::instance().check(func_name) {
                    // Check aggregate function
                    let params = params
                        .iter()
                        .map(|literal| self.parse_literal(literal, None))
                        .collect::<Result<Vec<DataValue>>>()?;

                    let mut arguments = vec![];
                    for arg in args.iter() {
                        arguments.push(self.resolve(arg, None).await?);
                    }

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
                            func_name: func_name.to_string(),
                            distinct: *distinct,
                            params,
                            args: if optimize_remove_count_args(
                                func_name,
                                *distinct,
                                args.as_slice(),
                            ) {
                                vec![]
                            } else {
                                arguments.into_iter().map(|arg| arg.0).collect()
                            },
                            return_type: agg_func.return_type()?,
                        }
                        .into(),
                        agg_func.return_type()?,
                    ))
                } else {
                    // Scalar function
                    self.resolve_function(func_name, &args, required_type).await
                }
            }

            Expr::CountAll { .. } => {
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

            Expr::Subquery { subquery, .. } => self.resolve_subquery(subquery, false, None).await,

            _ => Err(ErrorCode::UnImplement(format!(
                "Unsupported expr: {:?}",
                expr
            ))),
        }
    }

    /// Resolve function call.
    pub async fn resolve_function(
        &self,
        func_name: &str,
        arguments: &[&Expr<'a>],
        _required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        let mut args = vec![];
        let mut arg_types = vec![];

        for argument in arguments {
            let (arg, arg_type) = self.resolve(argument, None).await?;
            args.push(arg);
            arg_types.push(arg_type);
        }

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
    pub async fn resolve_binary_op(
        &self,
        op: &BinaryOperator,
        left: &Expr<'a>,
        right: &Expr<'a>,
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
                    .await
            }
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::Gte
            | BinaryOperator::Lte
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                let op = ComparisonOp::try_from(op)?;
                let (left, _) = self.resolve(left, None).await?;
                let (right, _) = self.resolve(right, None).await?;

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
                let (left, _) = self.resolve(left, Some(BooleanType::new_impl())).await?;
                let (right, _) = self.resolve(right, Some(BooleanType::new_impl())).await?;

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
                let (left, _) = self.resolve(left, Some(BooleanType::new_impl())).await?;
                let (right, _) = self.resolve(right, Some(BooleanType::new_impl())).await?;

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
    pub async fn resolve_unary_op(
        &self,
        op: &UnaryOperator,
        child: &Expr<'a>,
        required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        self.resolve_function(op.to_string().as_str(), &[child], required_type)
            .await
    }

    pub async fn resolve_subquery(
        &self,
        subquery: &Query<'a>,
        allow_multi_rows: bool,
        _required_type: Option<DataTypeImpl>,
    ) -> Result<(Scalar, DataTypeImpl)> {
        let mut binder = Binder::new(self.ctx.clone(), self.ctx.get_catalogs());

        // Create new `BindContext` with current `bind_context` as its parent, so we can resolve outer columns.
        let bind_context = BindContext::with_parent(Box::new(self.bind_context.clone()));
        let output_context = binder.bind_query(subquery, &bind_context).await?;

        if output_context.columns.len() > 1 {
            return Err(ErrorCode::SemanticError(
                "Scalar subquery must return only one column",
            ));
        }

        let data_type = output_context.columns[0].data_type.clone();

        let subquery_expr = SubqueryExpr {
            subquery: output_context.expression.clone().unwrap(),
            data_type: data_type.clone(),
            output_context: Box::new(output_context),
            allow_multi_rows,
        };

        Ok((subquery_expr.into(), data_type))
    }

    /// Resolve literal values.
    pub fn parse_literal(
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
