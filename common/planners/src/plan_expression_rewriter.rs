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

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::Expression;
use crate::ExpressionVisitor;
use crate::PlanNode;
use crate::Recursion;

/// Trait for potentially recursively rewriting an [`Expr`] expression
/// tree. When passed to `Expr::rewrite`, `ExprVisitor::mutate` is
/// invoked recursively on all nodes of an expression tree. See the
/// comments on `Expr::rewrite` for details on its use
pub trait ExpressionRewriter: Sized {
    /// Invoked before any children of `expr` are rewritten /
    /// visited. Default implementation returns `Ok(true)`
    fn pre_visit(&mut self, _expr: &Expression) -> Result<bool> {
        Ok(true)
    }

    fn mutate_binary(
        &mut self,
        op: &str,
        left: Expression,
        right: Expression,
        _origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::BinaryExpression {
            op: op.to_string(),
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn mutate_unary_expression(
        &mut self,
        op: &str,
        expr: Expression,
        _origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::UnaryExpression {
            op: op.to_string(),
            expr: Box::new(expr),
        })
    }

    fn mutate_scalar_function(
        &mut self,
        name: &str,
        args: Vec<Expression>,
        _origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::ScalarFunction {
            op: name.to_string(),
            args,
        })
    }

    fn mutate_subquery(
        &mut self,
        name: &str,
        subquery: &Arc<PlanNode>,
        _origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::Subquery {
            name: name.to_string(),
            query_plan: subquery.clone(),
        })
    }

    fn mutate_scalar_subquery(
        &mut self,
        name: &str,
        subquery: &Arc<PlanNode>,
        _origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::ScalarSubquery {
            name: name.to_string(),
            query_plan: subquery.clone(),
        })
    }

    fn mutate_wildcard(&mut self, _origin_expr: &Expression) -> Result<Expression> {
        Ok(Expression::Wildcard)
    }

    fn mutate_column(
        &mut self,
        column_name: &str,
        _origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::Column(column_name.to_string()))
    }

    fn mutate_aggregate_function(
        &mut self,
        name: &str,
        distinct: bool,
        params: &[DataValue],
        args: Vec<Expression>,
        _origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::AggregateFunction {
            op: name.to_string(),
            distinct,
            args,
            params: params.to_owned(),
        })
    }

    fn mutate_cast(
        &mut self,
        typ: &DataTypePtr,
        expr: Expression,
        _origin_expr: &Expression,
        is_nullable: bool,
    ) -> Result<Expression> {
        Ok(Expression::Cast {
            expr: Box::new(expr),
            data_type: typ.clone(),
            is_nullable,
        })
    }

    fn mutate_alias(
        &mut self,
        alias: &str,
        expr: Expression,
        _origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::Alias(alias.to_string(), Box::new(expr)))
    }

    fn mutate_literal(
        &mut self,
        value: &DataValue,
        column_name: &Option<String>,
        data_type: &DataTypePtr,
        _origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::Literal {
            value: value.clone(),
            column_name: column_name.clone(),
            data_type: data_type.clone(),
        })
    }

    fn mutate_qualified_column(
        &mut self,
        names: &[String],
        _origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::QualifiedColumn(names.to_vec()))
    }

    fn mutate_sort(
        &mut self,
        expr: Expression,
        asc: bool,
        nulls_first: bool,
        origin_expr: &Expression,
    ) -> Result<Expression> {
        Ok(Expression::Sort {
            expr: Box::new(expr),
            asc,
            nulls_first,
            origin_expr: Box::new(origin_expr.clone()),
        })
    }

    /// Invoked after all children of `expr` have been mutated and
    /// returns a potentially modified expr.
    fn mutate(self, expr: &Expression) -> Result<Expression> {
        ExpressionRewriteVisitor::create(self)
            .visit(expr)?
            .finalize()
    }
}

struct ExpressionRewriteVisitor<T: ExpressionRewriter> {
    inner: T,
    stack: Vec<Expression>,
}

impl<T: ExpressionRewriter> ExpressionRewriteVisitor<T> {
    pub fn create(inner: T) -> Self {
        Self {
            inner,
            stack: vec![],
        }
    }

    pub fn finalize(mut self) -> Result<Expression> {
        match self.stack.len() {
            1 => Ok(self.stack.remove(0)),
            _ => Err(ErrorCode::LogicalError(
                "Stack has too many elements in ExpressionDataTypeVisitor::finalize",
            )),
        }
    }
}

impl<T: ExpressionRewriter> ExpressionVisitor for ExpressionRewriteVisitor<T> {
    fn pre_visit(mut self, expr: &Expression) -> Result<Recursion<Self>> {
        match self.inner.pre_visit(expr)? {
            true => Ok(Recursion::Continue(self)),
            false => Ok(Recursion::Stop(self)),
        }
    }

    fn post_visit(mut self, expr: &Expression) -> Result<Self> {
        match expr {
            Expression::BinaryExpression { op, .. } => match (self.stack.pop(), self.stack.pop()) {
                (Some(left), Some(right)) => {
                    self.stack
                        .push(self.inner.mutate_binary(op, left, right, expr)?);
                    Ok(self)
                }
                (_, _) => Err(ErrorCode::LogicalError("Binary expr expected 2 arguments.")),
            },
            Expression::UnaryExpression { op, .. } => match self.stack.pop() {
                None => Err(ErrorCode::LogicalError("Unary expr expected 1 arguments.")),
                Some(new_expr) => {
                    self.stack
                        .push(self.inner.mutate_unary_expression(op, new_expr, expr)?);
                    Ok(self)
                }
            },
            Expression::ScalarFunction { op, args } => {
                let mut args_expr = Vec::with_capacity(args.len());
                for index in 0..args.len() {
                    match self.stack.pop() {
                        None => {
                            return Err(ErrorCode::LogicalError(format!(
                                "Expected {} arguments, actual {}.",
                                args.len(),
                                index
                            )));
                        }
                        Some(arg_type) => args_expr.push(arg_type),
                    };
                }

                self.stack
                    .push(self.inner.mutate_scalar_function(op, args_expr, expr)?);
                Ok(self)
            }
            Expression::AggregateFunction {
                op,
                distinct,
                params,
                args,
            } => {
                let mut args_expr = Vec::with_capacity(args.len());

                for index in 0..args.len() {
                    match self.stack.pop() {
                        None => {
                            return Err(ErrorCode::LogicalError(format!(
                                "Expected {} arguments, actual {}.",
                                args.len(),
                                index
                            )));
                        }
                        Some(arg_type) => args_expr.push(arg_type),
                    };
                }

                let new_expr = self
                    .inner
                    .mutate_aggregate_function(op, *distinct, params, args_expr, expr)?;
                self.stack.push(new_expr);
                Ok(self)
            }
            Expression::Cast {
                data_type,
                is_nullable,
                ..
            } => match self.stack.pop() {
                None => Err(ErrorCode::LogicalError(
                    "Cast expr expected 1 parameters, actual 0.",
                )),
                Some(new_expr) => {
                    self.stack.push(self.inner.mutate_cast(
                        data_type,
                        new_expr,
                        expr,
                        *is_nullable,
                    )?);
                    Ok(self)
                }
            },
            Expression::Subquery { name, query_plan } => {
                self.stack
                    .push(self.inner.mutate_subquery(name, query_plan, expr)?);
                Ok(self)
            }
            Expression::ScalarSubquery { name, query_plan } => {
                self.stack
                    .push(self.inner.mutate_scalar_subquery(name, query_plan, expr)?);
                Ok(self)
            }
            Expression::Wildcard => {
                self.stack.push(self.inner.mutate_wildcard(expr)?);
                Ok(self)
            }
            Expression::Column(column_name) => {
                self.stack
                    .push(self.inner.mutate_column(column_name, expr)?);
                Ok(self)
            }
            Expression::Sort {
                asc,
                nulls_first,
                origin_expr,
                ..
            } => match self.stack.pop() {
                None => Err(ErrorCode::LogicalError(
                    "Sort expr expected 1 parameters, actual 0.",
                )),
                Some(expr) => {
                    let new_expr = self
                        .inner
                        .mutate_sort(expr, *asc, *nulls_first, origin_expr)?;
                    self.stack.push(new_expr);
                    Ok(self)
                }
            },
            Expression::Alias(alias, _) => match self.stack.pop() {
                None => Err(ErrorCode::LogicalError(
                    "Alias expr expected 1 parameters, actual 0.",
                )),
                Some(new_expr) => {
                    let new_expr = self.inner.mutate_alias(alias, new_expr, expr);
                    self.stack.push(new_expr?);
                    Ok(self)
                }
            },
            Expression::Literal {
                value,
                column_name,
                data_type,
            } => {
                let new_expr = self
                    .inner
                    .mutate_literal(value, column_name, data_type, expr)?;
                self.stack.push(new_expr);
                Ok(self)
            }
            Expression::QualifiedColumn(names) => {
                let new_expr = self.inner.mutate_qualified_column(names, expr)?;
                self.stack.push(new_expr);
                Ok(self)
            }
        }
    }
}
