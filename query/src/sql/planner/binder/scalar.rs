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

use std::any::Any;
use std::sync::Arc;

use common_ast::ast::BinaryOperator;
use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_datavalues::DataField;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;

use crate::sql::planner::binder::BindContext;
use crate::sql::planner::metadata::optimize_remove_count_args;
use crate::sql::plans::Scalar;

/// Helper for binding scalar expression with `BindContext`.
pub struct ScalarBinder;

impl ScalarBinder {
    pub fn new() -> Self {
        ScalarBinder {}
    }

    pub fn bind_expr(&self, expr: &Expr, bind_context: &BindContext) -> Result<ScalarExprRef> {
        match expr {
            Expr::ColumnRef { table, column, .. } => {
                let table_name: Option<String> = table.clone().map(|ident| ident.name);
                let column_name = column.name.clone();
                let column_binding = bind_context.resolve_column(table_name, column_name)?;

                Ok(Arc::new(Scalar::ColumnRef {
                    index: column_binding.index,
                    data_type: column_binding.data_type.clone(),
                    nullable: column_binding.nullable,
                }))
            }
            Expr::Literal(literal) => match literal {
                Literal::Number(val) => Ok(Arc::new(Scalar::Literal {
                    data_value: DataValue::try_from_literal(val, None)?,
                })),
                Literal::String(val) => Ok(Arc::new(Scalar::Literal {
                    data_value: DataValue::String(val.clone().into_bytes()),
                })),
                Literal::Boolean(val) => Ok(Arc::new(Scalar::Literal {
                    data_value: DataValue::Boolean(*val),
                })),
                Literal::Null => Ok(Arc::new(Scalar::Literal {
                    data_value: DataValue::Null,
                })),
                _ => Err(ErrorCode::UnImplement(format!(
                    "Unsupported Literal: {literal}"
                ))),
            },
            Expr::BinaryOp { op, left, right } => {
                self.bind_binary_op(op, left.as_ref(), right.as_ref(), bind_context)
            }
            Expr::CountAll => {
                let name = Identifier {
                    name: "count".to_string(),
                    quote: None,
                };
                self.bind_aggregate_op(false, &name, &[], &[], bind_context)
            }
            Expr::FunctionCall {
                distinct,
                name,
                args,
                params,
            } => {
                match AggregateFunctionFactory::instance().check(&name.name) {
                    true => {
                        // Function is aggregate function
                        self.bind_aggregate_op(*distinct, name, args, params, bind_context)
                    }
                    false => Err(ErrorCode::UnImplement(format!(
                        "Unsupported function: {name}"
                    ))),
                }
            }
            _ => Err(ErrorCode::UnImplement(format!(
                "Unsupported expr: {:?}",
                expr
            ))),
        }
    }

    fn bind_binary_op(
        &self,
        op: &BinaryOperator,
        left_child: &Expr,
        right_child: &Expr,
        bind_context: &BindContext,
    ) -> Result<ScalarExprRef> {
        let left_scalar = self.bind_expr(left_child, bind_context)?;
        let right_scalar = self.bind_expr(right_child, bind_context)?;
        match op {
            BinaryOperator::Eq => Ok(Arc::new(Scalar::Equal {
                left: Box::from(
                    left_scalar
                        .as_any()
                        .downcast_ref::<Scalar>()
                        .ok_or_else(|| ErrorCode::UnImplement("Can't downcast to Scalar"))?
                        .clone(),
                ),
                right: Box::from(
                    right_scalar
                        .as_any()
                        .downcast_ref::<Scalar>()
                        .ok_or_else(|| ErrorCode::UnImplement("Can't downcast to Scalar"))?
                        .clone(),
                ),
            })),
            _ => Err(ErrorCode::UnImplement(format!(
                "Unsupported binary operator: {op}",
            ))),
        }
    }

    fn bind_aggregate_op(
        &self,
        distinct: bool,
        func_name: &Identifier,
        args: &[Expr],
        params: &[Literal],
        bind_context: &BindContext,
    ) -> Result<ScalarExprRef> {
        let mut data_values = Vec::with_capacity(params.len());
        for param in params.iter() {
            data_values.push(match param {
                Literal::Number(val) => DataValue::try_from_literal(val, None)?,
                Literal::String(val) => DataValue::String(val.clone().into_bytes()),
                Literal::Boolean(val) => DataValue::Boolean(*val),
                Literal::Null => DataValue::Null,
                Literal::Interval(_) => unimplemented!(),
                Literal::CurrentTimestamp => unimplemented!(),
            })
        }

        let scalar_binder = ScalarBinder::new();
        let mut scalar_exprs = Vec::with_capacity(args.len());
        let mut arg_col_name = Vec::with_capacity(args.len());
        for arg in args.iter() {
            if let Expr::ColumnRef { column, .. } = arg {
                arg_col_name.push(column.clone().name);
            }
            scalar_exprs.push(
                scalar_binder
                    .bind_expr(arg, bind_context)?
                    .as_any()
                    .downcast_ref::<Scalar>()
                    .ok_or_else(|| ErrorCode::UnImplement("Can't downcast to Scalar"))?
                    .clone(),
            );
        }

        let col_bindings = bind_context.all_column_bindings();

        let mut fields = Vec::with_capacity(col_bindings.len());
        for col_binding in col_bindings.iter() {
            if !arg_col_name.contains(&col_binding.column_name) {
                continue;
            }
            fields.push(DataField::new(
                col_binding.column_name.as_str(),
                col_binding.data_type.clone(),
            ))
        }

        let agg_func_ref = AggregateFunctionFactory::instance().get(
            func_name.name.clone(),
            data_values.clone(),
            fields,
        )?;
        Ok(Arc::new(Scalar::AggregateFunction {
            func_name: func_name.name.clone(),
            distinct,
            params: data_values,
            args: if optimize_remove_count_args(func_name.name.as_str(), distinct, args) {
                vec![]
            } else {
                scalar_exprs
            },
            data_type: agg_func_ref.return_type()?,
            nullable: false,
        }))
    }
}

pub type ScalarExprRef = Arc<dyn ScalarExpr>;

pub trait ScalarExpr: Any {
    /// Get return type and nullability
    fn data_type(&self) -> (DataTypeImpl, bool);

    // TODO: implement this in the future
    // fn used_columns(&self) -> ColumnSet;

    // TODO: implement this in the future
    // fn outer_columns(&self) -> ColumnSet;

    fn contains_aggregate(&self) -> bool;

    fn contains_subquery(&self) -> bool;

    fn as_any(&self) -> &dyn Any;
}
