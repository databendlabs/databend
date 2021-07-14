// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::is_numeric;
use common_datavalues::numerical_coercion;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;

use crate::ActionAlias;
use crate::ActionConstant;
use crate::ActionExists;
use crate::ActionFunction;
use crate::ActionInList;
use crate::ActionInput;
use crate::Expression;
use crate::ExpressionAction;

#[derive(Debug, Clone)]
pub struct ExpressionChain {
    // input schema
    pub schema: DataSchemaRef,
    pub actions: Vec<ExpressionAction>,
}

impl ExpressionChain {
    pub fn try_create(schema: DataSchemaRef, exprs: &[Expression]) -> Result<Self> {
        let mut chain = Self {
            schema,
            actions: vec![],
        };

        for expr in exprs {
            chain.add_expr(expr)?;
        }

        Ok(chain)
    }

    fn add_expr(&mut self, expr: &Expression) -> Result<()> {
        match expr {
            Expression::Alias(name, sub_expr) => {
                self.add_expr(sub_expr)?;
                let return_type = expr.to_data_type(&self.schema)?;

                let alias = ActionAlias {
                    name: name.clone(),
                    arg_name: sub_expr.column_name(),
                    arg_type: return_type,
                };

                self.actions.push(ExpressionAction::Alias(alias));
            }
            Expression::Column(c) => {
                let arg_type = self.schema.field_with_name(c)?.data_type();
                let input = ActionInput {
                    name: expr.column_name(),
                    return_type: arg_type.clone(),
                };
                self.actions.push(ExpressionAction::Input(input));
            }
            Expression::Literal(l) => {
                let value = ActionConstant {
                    name: expr.column_name(),
                    value: l.clone(),
                };

                self.actions.push(ExpressionAction::Constant(value));
            }
            Expression::InList(inlist_expr) => {
                self.add_expr(inlist_expr.expr())?;
                let mut val_list = Vec::new();
                let mut final_dtype = inlist_expr.expr().to_data_type(&self.schema)?;

                // Use two passes to transform the data type here
                for e in inlist_expr.list() {
                    let tmp_dtype = e.to_data_type(&self.schema)?;
                    if is_numeric(&tmp_dtype) && is_numeric(&final_dtype) {
                        final_dtype = numerical_coercion(&final_dtype, &tmp_dtype)?;
                    } else if tmp_dtype != final_dtype {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "InList has invalid datatype: {} for {:?}",
                            tmp_dtype, e
                        )));
                    }
                }

                for e in inlist_expr.list() {
                    if let Expression::Literal(l) = e {
                        let array = l.to_series_with_size(1)?;
                        let array = array.cast_with_type(&final_dtype)?;
                        let value = array.try_get(0)?;
                        val_list.push(value.clone());
                    }
                }

                let v = ActionInList {
                    // name for the whole exprs
                    name: expr.column_name(),
                    // name for the input column
                    expr_name: inlist_expr.expr().column_name(),
                    // variable passed to InListFunction
                    list: val_list,
                    negated: inlist_expr.negated(),
                    data_type: final_dtype,
                };
                self.actions.push(ExpressionAction::InList(v));
            }
            Expression::Exists(_p) => {
                let value = ActionExists {
                    name: format!("{:?}", expr),
                };
                self.actions.push(ExpressionAction::Exists(value));
            }
            Expression::UnaryExpression {
                op,
                expr: nested_expr,
            } => {
                self.add_expr(nested_expr)?;

                let func = FunctionFactory::get(op)?;
                let arg_types = vec![nested_expr.to_data_type(&self.schema)?];

                let function = ActionFunction {
                    name: expr.column_name(),
                    func_name: op.clone(),
                    is_aggregated: false,
                    arg_names: vec![nested_expr.column_name()],
                    arg_types: arg_types.clone(),
                    arg_fields: vec![],
                    return_type: func.return_type(&arg_types)?,
                };

                self.actions.push(ExpressionAction::Function(function));
            }

            Expression::BinaryExpression { op, left, right } => {
                self.add_expr(left)?;
                self.add_expr(right)?;

                let func = FunctionFactory::get(op)?;
                let arg_types = vec![
                    left.to_data_type(&self.schema)?,
                    right.to_data_type(&self.schema)?,
                ];

                let function = ActionFunction {
                    name: expr.column_name(),
                    func_name: op.clone(),
                    is_aggregated: false,
                    arg_names: vec![left.column_name(), right.column_name()],
                    arg_types: arg_types.clone(),
                    arg_fields: vec![],
                    return_type: func.return_type(&arg_types)?,
                };

                self.actions.push(ExpressionAction::Function(function));
            }

            Expression::ScalarFunction { op, args } => {
                for expr in args.iter() {
                    self.add_expr(expr)?;
                }

                let func = FunctionFactory::get(op)?;
                let arg_types = args
                    .iter()
                    .map(|action| action.to_data_type(&self.schema))
                    .collect::<Result<Vec<_>>>()?;

                let function = ActionFunction {
                    name: expr.column_name(),
                    func_name: op.clone(),
                    is_aggregated: false,
                    arg_names: args.iter().map(|action| action.column_name()).collect(),
                    arg_types: arg_types.clone(),
                    arg_fields: vec![],
                    return_type: func.return_type(&arg_types)?,
                };

                self.actions.push(ExpressionAction::Function(function));
            }

            Expression::AggregateFunction { op, args, .. } => {
                let mut arg_fields = Vec::with_capacity(args.len());
                for arg in args.iter() {
                    arg_fields.push(arg.to_data_field(&self.schema)?);
                }

                let func = expr.to_aggregate_function(&self.schema)?;
                let function = ActionFunction {
                    name: expr.column_name(),
                    func_name: op.clone(),
                    is_aggregated: true,
                    arg_types: vec![],
                    arg_names: vec![],
                    arg_fields,
                    return_type: func.return_type()?,
                };

                self.actions.push(ExpressionAction::Function(function));
            }
            Expression::Sort { expr, .. } => {
                self.add_expr(expr)?;
            }

            Expression::Wildcard => {}
            Expression::Cast {
                expr: sub_expr,
                data_type,
            } => {
                self.add_expr(sub_expr)?;
                let function = ActionFunction {
                    name: expr.column_name(),
                    func_name: "cast".to_string(),
                    is_aggregated: false,
                    arg_names: vec![sub_expr.column_name()],
                    arg_types: vec![sub_expr.to_data_type(&self.schema)?],
                    arg_fields: vec![],
                    return_type: data_type.clone(),
                };

                self.actions.push(ExpressionAction::Function(function));
            }
        }
        Ok(())
    }
}
