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

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_functions::scalars::FunctionFactory;

use crate::ActionAlias;
use crate::ActionConstant;
use crate::ActionFunction;
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
            Expression::QualifiedColumn(_) => {
                return Err(ErrorCode::LogicalError(
                    "QualifiedColumn should be resolve in analyze.",
                ));
            }
            Expression::Literal {
                value, data_type, ..
            } => {
                let value = ActionConstant {
                    name: expr.column_name(),
                    value: value.clone(),
                    data_type: data_type.clone(),
                };

                self.actions.push(ExpressionAction::Constant(value));
            }
            Expression::Subquery { name, query_plan } => {
                // Subquery results are ready in the expression input
                self.actions.push(ExpressionAction::Input(ActionInput {
                    name: name.clone(),
                    return_type: Expression::to_subquery_type(query_plan).data_type().clone(),
                }));
            }
            Expression::ScalarSubquery { name, query_plan } => {
                // Scalar subquery results are ready in the expression input
                self.actions.push(ExpressionAction::Input(ActionInput {
                    name: name.to_string(),
                    return_type: Expression::to_subquery_type(query_plan).data_type().clone(),
                }));
            }
            Expression::UnaryExpression {
                op,
                expr: nested_expr,
            } => {
                self.add_expr(nested_expr)?;

                let arg_types = vec![nested_expr.to_data_type_and_nullable(&self.schema)?];
                let func = FunctionFactory::instance().get(op, &arg_types)?;
                let return_type = func.return_type(&arg_types)?;
                let is_nullable = func.nullable(&arg_types)?;

                let function = ActionFunction {
                    name: expr.column_name(),
                    func_name: op.clone(),
                    func,
                    arg_names: vec![nested_expr.column_name()],
                    arg_types,
                    is_nullable,
                    return_type,
                };

                self.actions.push(ExpressionAction::Function(function));
            }

            Expression::BinaryExpression { op, left, right } => {
                self.add_expr(left)?;
                self.add_expr(right)?;

                let arg_types = vec![
                    left.to_data_type_and_nullable(&self.schema)?,
                    right.to_data_type_and_nullable(&self.schema)?,
                ];

                let func = FunctionFactory::instance().get(op, &arg_types)?;
                let is_nullable = func.nullable(&arg_types)?;
                let return_type = func.return_type(&arg_types)?;

                let function = ActionFunction {
                    name: expr.column_name(),
                    func_name: op.clone(),
                    func,
                    arg_names: vec![left.column_name(), right.column_name()],
                    arg_types,
                    is_nullable,
                    return_type,
                };

                self.actions.push(ExpressionAction::Function(function));
            }

            Expression::ScalarFunction { op, args } => {
                for expr in args.iter() {
                    self.add_expr(expr)?;
                }

                let arg_types = args
                    .iter()
                    .map(|action| action.to_data_type_and_nullable(&self.schema))
                    .collect::<Result<Vec<_>>>()?;

                let func = FunctionFactory::instance().get(op, &arg_types)?;
                let is_nullable = func.nullable(&arg_types)?;
                let return_type = func.return_type(&arg_types)?;

                let function = ActionFunction {
                    name: expr.column_name(),
                    func_name: op.clone(),
                    func,
                    arg_names: args.iter().map(|action| action.column_name()).collect(),
                    arg_types,
                    is_nullable,
                    return_type,
                };

                self.actions.push(ExpressionAction::Function(function));
            }

            Expression::AggregateFunction { .. } => {
                return Err(ErrorCode::LogicalError(
                    "Action must be a non-aggregated function.",
                ));
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
                let func_name = "cast".to_string();
                let return_type = data_type.clone();
                let func = CastFunction::create(func_name.clone(), return_type.clone())?;
                let function = ActionFunction {
                    name: expr.column_name(),
                    func_name,
                    func,
                    arg_names: vec![sub_expr.column_name()],
                    arg_types: vec![sub_expr.to_data_type_and_nullable(&self.schema)?],
                    is_nullable: false,
                    return_type,
                };

                self.actions.push(ExpressionAction::Function(function));
            }
        }
        Ok(())
    }
}
