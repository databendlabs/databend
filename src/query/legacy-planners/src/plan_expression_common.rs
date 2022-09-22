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

use std::collections::HashSet;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;

use crate::validate_function_arg;
use crate::Expression;
use crate::ExpressionVisitor;
use crate::Recursion;

pub struct ExpressionDataTypeVisitor {
    stack: Vec<DataTypeImpl>,
    input_schema: DataSchemaRef,
}

impl ExpressionDataTypeVisitor {
    pub fn create(input_schema: DataSchemaRef) -> ExpressionDataTypeVisitor {
        ExpressionDataTypeVisitor {
            input_schema,
            stack: vec![],
        }
    }

    pub fn finalize(mut self) -> Result<DataTypeImpl> {
        match self.stack.len() {
            1 => Ok(self.stack.remove(0)),
            _ => Err(ErrorCode::LogicalError(
                "Stack has too many elements in ExpressionDataTypeVisitor::finalize",
            )),
        }
    }

    fn visit_function(mut self, op: &str, args_size: usize) -> Result<ExpressionDataTypeVisitor> {
        let features = FunctionFactory::instance().get_features(op)?;
        validate_function_arg(
            op,
            args_size,
            features.variadic_arguments,
            features.num_arguments,
        )?;

        let mut arguments = Vec::with_capacity(args_size);
        for index in 0..args_size {
            arguments.push(match self.stack.pop() {
                None => Err(ErrorCode::LogicalError(format!(
                    "Expected {} arguments, actual {}.",
                    args_size, index
                ))),
                Some(element) => Ok(element),
            }?);
        }

        let arguments: Vec<&DataTypeImpl> = arguments.iter().collect();

        let function = FunctionFactory::instance().get(op, &arguments)?;
        let return_type = function.return_type();
        self.stack.push(return_type);
        Ok(self)
    }
}

impl ExpressionVisitor for ExpressionDataTypeVisitor {
    fn pre_visit(self, _expr: &Expression) -> Result<Recursion<Self>> {
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expression) -> Result<Self> {
        match expr {
            Expression::Column(s) => {
                let field = self.input_schema.field_with_name(s)?;
                self.stack.push(field.data_type().clone());
                Ok(self)
            }
            Expression::Wildcard => Result::Err(ErrorCode::IllegalDataType(
                "Wildcard expressions are not valid to get return type",
            )),
            Expression::QualifiedColumn(_) => Err(ErrorCode::LogicalError(
                "QualifiedColumn should be resolve in analyze.",
            )),
            Expression::Literal { data_type, .. } => {
                self.stack.push(data_type.clone());
                Ok(self)
            }
            Expression::BinaryExpression { op, .. } => self.visit_function(op, 2),
            Expression::UnaryExpression { op, .. } => self.visit_function(op, 1),
            Expression::ScalarFunction { op, args } => self.visit_function(op, args.len()),
            expr @ Expression::AggregateFunction { args, .. } => {
                // Pop arguments.
                for index in 0..args.len() {
                    if self.stack.pop().is_none() {
                        return Err(ErrorCode::LogicalError(format!(
                            "Expected {} arguments, actual {}.",
                            args.len(),
                            index
                        )));
                    }
                }

                let aggregate_function = expr.to_aggregate_function(&self.input_schema)?;
                let return_type = aggregate_function.return_type()?;

                self.stack.push(return_type);
                Ok(self)
            }

            Expression::Cast { data_type, .. } => {
                let inner_type = match self.stack.pop() {
                    None => Err(ErrorCode::LogicalError(
                        "Cast expr expected 1 arguments, actual 0.",
                    )),
                    Some(_) => Ok(data_type),
                }?;

                self.stack.push(inner_type.clone());
                Ok(self)
            }
            Expression::MapAccess { args, .. } => self.visit_function("get", args.len()),
            Expression::Alias(_, _) | Expression::Sort { .. } => Ok(self),
        }
    }
}

// This visitor is for recursively visiting expression tree and collects all columns.
pub struct RequireColumnsVisitor {
    pub required_columns: HashSet<String>,
}

impl RequireColumnsVisitor {
    pub fn default() -> Self {
        Self {
            required_columns: HashSet::new(),
        }
    }

    pub fn collect_columns_from_expr(expr: &Expression) -> Result<HashSet<String>> {
        let mut visitor = Self::default();
        visitor = expr.accept(visitor)?;
        Ok(visitor.required_columns)
    }
}

impl ExpressionVisitor for RequireColumnsVisitor {
    fn pre_visit(self, expr: &Expression) -> Result<Recursion<Self>> {
        match expr {
            Expression::Column(c) => {
                let mut v = self;
                v.required_columns.insert(c.clone());
                Ok(Recursion::Continue(v))
            }
            _ => Ok(Recursion::Continue(self)),
        }
    }
}
