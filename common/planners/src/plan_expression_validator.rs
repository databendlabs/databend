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
use common_functions::scalars::FunctionFactory;

use crate::Expression;
use crate::ExpressionVisitor;
use crate::Recursion;

// Visitor the expressions to do some validator
struct ExpressionValidator<'a, F>
where F: Fn(&Expression) -> Result<()>
{
    error: Option<ErrorCode>,
    test_fn: &'a F,
}

impl<'a, F> ExpressionValidator<'a, F>
where F: Fn(&Expression) -> Result<()>
{
    /// Create a new finder with the `test_fn`
    fn new(test_fn: &'a F) -> Self {
        Self {
            error: None,
            test_fn,
        }
    }
}

impl<'a, F> ExpressionVisitor for ExpressionValidator<'a, F>
where F: Fn(&Expression) -> Result<()>
{
    fn pre_visit(self, expr: &Expression) -> Result<Recursion<Self>> {
        match (self.test_fn)(expr) {
            Ok(()) => Ok(Recursion::Continue(self)),
            Err(e) => Ok(Recursion::Stop(ExpressionValidator {
                error: Some(e),
                test_fn: self.test_fn,
            })),
        }
    }
}

pub fn validate_function_arg(
    name: &str,
    args_len: usize,
    variadic_arguments: Option<(usize, usize)>,
    num_arguments: usize,
) -> Result<()> {
    match variadic_arguments {
        Some((start, end)) => {
            return if args_len < start || args_len > end {
                Err(ErrorCode::NumberArgumentsNotMatch(format!(
                    "Function `{}` expect to have [{}, {}] arguments, but got {}",
                    name, start, end, args_len
                )))
            } else {
                Ok(())
            };
        }
        None => {
            return if num_arguments != args_len {
                Err(ErrorCode::NumberArgumentsNotMatch(format!(
                    "Function `{}` expect to have {} arguments, but got {}",
                    name, num_arguments, args_len
                )))
            } else {
                Ok(())
            };
        }
    }
}

// Can works before expression,filter,having in PlanBuilder
pub fn validate_expression(expr: &Expression, schema: &DataSchemaRef) -> Result<()> {
    let _ = expr.to_data_field(schema)?;
    let validator = ExpressionValidator::new(&|expr: &Expression| match expr {
        Expression::ScalarFunction { op, args } => {
            let features = FunctionFactory::instance().get_features(op)?;
            validate_function_arg(
                op,
                args.len(),
                features.variadic_arguments,
                features.num_arguments,
            )
        }

        // Currently no need to check  UnaryExpression and BinaryExpression
        // todo: AggregateFunction validation after generic AggregateFunctions
        _ => Ok(()),
    });

    let validator = expr.accept(validator)?;
    match validator.error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn check_deterministic(op: &str) -> Result<()> {
    let features = FunctionFactory::instance().get_features(op)?;
    if !features.is_deterministic {
        return Err(ErrorCode::InvalidClusterKeys(format!(
            "Function `{}` is not valid for clustering",
            op
        )));
    }
    Ok(())
}

pub fn validate_clustering(expr: &Expression) -> Result<()> {
    let validator = ExpressionValidator::new(&|expr: &Expression| match expr {
        Expression::Literal { .. } => Ok(()),
        Expression::Column { .. } => Ok(()),
        Expression::Cast { .. } => Ok(()),
        Expression::UnaryExpression { op, .. } => check_deterministic(op),
        Expression::BinaryExpression { op, .. } => check_deterministic(op),
        Expression::ScalarFunction { op, .. } => check_deterministic(op),
        _ => Err(ErrorCode::InvalidClusterKeys(format!(
            "Function `{}` is not valid for clustering",
            expr.column_name()
        ))),
    });

    let validator = expr.accept(validator)?;
    match validator.error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}
