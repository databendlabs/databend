// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCodes;
use common_exception::Result;
use common_functions::FunctionFactory;
use common_functions::IFunction;

use crate::Expression;
use crate::ExpressionVisitor;
use crate::Recursion;

// Visitor the expressions to do some validator
struct ExpressionValidator<'a, F>
where F: Fn(&Expression) -> Result<()>
{
    error: Option<ErrorCodes>,
    test_fn: &'a F
}

impl<'a, F> ExpressionValidator<'a, F>
where F: Fn(&Expression) -> Result<()>
{
    /// Create a new finder with the `test_fn`
    fn new(test_fn: &'a F) -> Self {
        Self {
            error: None,
            test_fn
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
                test_fn: self.test_fn
            }))
        }
    }
}

fn validate_function_arg(func: Box<dyn IFunction>, args: &[Expression]) -> Result<()> {
    match func.variadic_arguments() {
        Some((start, end)) => {
            return if args.len() < start || args.len() > end {
                Err(ErrorCodes::NumberArgumentsNotMatch(format!(
                    "Expect to have [{}, {}) arguments, but got {}",
                    start,
                    end,
                    args.len()
                )))
            } else {
                Ok(())
            };
        }
        None => {
            let num = func.num_arguments();
            return if num != args.len() {
                Err(ErrorCodes::NumberArgumentsNotMatch(format!(
                    "Expect to have {} arguments, but got {}",
                    num,
                    args.len()
                )))
            } else {
                Ok(())
            };
        }
    }
}

pub fn validate_expression(expr: &Expression) -> Result<()> {
    let validator = ExpressionValidator::new(&|expr: &Expression| match expr {
        Expression::UnaryExpression { op, expr } => {
            let func = FunctionFactory::get(op)?;
            let args = vec![*expr.clone()];
            validate_function_arg(func, &args)
        }
        Expression::BinaryExpression { left, op, right } => {
            let func = FunctionFactory::get(op)?;
            let args = vec![*left.clone(), *right.clone()];
            validate_function_arg(func, &args)
        }
        Expression::ScalarFunction { op, args } => {
            let func = FunctionFactory::get(op)?;
            validate_function_arg(func, args)
        }

        // todo: AggregateFunction validation after generic AggregateFunctions
        _ => Ok(())
    });

    let validator = expr.accept(validator)?;
    match validator.error {
        Some(err) => Err(err),
        None => Ok(())
    }
}
