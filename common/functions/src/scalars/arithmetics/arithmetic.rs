// Copyright 2020 Datafuse Labs.
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

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::Result;

use crate::scalars::dates::IntervalFunctionFactory;
use crate::scalars::function::Monotonicity;
use crate::scalars::function_factory::FunctionFactory;
use crate::scalars::ArithmeticDivFunction;
use crate::scalars::ArithmeticMinusFunction;
use crate::scalars::ArithmeticModuloFunction;
use crate::scalars::ArithmeticMulFunction;
use crate::scalars::ArithmeticPlusFunction;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ArithmeticFunction {
    op: DataValueArithmeticOperator,
}

impl ArithmeticFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register("+", ArithmeticPlusFunction::desc());
        factory.register("plus", ArithmeticPlusFunction::desc());
        factory.register("-", ArithmeticMinusFunction::desc());
        factory.register("minus", ArithmeticMinusFunction::desc());
        factory.register("*", ArithmeticMulFunction::desc());
        factory.register("multiply", ArithmeticMulFunction::desc());
        factory.register("/", ArithmeticDivFunction::desc());
        factory.register("divide", ArithmeticDivFunction::desc());
        factory.register("%", ArithmeticModuloFunction::desc());
        factory.register("modulo", ArithmeticModuloFunction::desc());
    }

    pub fn try_create_func(op: DataValueArithmeticOperator) -> Result<Box<dyn Function>> {
        Ok(Box::new(ArithmeticFunction::new(op)))
    }

    pub fn new(op: DataValueArithmeticOperator) -> Self {
        ArithmeticFunction { op }
    }
}

impl Function for ArithmeticFunction {
    fn name(&self) -> &str {
        "ArithmeticFunction"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args.len() == 1 {
            return numerical_unary_arithmetic_coercion(&self.op, &args[0]);
        }

        if is_interval(&args[0]) || is_interval(&args[1]) {
            return interval_arithmetic_coercion(&self.op, &args[0], &args[1]);
        }
        if is_date_or_date_time(&args[0]) || is_date_or_date_time(&args[1]) {
            return datetime_arithmetic_coercion(&self.op, &args[0], &args[1]);
        }
        numerical_arithmetic_coercion(&self.op, &args[0], &args[1])
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let result: DataColumn = {
            // Some logic type need DateType information, try arithmetic on column with field first.
            if let Some(f) = IntervalFunctionFactory::try_get_arithmetic_func(columns) {
                f(&self.op, &columns[0], &columns[1])?
            } else {
                match columns.len() {
                    1 => columns[0].column().unary_arithmetic(self.op.clone()),
                    _ => columns[0]
                        .column()
                        .arithmetic(self.op.clone(), columns[1].column()),
                }?
            }
        };

        let has_date_or_date_time = columns.iter().any(|c| is_date_or_date_time(c.data_type()));

        if has_date_or_date_time {
            let args = columns
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>();
            let data_type = self.return_type(&args)?;
            result.cast_with_type(&data_type)
        } else {
            Ok(result)
        }
    }

    fn num_arguments(&self) -> usize {
        0
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((1, 2))
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        match self.op {
            Plus => ArithmeticPlusFunction::get_monotonicity(args),
            Minus => ArithmeticMinusFunction::get_monotonicity(args),
            Mul => ArithmeticMulFunction::get_monotonicity(args),
            Div => ArithmeticDivFunction::get_monotonicity(args),
            Modulo => ArithmeticModuloFunction::get_monotonicity(args),
        }
    }
}

impl fmt::Display for ArithmeticFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
