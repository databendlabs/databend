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

use std::fmt;
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;

use super::arithmetic::ArithmeticTrait;
use crate::scalars::ArithmeticNegateFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;

#[derive(Clone)]
pub struct UnaryArithmeticFunction<T> {
    op: DataValueUnaryOperator,
    result_type: DataType,
    t: PhantomData<T>,
}

impl<T> UnaryArithmeticFunction<T>
where T: ArithmeticTrait + Clone + Sync + Send + 'static
{
    pub fn try_create_func(
        op: DataValueUnaryOperator,
        result_type: DataType,
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            op,
            result_type,
            t: PhantomData,
        }))
    }
}

impl<T> Function for UnaryArithmeticFunction<T>
where T: ArithmeticTrait + Clone + Sync + Send + 'static
{
    fn name(&self) -> &str {
        "UnaryArithmeticFunction"
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        let nullable = args.iter().any(|arg| arg.is_nullable());
        Ok(DataTypeAndNullable::create(&self.result_type, nullable))
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        T::arithmetic(columns)
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        if args.len() != 1 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        match self.op {
            DataValueUnaryOperator::Negate => ArithmeticNegateFunction::get_monotonicity(args),
        }
    }
}

impl<T> fmt::Display for UnaryArithmeticFunction<T>
where T: ArithmeticTrait + Clone + Sync + Send + 'static
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
