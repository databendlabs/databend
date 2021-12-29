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

use common_datavalues::columns::DataColumn;
<<<<<<< HEAD
use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_datavalues::DataValueArithmeticOperator;
=======
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataType;
>>>>>>> 09b313bbb (Refactor arithmetics negate function)
use common_exception::Result;

use super::arithmetic::ArithmeticTrait;
use crate::scalars::ArithmeticDivFunction;
use crate::scalars::ArithmeticIntDivFunction;
use crate::scalars::ArithmeticMinusFunction;
use crate::scalars::ArithmeticModuloFunction;
use crate::scalars::ArithmeticMulFunction;
use crate::scalars::ArithmeticPlusFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;

#[derive(Clone, Debug)]
pub enum BinaryArithmeticOperator {
    Plus,
    Minus,
    Mul,
    Div,
    IntDiv,
    Modulo,
}

impl std::fmt::Display for BinaryArithmeticOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let display = match &self {
            BinaryArithmeticOperator::Plus => "plus",
            BinaryArithmeticOperator::Minus => "minus",
            BinaryArithmeticOperator::Mul => "multiply",
            BinaryArithmeticOperator::Div => "divide",
            BinaryArithmeticOperator::IntDiv => "div",
            BinaryArithmeticOperator::Modulo => "modulo",
        };
        write!(f, "{}", display)
    }
}

#[derive(Clone)]
pub struct BinaryArithmeticFunction<T> {
    op: BinaryArithmeticOperator,
    result_type: DataType,
    t: PhantomData<T>,
}

impl<T> BinaryArithmeticFunction<T>
where T: ArithmeticTrait + Clone + Sync + Send + 'static
{
    pub fn try_create_func(
        op: BinaryArithmeticOperator,
        result_type: DataType,
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            op,
            result_type,
            t: PhantomData,
        }))
    }
}

impl<T> Function for BinaryArithmeticFunction<T>
where T: ArithmeticTrait + Clone + Sync + Send + 'static
{
    fn name(&self) -> &str {
        "BinaryArithmeticFunction"
    }

    fn return_type(&self, _args: &[DataTypeAndNullable]) -> Result<DataType> {
        Ok(self.result_type.clone())
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let result = T::arithmetic(columns)?;
        if result.data_type() != self.result_type {
            result.cast_with_type(&self.result_type)
        } else {
            Ok(result)
        }
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        match self.op {
            BinaryArithmeticOperator::Plus => ArithmeticPlusFunction::get_monotonicity(args),
            BinaryArithmeticOperator::Minus => ArithmeticMinusFunction::get_monotonicity(args),
            BinaryArithmeticOperator::Mul => ArithmeticMulFunction::get_monotonicity(args),
            BinaryArithmeticOperator::Div => ArithmeticDivFunction::get_monotonicity(args),
            BinaryArithmeticOperator::IntDiv => ArithmeticIntDivFunction::get_monotonicity(args),
            BinaryArithmeticOperator::Modulo => ArithmeticModuloFunction::get_monotonicity(args),
        }
    }
}

impl<T> fmt::Display for BinaryArithmeticFunction<T>
where T: ArithmeticTrait + Clone + Sync + Send + 'static
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}
