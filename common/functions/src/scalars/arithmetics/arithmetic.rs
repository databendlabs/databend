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
use common_datavalues::prelude::*;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFactory;
/*use crate::scalars::ArithmeticDivFunction;
use crate::scalars::ArithmeticIntDivFunction;
use crate::scalars::ArithmeticMinusFunction;
use crate::scalars::ArithmeticModuloFunction;
use crate::scalars::ArithmeticMulFunction;*/
use crate::scalars::ArithmeticPlusFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;

pub trait ArithmeticTrait {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn>;
}

#[derive(Clone)]
pub struct ArithmeticFunction {}

impl ArithmeticFunction {
    pub fn register(factory: &mut FunctionFactory) {
        factory.register_arithmetic("+", ArithmeticPlusFunction::desc());
        factory.register_arithmetic("plus", ArithmeticPlusFunction::desc());
        /*factory.register_arithmetic("-", ArithmeticMinusFunction::desc());
        factory.register_arithmetic("minus", ArithmeticMinusFunction::desc());
        factory.register_arithmetic("*", ArithmeticMulFunction::desc());
        factory.register_arithmetic("multiply", ArithmeticMulFunction::desc());
        factory.register_arithmetic("/", ArithmeticDivFunction::desc());
        factory.register_arithmetic("divide", ArithmeticDivFunction::desc());
        factory.register_arithmetic("%", ArithmeticModuloFunction::desc());
        factory.register_arithmetic("modulo", ArithmeticModuloFunction::desc());
        factory.register_arithmetic("div", ArithmeticIntDivFunction::desc());*/
    }
}

#[derive(Clone)]
pub struct BinaryArithmeticFunction<T> {
    op: DataValueArithmeticOperator,
    result_type: DataType,
    t: PhantomData<T>,
}

impl<T> BinaryArithmeticFunction<T>
where T: ArithmeticTrait + Clone + Sync + Send + 'static
{
    pub fn try_create_func(
        op: DataValueArithmeticOperator,
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

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(self.result_type.clone())
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        /*
        match columns.len() {
            1 => columns[0].column().unary_arithmetic(self.op.clone()),
            2 => {
                let result: DataColumn = {
                    // Some logic type need DateType information, try arithmetic on column with field first.
                    if let Some(f) = IntervalFunctionFactory::try_get_arithmetic_func(columns)? {
                        f(&self.op, &columns[0], &columns[1])?
                    } else {
                        columns[0]
                                .column()
                                .arithmetic(self.op.clone(), columns[1].column())?,
                    }
                };

                let has_date_or_date_time =
                    columns.iter().any(|c| c.data_type().is_date_or_date_time());

                if has_date_or_date_time {
                    let data_type = datetime_arithmetic_coercion(
                        &self.op,
                        columns[0].data_type(),
                        columns[1].data_type(),
                    )?;
                    result.cast_with_type(&data_type)
                } else {
                    Ok(result)
                }
            }
            _ => unreachable!(),
        }*/
        let result = T::arithmetic(columns)?;
        if self.result_type.is_date_or_date_time() {
            result.cast_with_type(&self.result_type)
        } else {
            Ok(result)
        }
    }

    fn num_arguments(&self) -> usize {
        2
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        match self.op {
            Plus => ArithmeticPlusFunction::get_monotonicity(args),
            _ => unimplemented!(),
            /* Minus => ArithmeticMinusFunction::get_monotonicity(args),
            Mul => ArithmeticMulFunction::get_monotonicity(args),
            Div => ArithmeticDivFunction::get_monotonicity(args),
            IntDiv => ArithmeticIntDivFunction::get_monotonicity(args),
            Modulo => ArithmeticModuloFunction::get_monotonicity(args),*/
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
