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
/*
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;

use super::arithmetic::ArithmeticTrait;
use super::arithmetic_mul::arithmetic_mul_div_monotonicity;
use super::utils::assert_binary_arguments;
use crate::binary_arithmetic;
use crate::binary_arithmetic_helper;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::with_match_arithmetic_type;

pub struct ArithmeticDivFunction;

impl ArithmeticDivFunction {
    pub fn try_create_func(
        _display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn Function>> {
        let op = DataValueArithmeticOperator::Div;
        assert_binary_arguments(op.clone(), arguments.len())?;

        let left_type = arguments[0].data_type();
        let right_type = arguments[1].data_type();
        let e = Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
            left_type, op, right_type
        )));

        // error on any non-numeric type
        if !left_type.is_numeric() || !right_type.is_numeric() {
            return e;
        };
        let result_type = DataType::Float64;

        with_match_arithmetic_type!(left_type, |$T| {
            with_match_arithmetic_type!(right_type, |$D| {
                BinaryArithmeticFunction::<ArithmeticDiv::<$T,$D>>::try_create_func(
                    op,
                    result_type.clone(),
                )
            }, e)
        }, e)
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().monotonicity())
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        arithmetic_mul_div_monotonicity(args, DataValueArithmeticOperator::Div)
    }
}

#[derive(Clone)]
pub struct ArithmeticDiv<T, D> {
    t: PhantomData<T>,
    d: PhantomData<D>,
}

impl<T, D> ArithmeticTrait for ArithmeticDiv<T, D>
where
    T: DFPrimitiveType + AsPrimitive<f64>,
    D: DFPrimitiveType + AsPrimitive<f64>,
{
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        binary_arithmetic!(
            columns[0].column(),
            columns[1].column(),
            f64,
            |l: f64, r: f64| l / r
        )
    }
}
*/