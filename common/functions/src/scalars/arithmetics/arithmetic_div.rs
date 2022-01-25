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

use common_datavalues2::prelude::*;
use common_datavalues2::with_match_primitive_type;
use common_exception::ErrorCode;
use common_exception::Result;
use num::traits::AsPrimitive;

use super::arithmetic_mul::arithmetic_mul_div_monotonicity;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Arithmetic2Description;
use crate::scalars::BinaryArithmeticFunction2;
use crate::scalars::Function2;
use crate::scalars::Monotonicity;
use crate::scalars::ScalarBinaryFunction;

#[derive(Clone, Debug, Default)]
struct DivFunction {}

impl<L, R> ScalarBinaryFunction<L, R, f64> for DivFunction
where
    L: PrimitiveType + AsPrimitive<f64>,
    R: PrimitiveType + AsPrimitive<f64>,
{
    fn eval(&self, l: L::RefType<'_>, r: R::RefType<'_>) -> f64 {
        l.to_owned_scalar().as_() / r.to_owned_scalar().as_()
    }
}

pub struct ArithmeticDivFunction;

impl ArithmeticDivFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function2>> {
        let op = DataValueBinaryOperator::Div;
        let left_type = args[0].data_type_id();
        let right_type = args[1].data_type_id();

        let error_fn = || -> Result<Box<dyn Function2>> {
            Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
                left_type, op, right_type
            )))
        };

        with_match_primitive_type!(left_type, |$T| {
            with_match_primitive_type!(right_type, |$D| {
                BinaryArithmeticFunction2::<$T, $D, f64, _>::try_create_func(
                    op,
                    Float64Type::arc(),
                    DivFunction::default(),
                )
            }, {
                error_fn()
            })
        }, {
            error_fn()
        })
    }

    pub fn desc() -> Arithmetic2Description {
        Arithmetic2Description::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(2),
        )
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        arithmetic_mul_div_monotonicity(args, DataValueBinaryOperator::Div)
    }
}
