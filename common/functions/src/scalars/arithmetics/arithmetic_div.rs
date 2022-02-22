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

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_types_error;
use common_exception::ErrorCode;
use common_exception::Result;
use num::traits::AsPrimitive;

use super::arithmetic_mul::arithmetic_mul_div_monotonicity;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticDescription;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::Monotonicity;

fn div_scalar<L, R>(l: L::RefType<'_>, r: R::RefType<'_>, _ctx: &mut EvalContext) -> f64
where
    L: PrimitiveType + AsPrimitive<f64>,
    R: PrimitiveType + AsPrimitive<f64>,
{
    l.to_owned_scalar().as_() / r.to_owned_scalar().as_()
}

pub struct ArithmeticDivFunction;

impl ArithmeticDivFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function>> {
        let op = DataValueBinaryOperator::Div;
        let left_type = remove_nullable(args[0]).data_type_id();
        let right_type = remove_nullable(args[1]).data_type_id();

        with_match_primitive_types_error!(left_type, |$T| {
            with_match_primitive_types_error!(right_type, |$D| {
                BinaryArithmeticFunction::<$T, $D, f64, _>::try_create_func(
                    op,
                    Float64Type::arc(),
                    div_scalar::<$T, $D>
                )
            })
        })
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func)).features(
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
