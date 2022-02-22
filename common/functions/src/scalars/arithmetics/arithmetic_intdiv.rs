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
use num::Zero;
use num_traits::AsPrimitive;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticDescription;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::EvalContext;
use crate::scalars::Function;

fn intdiv_scalar<L, R, O>(l: L::RefType<'_>, r: R::RefType<'_>, ctx: &mut EvalContext) -> O
where
    f64: AsPrimitive<O>,
    L: PrimitiveType + AsPrimitive<f64>,
    R: PrimitiveType + AsPrimitive<f64>,
    O: IntegerType + Zero,
{
    let l = l.to_owned_scalar().as_();
    let r = r.to_owned_scalar().as_();
    if std::intrinsics::unlikely(r == 0.0) {
        ctx.set_error(ErrorCode::BadArguments("Division by zero"));
        return O::zero();
    }
    (l / r).as_()
}

pub struct ArithmeticIntDivFunction;

impl ArithmeticIntDivFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function>> {
        let left_type = remove_nullable(args[0]).data_type_id();
        let right_type = remove_nullable(args[1]).data_type_id();

        with_match_primitive_types_error!(left_type, |$T| {
            with_match_primitive_types_error!(right_type, |$D| {
                BinaryArithmeticFunction::<$T, $D, <($T, $D) as ResultTypeOfBinary>::IntDiv, _>::try_create_func(
                    DataValueBinaryOperator::IntDiv,
                    <($T, $D) as ResultTypeOfBinary>::IntDiv::to_data_type(),
                    intdiv_scalar::<$T, $D, _>
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
}
