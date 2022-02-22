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

use std::ops::Neg;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_types_error;
use common_exception::ErrorCode;
use common_exception::Result;
use num::traits::AsPrimitive;
use num_traits::WrappingNeg;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticDescription;
use crate::scalars::EvalContext;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::scalars::ScalarUnaryFunction;
use crate::scalars::UnaryArithmeticFunction;

#[derive(Clone, Debug, Default)]
struct NegFunction {}

impl<L, O> ScalarUnaryFunction<L, O> for NegFunction
where
    L: PrimitiveType + AsPrimitive<O>,
    O: PrimitiveType + Neg<Output = O>,
{
    fn eval(&self, l: L::RefType<'_>, _ctx: &mut EvalContext) -> O {
        -l.to_owned_scalar().as_()
    }
}

#[derive(Clone, Debug, Default)]
struct WrappingNegFunction {}

impl<L, O> ScalarUnaryFunction<L, O> for WrappingNegFunction
where
    L: PrimitiveType + AsPrimitive<O>,
    O: IntegerType + WrappingNeg,
{
    fn eval(&self, l: L::RefType<'_>, _ctx: &mut EvalContext) -> O {
        l.to_owned_scalar().as_().wrapping_neg()
    }
}

pub struct ArithmeticNegateFunction;

impl ArithmeticNegateFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[&DataTypePtr],
    ) -> Result<Box<dyn Function>> {
        let arg_type = remove_nullable(args[0]).data_type_id();
        let op = DataValueUnaryOperator::Negate;

        with_match_primitive_types_error!(arg_type, |$T| {
            let result_type = <$T as ResultTypeOfUnary>::Negate::to_data_type();
            match result_type.data_type_id() {
                TypeID::Int64 => UnaryArithmeticFunction::<$T, i64, _>::try_create_func(
                    op,
                    result_type,
                    WrappingNegFunction::default(),
                ),
                TypeID::Int32 => UnaryArithmeticFunction::<$T, i32, _>::try_create_func(
                    op,
                    result_type,
                    WrappingNegFunction::default(),
                ),
                TypeID::Int16 => UnaryArithmeticFunction::<$T, i16, _>::try_create_func(
                    op,
                    result_type,
                    WrappingNegFunction::default(),
                ),
                TypeID::Int8 => UnaryArithmeticFunction::<$T, i8, _>::try_create_func(
                    op,
                    result_type,
                    WrappingNegFunction::default(),
                ),
                _ => UnaryArithmeticFunction::<$T, <$T as ResultTypeOfUnary>::Negate, _>::try_create_func(
                    op,
                    result_type,
                    NegFunction::default(),
                ),
            }
        })
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(1),
        )
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        // unary operation like '-f(x)', just flip the is_positive.
        // also pass the is_constant, in case the input is a constant value.
        Ok(Monotonicity::create(
            args[0].is_monotonic || args[0].is_constant,
            !args[0].is_positive,
            args[0].is_constant,
        ))
    }
}
