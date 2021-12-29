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

use std::marker::PhantomData;
use std::ops::Neg;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_datavalues::DataValueUnaryOperator;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;
use num_traits::WrappingNeg;

use super::arithmetic::ArithmeticTrait;
use crate::impl_unary_arith;
use crate::impl_wrapping_unary_arith;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::scalars::UnaryArithmeticFunction;
use crate::unary_arithmetic;
use crate::with_match_primitive_type;

impl_unary_arith!(ArithmeticNeg, -);

impl_wrapping_unary_arith!(ArithmeticWrappingNeg, wrapping_neg);

pub struct ArithmeticNegateFunction;

impl ArithmeticNegateFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[DataTypeAndNullable],
    ) -> Result<Box<dyn Function>> {
        let arg_type = &args[0].data_type();
        let op = DataValueUnaryOperator::Negate;
        let error_fn = || -> Result<Box<dyn Function>> {
            Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported arithmetic {} ({:?})",
                op, arg_type
            )))
        };

        if !arg_type.is_numeric() {
            return error_fn();
        };

        with_match_primitive_type!(arg_type, |$T| {
            let result_type = <$T as ResultTypeOfUnary>::Negate::data_type();
            match result_type {
                DataType::Int64 => UnaryArithmeticFunction::<ArithmeticWrappingNeg<$T, i64>>::try_create_func(
                    op,
                    result_type,
                ),
                DataType::Int32 => UnaryArithmeticFunction::<ArithmeticWrappingNeg<$T, i32>>::try_create_func(
                    op,
                    result_type,
                ),
                DataType::Int16 => UnaryArithmeticFunction::<ArithmeticWrappingNeg<$T, i16>>::try_create_func(
                    op,
                    result_type,
                ),
                DataType::Int8 => UnaryArithmeticFunction::<ArithmeticWrappingNeg<$T, i8>>::try_create_func(
                    op,
                    result_type,
                ),
                _ => UnaryArithmeticFunction::<ArithmeticNeg<$T, <$T as ResultTypeOfUnary>::Negate>>::try_create_func(
                    op,
                    result_type,
                ),
            }
        }, {
            error_fn()
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
        if args.len() != 1 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        // unary operation like '-f(x)', just flip the is_positive.
        // also pass the is_constant, in case the input is a constant value.
        Ok(Monotonicity::create(
            args[0].is_monotonic || args[0].is_constant,
            !args[0].is_positive,
            args[0].is_constant,
        ))
    }
}
