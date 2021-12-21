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
use super::utils::assert_binary_arguments;
use crate::binary_arithmetic_helper;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::with_match_arithmetic_type;

pub struct ArithmeticIntDivFunction;

impl ArithmeticIntDivFunction {
    pub fn try_create_func(
        _display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn Function>> {
        let op = DataValueArithmeticOperator::IntDiv;
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
        let has_signed = left_type.is_signed_numeric() || right_type.is_signed_numeric();
        let size = left_type.numeric_byte_size()?;
        let result_type = construct_numeric_type(has_signed, false, size)?;

        with_match_arithmetic_type!(left_type, |$T| {
            with_match_arithmetic_type!(right_type, |$D| {
                with_match_arithmetic_type!(result_type, |$R| {
                    BinaryArithmeticFunction::<ArithmeticIntDiv::<$T,$D,$R>>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }, e)
            }, e)
        }, e)
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().monotonicity())
    }

    pub fn get_monotonicity(_args: &[Monotonicity]) -> Result<Monotonicity> {
        Ok(Monotonicity::default())
    }
}

#[derive(Clone)]
pub struct ArithmeticIntDiv<T, D, R> {
    t: PhantomData<T>,
    d: PhantomData<D>,
    r: PhantomData<R>,
}

impl<T, D, R> ArithmeticTrait for ArithmeticIntDiv<T, D, R>
where
    f64: AsPrimitive<R>,
    T: DFPrimitiveType + AsPrimitive<f64>,
    D: DFPrimitiveType + AsPrimitive<f64> + num::Zero,
    R: DFPrimitiveType,
    DFPrimitiveArray<R>: IntoSeries,
{
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let lhs = columns[0].column().to_minimal_array()?;
        let rhs = columns[1].column().to_minimal_array()?;
        let lhs: &DFPrimitiveArray<T> = lhs.static_cast();
        let rhs: &DFPrimitiveArray<D> = rhs.static_cast();

        if rhs.into_iter().any(|v| v == Some(&D::zero())) {
            return Err(ErrorCode::BadArguments("Division by zero"));
        }

        let result: DataColumn = binary_arithmetic_helper!(lhs, rhs, f64, R, |l: f64, r: f64| {
            AsPrimitive::<R>::as_(l / r)
        });

        Ok(result.resize_constant(columns[0].column().len()))
    }
}
*/