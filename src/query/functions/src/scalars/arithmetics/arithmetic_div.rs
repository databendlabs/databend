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

use core::fmt;
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_types_error;
use common_exception::ErrorCode;
use common_exception::Result;
use num::traits::AsPrimitive;

use super::arithmetic_mul::arithmetic_mul_div_monotonicity;
use crate::scalars::AlwaysNullFunction;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;
use crate::scalars::Monotonicity;

pub struct ArithmeticDivFunction;

impl ArithmeticDivFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[&DataTypeImpl],
    ) -> Result<Box<dyn Function>> {
        let a = remove_nullable(args[0]);
        let b = remove_nullable(args[1]);

        let return_null = a.data_type_id() == TypeID::Null || b.data_type_id() == TypeID::Null;

        if return_null {
            Ok(Box::new(AlwaysNullFunction))
        } else {
            with_match_primitive_types_error!(a.data_type_id(), |$T| {
                with_match_primitive_types_error!(b.data_type_id(), |$D| {
                    Ok(Box::new(
                        DivFunctionImpl::<$T, $D>::default()
                    ))
                })
            })
        }
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        arithmetic_mul_div_monotonicity(args, DataValueBinaryOperator::Div)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .disable_passthrough_null()
                .num_arguments(2),
        )
    }
}

#[derive(Clone, Default)]
pub struct DivFunctionImpl<L, R> {
    l: PhantomData<L>,
    r: PhantomData<R>,
}

impl<L, R> Function for DivFunctionImpl<L, R>
where
    L: PrimitiveType + AsPrimitive<f64>,
    R: PrimitiveType + AsPrimitive<f64>,
{
    fn name(&self) -> &str {
        "DivFunctionImpl"
    }

    fn return_type(&self) -> DataTypeImpl {
        wrap_nullable(&f64::to_data_type())
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let lhs = columns[0].column();
        let rhs = columns[1].column();

        let lhs_viewer = L::try_create_viewer(lhs)?;
        let rhs_viewer = R::try_create_viewer(rhs)?;

        let mut builder = NullableColumnBuilder::<f64>::with_capacity(input_rows);

        for i in 0..input_rows {
            let rhs_not_zero = rhs_viewer.value_at(i).to_owned_scalar().as_() != 0.0f64;
            let valid = lhs_viewer.valid_at(i) && rhs_viewer.valid_at(i) && rhs_not_zero;
            if valid {
                builder.append(
                    lhs_viewer.value_at(i).to_owned_scalar().as_()
                        / rhs_viewer.value_at(i).to_owned_scalar().as_(),
                    true,
                );
            } else if rhs_viewer.valid_at(i) && !rhs_not_zero {
                return Err(ErrorCode::BadArguments("/ by zero"));
            } else {
                builder.append_null();
            }
        }

        Ok(builder.build(input_rows))
    }
}

impl<L, R> fmt::Display for DivFunctionImpl<L, R>
where
    L: PrimitiveType + AsPrimitive<f64>,
    R: PrimitiveType + AsPrimitive<f64>,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "div")
    }
}
