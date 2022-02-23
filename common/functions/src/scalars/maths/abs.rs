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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function::Function;
use crate::scalars::function_common::assert_numeric;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::EvalContext;
use crate::scalars::Monotonicity;
use crate::scalars::ScalarUnaryExpression;

#[derive(Clone)]
pub struct AbsFunction {
    _display_name: String,
}

impl AbsFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(AbsFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(1),
        )
    }
}

macro_rules! impl_abs_function {
    ($column:expr, $type:ident, $super:ident, $target:ident) => {{
        let unary =
            ScalarUnaryExpression::<$type, $target, _>::new(|v: $type, ctx: &mut EvalContext| {
                let s = v as $super;
                if s == $super::MIN {
                    ctx.set_error(ErrorCode::Overflow(format!(
                        "Overflow on abs signed number {}",
                        v
                    )));
                    return $target::default();
                }
                $super::abs(s) as $target
            });
        let col = unary.eval($column.column(), &mut EvalContext::default())?;
        Ok(col.arc())
    }};
}

impl Function for AbsFunction {
    fn name(&self) -> &str {
        "abs"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_numeric(args[0])?;
        let data_type = match args[0].data_type_id() {
            TypeID::Int8 => u8::to_data_type(),
            TypeID::Int16 => u16::to_data_type(),
            TypeID::Int32 => u32::to_data_type(),
            TypeID::Int64 => u64::to_data_type(),
            TypeID::UInt8 => u8::to_data_type(),
            TypeID::UInt16 => u16::to_data_type(),
            TypeID::UInt32 => u32::to_data_type(),
            TypeID::UInt64 => u64::to_data_type(),
            TypeID::Float32 => f32::to_data_type(),
            TypeID::Float64 => f64::to_data_type(),
            _ => unreachable!(),
        };
        Ok(data_type)
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        match columns[0].data_type().data_type_id() {
            TypeID::Int8 => impl_abs_function!(columns[0], i8, i64, u8),
            TypeID::Int16 => impl_abs_function!(columns[0], i16, i64, u16),
            TypeID::Int32 => impl_abs_function!(columns[0], i32, i64, u32),
            TypeID::Int64 => impl_abs_function!(columns[0], i64, i64, u64),
            TypeID::Float32 => impl_abs_function!(columns[0], f32, f64, f32),
            TypeID::Float64 => impl_abs_function!(columns[0], f64, f64, f64),
            _ => Ok(columns[0].column().clone()),
        }
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        // for constant value, just return clone
        if args[0].is_constant {
            return Ok(args[0].clone());
        }

        // if either left boundary or right boundary is unknown, we don't known the monotonicity
        if args[0].left.is_none() || args[0].right.is_none() {
            return Ok(Monotonicity::default());
        }

        match args[0].compare_with_zero()? {
            1 => {
                // the range is >= 0, abs function do nothing
                Ok(Monotonicity::create(true, args[0].is_positive, false))
            }
            -1 => {
                // the range is <= 0, abs function flip the is_positive
                Ok(Monotonicity::create(true, !args[0].is_positive, false))
            }
            _ => Ok(Monotonicity::default()),
        }
    }
}

impl fmt::Display for AbsFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ABS")
    }
}
