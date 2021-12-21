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
use std::ops::Div;
use std::ops::Mul;
use std::ops::Rem;
use std::ops::Sub;

use common_datavalues::prelude::*;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;
use strength_reduce::StrengthReducedU16;
use strength_reduce::StrengthReducedU32;
use strength_reduce::StrengthReducedU64;
use strength_reduce::StrengthReducedU8;

use super::arithmetic::ArithmeticTrait;
use super::utils::assert_binary_arguments;
use crate::arithmetic_helper;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::with_match_arithmetic_type;

pub struct ArithmeticModuloFunction;

impl ArithmeticModuloFunction {
    pub fn try_create_func(
        _display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn Function>> {
        let op = DataValueArithmeticOperator::Modulo;
        assert_binary_arguments(op.clone(), arguments.len())?;

        let left_type = arguments[0].data_type();
        let right_type = arguments[1].data_type();
        let result_type = numerical_arithmetic_coercion(&op, left_type, right_type)?;
        let dtype = numerical_coercion(left_type, right_type, true)?;

        let e = Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
            left_type, op, right_type
        )));

        with_match_arithmetic_type!(left_type, |$T| {
            with_match_arithmetic_type!(right_type, |$D| {
                with_match_arithmetic_type!(dtype, |$M| {
                    with_match_arithmetic_type!(result_type, |$R| {
                        BinaryArithmeticFunction::<ArithmeticModule::<$T,$D,$M, $R>>::try_create_func(
                            op,
                            result_type.clone(),
                        )
                    }, e)
                },e)
            }, e)
        }, e)
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic())
    }

    pub fn get_monotonicity(_args: &[Monotonicity]) -> Result<Monotonicity> {
        //TODO
        Ok(Monotonicity::default())
    }
}

#[derive(Clone)]
pub struct ArithmeticModule<T, D, M, R> {
    t: PhantomData<T>,
    d: PhantomData<D>,
    m: PhantomData<M>,
    r: PhantomData<R>,
}

impl<T, D, M, R> ArithmeticTrait for ArithmeticModule<T, D, M, R>
where
    T: DFPrimitiveType + AsPrimitive<M>,
    D: DFPrimitiveType + AsPrimitive<M> + num::Zero,
    R: DFPrimitiveType,
    M: DFPrimitiveType
        + AsPrimitive<R>
        + Div<Output = M>
        + Mul<Output = M>
        + Sub<Output = M>
        + Rem<Output = M>,
    u8: AsPrimitive<R>,
    u16: AsPrimitive<R>,
    u32: AsPrimitive<R>,
    u64: AsPrimitive<R>,
    DFPrimitiveArray<R>: IntoSeries,
    Option<R>: Into<DataValue>,
{
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let lhs = columns[0].column().to_minimal_array()?;
        let rhs = columns[1].column().to_minimal_array()?;
        let lhs: &DFPrimitiveArray<T> = lhs.static_cast();
        let rhs: &DFPrimitiveArray<D> = rhs.static_cast();
        if rhs.into_iter().any(|v| v == Some(&D::zero())) {
            return Err(ErrorCode::BadArguments("Division by zero"));
        }

        let result: DataColumn = match (rhs.len(), R::data_type()) {
            // TODO(sundy): add more specific cases
            // TODO(sundy): fastmod https://lemire.me/blog/2019/02/08/faster-remainders-when-the-divisor-is-a-constant-beating-compilers-and-libdivide/
            (1, DataType::UInt8) => {
                let opt_rhs = rhs.get(0);
                match opt_rhs {
                    None => DFPrimitiveArray::<R>::full_null(columns[0].column().len()),
                    Some(rhs) => match lhs.data_type() {
                        DataType::UInt64 => {
                            let rhs = rhs.to_u64().unwrap();

                            if rhs & (rhs - 1) > 0 {
                                let reduced_modulo = StrengthReducedU64::new(rhs);
                                unary(lhs, |a| {
                                    AsPrimitive::<R>::as_(a.to_u64().unwrap() % reduced_modulo)
                                })
                            } else {
                                let mask = rhs - 1;
                                unary(lhs, |a| AsPrimitive::<R>::as_(a.to_u64().unwrap() & mask))
                            }
                        }

                        _ => {
                            let r: M = rhs.as_();
                            unary(lhs, |a| {
                                let l: M = a.as_();
                                AsPrimitive::<R>::as_(l - (l / r) * r)
                            })
                        }
                    },
                }
            }

            _ => {
                arithmetic_helper!{lhs, rhs, M, R, rem_scalar::<T, M, R>, |l: M, r: M| AsPrimitive::<R>::as_(l % r)}
            }
        }
        .into();
        Ok(result.resize_constant(columns[0].column().len()))
    }
}

// https://github.com/jorgecarleitao/arrow2/blob/main/src/compute/arithmetics/basic/rem.rs#L95
pub fn rem_scalar<T, D, R>(lhs: &DFPrimitiveArray<T>, rhs: &D) -> DFPrimitiveArray<R>
where
    T: DFPrimitiveType + AsPrimitive<D>,
    D: DFPrimitiveType + Rem<Output = D> + AsPrimitive<R>,
    R: DFPrimitiveType,
    u8: AsPrimitive<R>,
    u16: AsPrimitive<R>,
    u32: AsPrimitive<R>,
    u64: AsPrimitive<R>,
{
    let rhs = *rhs;
    match D::data_type() {
        DataType::UInt64 => {
            let rhs = rhs.to_u64().unwrap();
            let reduced_rem = StrengthReducedU64::new(rhs);
            unary(lhs, |a| {
                AsPrimitive::<R>::as_(a.to_u64().unwrap() % reduced_rem)
            })
        }
        DataType::UInt32 => {
            let rhs = rhs.to_u32().unwrap();
            let reduced_rem = StrengthReducedU32::new(rhs);
            unary(lhs, |a| {
                AsPrimitive::<R>::as_(a.to_u32().unwrap() % reduced_rem)
            })
        }
        DataType::UInt16 => {
            let rhs = rhs.to_u16().unwrap();
            let reduced_rem = StrengthReducedU16::new(rhs);
            unary(lhs, |a| {
                AsPrimitive::<R>::as_(a.to_u16().unwrap() % reduced_rem)
            })
        }
        DataType::UInt8 => {
            let rhs = rhs.to_u8().unwrap();
            let reduced_rem = StrengthReducedU8::new(rhs);
            unary(lhs, |a| {
                AsPrimitive::<R>::as_(a.to_u8().unwrap() % reduced_rem)
            })
        }
        _ => unary(lhs, |a| {
            let a: D = a.as_();
            AsPrimitive::<R>::as_(a % rhs)
        }),
    }
}
*/
