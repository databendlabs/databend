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
use std::ops::Div;
use std::ops::Mul;
use std::ops::Rem;
use std::ops::Sub;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;
use strength_reduce::StrengthReducedU16;
use strength_reduce::StrengthReducedU32;
use strength_reduce::StrengthReducedU64;
use strength_reduce::StrengthReducedU8;

use super::arithmetic::ArithmeticTrait;
use super::result_type::ResultTypeOfBinaryArith;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::BinaryArithmeticOperator;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::with_match_primitive_type;

pub struct ArithmeticModuloFunction;

impl ArithmeticModuloFunction {
    pub fn try_create_func(_display_name: &str, args: &[DataType]) -> Result<Box<dyn Function>> {
        let left_type = &args[0];
        let right_type = &args[1];
        let op = BinaryArithmeticOperator::Modulo;

        let error_fn = || -> Result<Box<dyn Function>> {
            Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
                left_type, op, right_type
            )))
        };

        if !left_type.is_numeric() || !right_type.is_numeric() {
            return error_fn();
        };

        with_match_primitive_type!(left_type, |$T| {
            with_match_primitive_type!(right_type, |$D| {
                let result_type = <($T, $D) as ResultTypeOfBinaryArith>::Modulo::data_type();
                BinaryArithmeticFunction::<ArithmeticModule<$T, $D, <($T, $D) as ResultTypeOfBinaryArith>::LeastSuper, <($T, $D) as ResultTypeOfBinaryArith>::Modulo>>::try_create_func(
                    op,
                    result_type,
                )
            }, {
                error_fn()
            })
        }, {
            error_fn()
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
        + AsPrimitive<u8>
        + Div<Output = M>
        + Mul<Output = M>
        + Sub<Output = M>
        + Rem<Output = M>,
    DFPrimitiveArray<M>: IntoSeries,
{
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let lhs = columns[0].column().to_minimal_array()?;
        let rhs = columns[1].column().to_minimal_array()?;
        let lhs: &DFPrimitiveArray<T> = lhs.static_cast();
        let rhs: &DFPrimitiveArray<D> = rhs.static_cast();
        let result_type = R::data_type();
        let least_super = M::data_type();
        if result_type.is_integer() && rhs.into_iter().any(|v| v == Some(&D::zero())) {
            return Err(ErrorCode::BadArguments("Division by zero"));
        }

        let series = match (rhs.len(), result_type.clone()) {
            // TODO(sundy): add more specific cases
            // TODO(sundy): fastmod https://lemire.me/blog/2019/02/08/faster-remainders-when-the-divisor-is-a-constant-beating-compilers-and-libdivide/
            (1, DataType::UInt8) => {
                let opt_rhs = rhs.get(0);
                match opt_rhs {
                    None => DFUInt8Array::full_null(lhs.len()).into_series(),
                    Some(rhs) => match least_super {
                        DataType::UInt64 => {
                            let rhs = rhs.to_u8().unwrap();
                            let rhs = rhs as u64;

                            if rhs & (rhs - 1) > 0 {
                                let reduced_modulo = StrengthReducedU64::new(rhs);
                                let array =
                                    unary(lhs, |a| (a.to_u64().unwrap() % reduced_modulo) as u8);
                                array.into_series()
                            } else {
                                let mask = rhs - 1;
                                let array = unary(lhs, |a| (a.to_u64().unwrap() & mask) as u8);
                                array.into_series()
                            }
                        }

                        _ => {
                            let r: M = rhs.as_();
                            let array = unary(lhs, |v| {
                                let l: M = v.as_();
                                AsPrimitive::<u8>::as_(l - (l / r) * r)
                            });
                            array.into_series()
                        }
                    },
                }
            }

            _ => match (lhs.len(), rhs.len()) {
                (a, b) if a == b => {
                    let array = binary(lhs, rhs, |l, r| {
                        AsPrimitive::<M>::as_(l) % AsPrimitive::<M>::as_(r)
                    });
                    array.into_series()
                }
                (_, 1) => {
                    let opt_rhs = rhs.get(0);
                    match opt_rhs {
                        None => DFPrimitiveArray::<M>::full_null(lhs.len()).into_series(),
                        Some(rhs) => {
                            let r: M = rhs.as_();
                            rem_scalar(lhs, &r)
                        }
                    }
                }
                (1, _) => {
                    let opt_lhs = lhs.get(0);
                    match opt_lhs {
                        None => DFPrimitiveArray::<M>::full_null(rhs.len()).into_series(),
                        Some(lhs) => {
                            let l: M = lhs.as_();
                            let array = unary(rhs, |r| l % AsPrimitive::<M>::as_(r));
                            array.into_series()
                        }
                    }
                }
                _ => unreachable!(),
            },
        };

        let result: DataColumn = if series.data_type() != &result_type {
            series.cast_with_type(&result_type)?
        } else {
            series
        }
        .into();
        Ok(result.resize_constant(columns[0].column().len()))
    }
}

// https://github.com/jorgecarleitao/arrow2/blob/main/src/compute/arithmetics/basic/rem.rs#L95
pub fn rem_scalar<T, D>(lhs: &DFPrimitiveArray<T>, rhs: &D) -> Series
where
    T: DFPrimitiveType + AsPrimitive<D>,
    D: DFPrimitiveType + Rem<Output = D>,
    DFPrimitiveArray<D>: IntoSeries,
{
    let rhs = *rhs;
    match D::data_type() {
        DataType::UInt64 => {
            let rhs = rhs.to_u64().unwrap();
            let reduced_rem = StrengthReducedU64::new(rhs);
            let array = unary(lhs, |a| a.to_u64().unwrap() % reduced_rem);
            array.into_series()
        }
        DataType::UInt32 => {
            let rhs = rhs.to_u32().unwrap();
            let reduced_rem = StrengthReducedU32::new(rhs);
            let array = unary(lhs, |a| a.to_u32().unwrap() % reduced_rem);
            array.into_series()
        }
        DataType::UInt16 => {
            let rhs = rhs.to_u16().unwrap();
            let reduced_rem = StrengthReducedU16::new(rhs);
            let array = unary(lhs, |a| a.to_u16().unwrap() % reduced_rem);
            array.into_series()
        }
        DataType::UInt8 => {
            let rhs = rhs.to_u8().unwrap();
            let reduced_rem = StrengthReducedU8::new(rhs);
            let array = unary(lhs, |a| a.to_u8().unwrap() % reduced_rem);
            array.into_series()
        }
        _ => {
            let array = unary(lhs, |a| {
                let a: D = a.as_();
                a % rhs
            });
            array.into_series()
        }
    }
}
