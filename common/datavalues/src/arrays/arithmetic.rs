// Copyright 2020 Datafuse Labs.
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

//! Implementations of arithmetic operations on DataArray's.
use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Neg;
use std::ops::Rem;
use std::ops::Sub;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::array::UInt64Array;
use common_arrow::arrow::compute::arithmetics::basic;
use common_arrow::arrow::compute::arithmetics::negate;
use common_arrow::arrow::compute::arity::unary;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::error::ArrowError;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;
use num::NumCast;
use num::ToPrimitive;
use strength_reduce::StrengthReducedU64;

use crate::arrays::ops::*;
use crate::prelude::*;

/// TODO: sundy
/// check division by zero in rem & div ops
fn arithmetic_helper<T, Kernel, SKernel, F>(
    lhs: &DFPrimitiveArray<T>,
    rhs: &DFPrimitiveArray<T>,
    kernel: Kernel,
    scalar_kernel: SKernel,
    operation: F,
) -> Result<DFPrimitiveArray<T>>
where
    T: DFPrimitiveType,
    T: Add<Output = T> + Sub<Output = T> + Mul<Output = T> + Div<Output = T> + num::Zero,
    Kernel: Fn(
        &PrimitiveArray<T>,
        &PrimitiveArray<T>,
    ) -> std::result::Result<PrimitiveArray<T>, ArrowError>,
    SKernel: Fn(&PrimitiveArray<T>, &T) -> PrimitiveArray<T>,
    F: Fn(T, T) -> T,
{
    let ca = match (lhs.len(), rhs.len()) {
        (a, b) if a == b => {
            let array = kernel(lhs.inner(), rhs.inner()).expect("output");

            array.into()
        }
        // broadcast right path
        (_, 1) => {
            let opt_rhs = rhs.get(0);
            match opt_rhs {
                None => DFPrimitiveArray::<T>::full_null(lhs.len()),
                Some(rhs) => {
                    let array = scalar_kernel(lhs.inner(), &rhs);
                    array.into()
                }
            }
        }
        (1, _) => {
            let opt_lhs = lhs.get(0);
            match opt_lhs {
                None => DFPrimitiveArray::<T>::full_null(rhs.len()),
                Some(lhs) => rhs.apply(|rhs| operation(lhs, rhs)),
            }
        }
        _ => unreachable!(),
    };
    Ok(ca)
}

impl<T> Add for &DFPrimitiveArray<T>
where T: DFPrimitiveType
        + Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Div<Output = T>
        + NumCast
        + num::Zero
{
    type Output = Result<DFPrimitiveArray<T>>;

    fn add(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::add, basic::add_scalar, |lhs, rhs| {
            lhs + rhs
        })
    }
}

impl<T> Sub for &DFPrimitiveArray<T>
where
    T: DFPrimitiveType,
    T: Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Div<Output = T>
        + Rem<Output = T>
        + num::Zero,
{
    type Output = Result<DFPrimitiveArray<T>>;

    fn sub(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::sub, basic::sub_scalar, |lhs, rhs| {
            lhs - rhs
        })
    }
}

impl<T> Mul for &DFPrimitiveArray<T>
where
    T: DFPrimitiveType,
    T: Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Div<Output = T>
        + Rem<Output = T>
        + NumCast
        + num::Zero,
{
    type Output = Result<DFPrimitiveArray<T>>;

    fn mul(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::mul, basic::mul_scalar, |lhs, rhs| {
            lhs * rhs
        })
    }
}

impl<T> Div for &DFPrimitiveArray<T>
where
    T: DFPrimitiveType,
    T: Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Div<Output = T>
        + Rem<Output = T>
        + NumCast
        + num::Zero
        + num::One,
{
    type Output = Result<DFPrimitiveArray<T>>;

    fn div(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::div, basic::div_scalar, |lhs, rhs| {
            lhs / rhs
        })
    }
}

// we don't impl Rem because we have specific dtype for the result type
// this is very efficient for some cases
// such as: UInt64 % Const UInt8, the result is always UInt8
// 1. turn it into UInt64 % Const UInt64
// 2. create UInt8Array to accept the result, this could save lots of allocation than UInt64Array

impl<T> DFPrimitiveArray<T>
where
    T: DFPrimitiveType,
    DFPrimitiveArray<T>: IntoSeries,
    T: Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Div<Output = T>
        + Rem<Output = T>
        + NumCast
        + ToPrimitive
        + AsPrimitive<u8>
        + num::Zero
        + num::One,
{
    pub fn rem(&self, rhs: &Self, dtype: &DataType) -> Result<Series> {
        match (rhs.len(), dtype) {
            // TODO(sundy): add more specific cases
            // TODO(sundy): fastmod https://lemire.me/blog/2019/02/08/faster-remainders-when-the-divisor-is-a-constant-beating-compilers-and-libdivide/
            (1, DataType::UInt8) => {
                let opt_rhs = rhs.get(0);
                match opt_rhs {
                    None => Ok(DFUInt8Array::full_null(self.len()).into_series()),
                    Some(rhs) => match self.data_type() {
                        DataType::UInt64 => {
                            let arr = self.array.as_any().downcast_ref::<UInt64Array>().unwrap();

                            let rhs: u8 = rhs.as_();
                            let rhs = rhs as u64;

                            if rhs & (rhs - 1) > 0 {
                                let reduced_modulo = StrengthReducedU64::new(rhs);
                                let res = unary(
                                    arr,
                                    |a| (a % reduced_modulo) as u8,
                                    ArrowDataType::UInt8,
                                );
                                let array = DFUInt8Array::new(res);
                                Ok(array.into_series())
                            } else {
                                let mask = rhs - 1;
                                let res = unary(arr, |a| (a & mask) as u8, ArrowDataType::UInt8);
                                let array = DFUInt8Array::new(res);
                                Ok(array.into_series())
                            }
                        }

                        _ => {
                            let array: DFUInt8Array = self.apply_cast_numeric(|a| {
                                AsPrimitive::<u8>::as_(a - (a / rhs) * rhs)
                            });
                            Ok(array.into_series())
                        }
                    },
                }
            }

            _ => {
                let array =
                    arithmetic_helper(self, rhs, basic::rem, basic::rem_scalar, |lhs, rhs| {
                        lhs % rhs
                    })?;
                Ok(array.into_series())
            }
        }
    }
}

impl<T> Neg for &DFPrimitiveArray<T>
where
    T: DFPrimitiveType,
    T: Add<Output = T>
        + Sub<Output = T>
        + Mul<Output = T>
        + Div<Output = T>
        + Rem<Output = T>
        + NumCast
        + num::Zero
        + num::One,
{
    type Output = Result<Series>;

    fn neg(self) -> Self::Output {
        // let arr = negate(&self.array);
        let arr = &self.array;
        unsafe {
            match self.data_type() {
                DataType::Int8 => {
                    let v = negate(&*(arr as *const dyn Array as *const PrimitiveArray<i8>));
                    Ok(DFInt8Array::new(v).into_series())
                }

                DataType::Int16 => {
                    let v = negate(&*(arr as *const dyn Array as *const PrimitiveArray<i16>));
                    Ok(DFInt16Array::new(v).into_series())
                }

                DataType::Int32 => {
                    let v = negate(&*(arr as *const dyn Array as *const PrimitiveArray<i32>));
                    Ok(DFInt32Array::new(v).into_series())
                }
                DataType::Int64 => {
                    let v = negate(&*(arr as *const dyn Array as *const PrimitiveArray<i64>));
                    Ok(DFInt64Array::new(v).into_series())
                }
                DataType::Float32 => {
                    let v = negate(&*(arr as *const dyn Array as *const PrimitiveArray<f32>));
                    Ok(DFFloat32Array::new(v).into_series())
                }
                DataType::Float64 => {
                    let v = negate(&*(arr as *const dyn Array as *const PrimitiveArray<f64>));
                    Ok(DFFloat64Array::new(v).into_series())
                }

                _ => Err(ErrorCode::IllegalDataType(format!(
                    "DataType {:?} is Unsupported for neg op",
                    self.data_type()
                ))),
            }
        }
    }
}

fn concat_strings(l: &[u8], r: &[u8]) -> Vec<u8> {
    let mut s = Vec::with_capacity(l.len() + r.len());
    s.extend_from_slice(l);
    s.extend_from_slice(r);
    s
}

impl Add for &DFStringArray {
    type Output = Result<DFStringArray>;

    fn add(self, rhs: Self) -> Self::Output {
        // broadcasting path
        if rhs.len() == 1 {
            let rhs = rhs.get(0);
            return match rhs {
                Some(rhs) => self.add(rhs),
                None => Ok(DFStringArray::full_null(self.len())),
            };
        }

        // todo! add no_null variants. Need 4 paths.
        Ok(self
            .inner()
            .iter()
            .zip(rhs.inner().iter())
            .map(|(opt_l, opt_r)| match (opt_l, opt_r) {
                (Some(l), Some(r)) => Some(concat_strings(l, r)),
                _ => None,
            })
            .collect())
    }
}

impl Add for DFStringArray {
    type Output = Result<DFStringArray>;

    fn add(self, rhs: Self) -> Self::Output {
        (&self).add(&rhs)
    }
}

impl Add<&[u8]> for &DFStringArray {
    type Output = Result<DFStringArray>;

    fn add(self, rhs: &[u8]) -> Self::Output {
        Ok(match self.null_count() {
            0 => self
                .into_no_null_iter()
                .map(|l| concat_strings(l, rhs))
                .collect(),
            _ => self
                .inner()
                .iter()
                .map(|opt_l| opt_l.map(|l| concat_strings(l, rhs)))
                .collect(),
        })
    }
}

pub trait Pow {
    fn pow_f32(&self, _exp: f32) -> DFFloat32Array {
        unimplemented!()
    }
    fn pow_f64(&self, _exp: f64) -> DFFloat64Array {
        unimplemented!()
    }
}

impl<T> Pow for DFPrimitiveArray<T>
where
    T: DFPrimitiveType,
    DFPrimitiveArray<T>: ArrayCast,
{
    fn pow_f32(&self, exp: f32) -> DFFloat32Array {
        let arr = self.cast_with_type(&DataType::Float32).expect("f32 array");
        let arr = arr.f32().unwrap();
        DFFloat32Array::new(basic::powf_scalar(&arr.array, exp))
    }

    fn pow_f64(&self, exp: f64) -> DFFloat64Array {
        let arr = self.cast_with_type(&DataType::Float64).expect("f64 array");
        let arr = arr.f64().unwrap();

        DFFloat64Array::new(basic::powf_scalar(&arr.array, exp))
    }
}

impl Pow for DFBooleanArray {}
impl Pow for DFStringArray {}
impl Pow for DFListArray {}
