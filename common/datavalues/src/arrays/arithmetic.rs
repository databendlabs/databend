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
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::compute::arithmetics::basic;
use common_arrow::arrow::compute::arithmetics::negate;
use common_arrow::arrow::compute::arity::unary;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::error::ArrowError;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;
use num::ToPrimitive;
use strength_reduce::StrengthReducedU64;

use crate::arrays::ops::*;
use crate::arrays::DataArray;
use crate::prelude::*;

fn arithmetic_helper<T, Kernel, SKernel, F>(
    lhs: &DataArray<T>,
    rhs: &DataArray<T>,
    kernel: Kernel,
    scalar_kernel: SKernel,
    operation: F,
) -> Result<DataArray<T>>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + num::Zero,
    Kernel: Fn(
        &PrimitiveArray<T::Native>,
        &PrimitiveArray<T::Native>,
    ) -> std::result::Result<PrimitiveArray<T::Native>, ArrowError>,
    SKernel: Fn(&PrimitiveArray<T::Native>, &T::Native) -> PrimitiveArray<T::Native>,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    let ca = match (lhs.len(), rhs.len()) {
        (a, b) if a == b => {
            let array = Arc::new(kernel(lhs.downcast_ref(), rhs.downcast_ref()).expect("output"))
                as ArrayRef;

            array.into()
        }
        // broadcast right path
        (_, 1) => {
            let opt_rhs = rhs.get(0);
            match opt_rhs {
                None => DataArray::full_null(lhs.len()),
                Some(rhs) => {
                    let array = Arc::new(scalar_kernel(lhs.downcast_ref(), &rhs)) as ArrayRef;
                    array.into()
                }
            }
        }
        (1, _) => {
            let opt_lhs = lhs.get(0);
            match opt_lhs {
                None => DataArray::full_null(rhs.len()),
                Some(lhs) => rhs.apply(|rhs| operation(lhs, rhs)),
            }
        }
        _ => unreachable!(),
    };
    Ok(ca)
}

impl<T> Add for &DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + num::Zero,
{
    type Output = Result<DataArray<T>>;

    fn add(self, rhs: Self) -> Self::Output {
        arithmetic_helper(
            self,
            rhs,
            basic::add::add,
            basic::add::add_scalar,
            |lhs, rhs| lhs + rhs,
        )
    }
}

impl<T> Sub for &DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero,
{
    type Output = Result<DataArray<T>>;

    fn sub(self, rhs: Self) -> Self::Output {
        arithmetic_helper(
            self,
            rhs,
            basic::sub::sub,
            basic::sub::sub_scalar,
            |lhs, rhs| lhs - rhs,
        )
    }
}

impl<T> Mul for &DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero,
{
    type Output = Result<DataArray<T>>;

    fn mul(self, rhs: Self) -> Self::Output {
        arithmetic_helper(
            self,
            rhs,
            basic::mul::mul,
            basic::mul::mul_scalar,
            |lhs, rhs| lhs * rhs,
        )
    }
}

impl<T> Div for &DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero
        + num::One,
{
    type Output = Result<DataArray<T>>;

    fn div(self, rhs: Self) -> Self::Output {
        arithmetic_helper(
            self,
            rhs,
            basic::div::div,
            basic::div::div_scalar,
            |lhs, rhs| lhs / rhs,
        )
    }
}

// we don't impl Rem because we have specific dtype for the result type
// this is very efficient for some cases
// such as: UInt64 % Const UInt8, the result is always UInt8
// 1. turn it into UInt64 % Const UInt64
// 2. create UInt8Array to accept the result, this could save lots of allocation than UInt64Array

impl<T> DataArray<T>
where
    T: DFNumericType,
    DataArray<T>: IntoSeries,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
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
                            let arr = &*self.array;
                            let arr = unsafe {
                                &*(arr as *const dyn Array as *const PrimitiveArray<u64>)
                            };
                            let rhs: u8 = rhs.as_();
                            let rhs = rhs as u64;

                            if rhs & (rhs - 1) > 0 {
                                let reduced_modulo = StrengthReducedU64::new(rhs);
                                let res = unary(
                                    arr,
                                    |a| (a % reduced_modulo) as u8,
                                    ArrowDataType::UInt8,
                                );
                                let array = DFUInt8Array::from_arrow_array(res);
                                Ok(array.into_series())
                            } else {
                                let mask = rhs - 1;
                                let res = unary(arr, |a| (a & mask) as u8, ArrowDataType::UInt8);
                                let array = DFUInt8Array::from_arrow_array(res);
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
                let array = arithmetic_helper(
                    self,
                    rhs,
                    basic::rem::rem,
                    basic::rem::rem_scalar,
                    |lhs, rhs| lhs % rhs,
                )?;
                Ok(array.into_series())
            }
        }
    }
}

impl<T> Neg for &DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero
        + num::One,
{
    type Output = Result<DataArray<T>>;

    fn neg(self) -> Self::Output {
        let arr = &*self.array;
        let result = unsafe {
            match self.data_type() {
                DataType::Int8 => Ok(Arc::new(negate(
                    &*(arr as *const dyn Array as *const PrimitiveArray<i8>),
                )) as ArrayRef),

                DataType::Int16 => Ok(Arc::new(negate(
                    &*(arr as *const dyn Array as *const PrimitiveArray<i16>),
                )) as ArrayRef),

                DataType::Int32 => Ok(Arc::new(negate(
                    &*(arr as *const dyn Array as *const PrimitiveArray<i32>),
                )) as ArrayRef),
                DataType::Int64 => Ok(Arc::new(negate(
                    &*(arr as *const dyn Array as *const PrimitiveArray<i64>),
                )) as ArrayRef),
                DataType::Float32 => Ok(Arc::new(negate(
                    &*(arr as *const dyn Array as *const PrimitiveArray<f32>),
                )) as ArrayRef),
                DataType::Float64 => Ok(Arc::new(negate(
                    &*(arr as *const dyn Array as *const PrimitiveArray<f64>),
                )) as ArrayRef),

                _ => Err(ErrorCode::IllegalDataType(format!(
                    "DataType {:?} is Unsupported for neg op",
                    self.data_type()
                ))),
            }
        };
        let result = result?;
        Ok(result.into())
    }
}

fn concat_strings(l: &str, r: &str) -> String {
    // fastest way to concat strings according to https://github.com/hoodie/concatenation_benchmarks-rs
    let mut s = String::with_capacity(l.len() + r.len());
    s.push_str(l);
    s.push_str(r);
    s
}

impl Add for &DFUtf8Array {
    type Output = Result<DFUtf8Array>;

    fn add(self, rhs: Self) -> Self::Output {
        // broadcasting path
        if rhs.len() == 1 {
            let rhs = rhs.get(0);
            return match rhs {
                Some(rhs) => self.add(rhs),
                None => Ok(DFUtf8Array::full_null(self.len())),
            };
        }

        // todo! add no_null variants. Need 4 paths.
        Ok(self
            .downcast_iter()
            .zip(rhs.downcast_iter())
            .map(|(opt_l, opt_r)| match (opt_l, opt_r) {
                (Some(l), Some(r)) => Some(concat_strings(l, r)),
                _ => None,
            })
            .collect())
    }
}

impl Add for DFUtf8Array {
    type Output = Result<DFUtf8Array>;

    fn add(self, rhs: Self) -> Self::Output {
        (&self).add(&rhs)
    }
}

impl Add<&str> for &DFUtf8Array {
    type Output = Result<DFUtf8Array>;

    fn add(self, rhs: &str) -> Self::Output {
        Ok(match self.null_count() {
            0 => self
                .into_no_null_iter()
                .map(|l| concat_strings(l, rhs))
                .collect(),
            _ => self
                .downcast_iter()
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

impl<T> Pow for DataArray<T>
where
    T: DFNumericType,
    DataArray<T>: ArrayCast,
{
    fn pow_f32(&self, exp: f32) -> DFFloat32Array {
        self.cast::<Float32Type>()
            .expect("f32 array")
            .apply_kernel(|arr| Arc::new(basic::pow::powf_scalar(arr, exp)))
    }

    fn pow_f64(&self, exp: f64) -> DFFloat64Array {
        self.cast::<Float64Type>()
            .expect("f64 array")
            .apply_kernel(|arr| Arc::new(basic::pow::powf_scalar(arr, exp)))
    }
}

impl Pow for DFBooleanArray {}
impl Pow for DFUtf8Array {}
impl Pow for DFListArray {}
