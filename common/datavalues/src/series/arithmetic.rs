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

use std::fmt::Debug;
use std::ops;
use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Neg;
use std::ops::Rem;
use std::ops::Sub;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::arrays::DataArray;
use crate::prelude::*;
use crate::DataValueArithmeticOperator;

impl Add for &Series {
    type Output = Result<Series>;

    fn add(self, rhs: Self) -> Self::Output {
        let (lhs, rhs) = coerce_lhs_rhs(&DataValueArithmeticOperator::Plus, self, rhs)?;
        lhs.add_to(&rhs)
    }
}

impl Sub for &Series {
    type Output = Result<Series>;

    fn sub(self, rhs: Self) -> Self::Output {
        let (lhs, rhs) = coerce_lhs_rhs(&DataValueArithmeticOperator::Minus, self, rhs)?;
        lhs.subtract(&rhs)
    }
}

impl Mul for &Series {
    type Output = Result<Series>;

    fn mul(self, rhs: Self) -> Self::Output {
        let (lhs, rhs) = coerce_lhs_rhs(&DataValueArithmeticOperator::Mul, self, rhs)?;
        lhs.multiply(&rhs)
    }
}

impl Div for &Series {
    type Output = Result<Series>;

    fn div(self, rhs: Self) -> Self::Output {
        let (lhs, rhs) = coerce_lhs_rhs(&DataValueArithmeticOperator::Div, self, rhs)?;
        lhs.divide(&rhs)
    }
}

impl Rem for &Series {
    type Output = Result<Series>;

    fn rem(self, rhs: Self) -> Self::Output {
        // apply rem with the largest types
        let dtype = numerical_arithmetic_coercion(
            &DataValueArithmeticOperator::Modulo,
            &self.data_type(),
            &rhs.data_type(),
        )?;

        let (lhs, rhs) = coerce_lhs_rhs_no_op(self, rhs)?;
        let result = lhs.remainder(&rhs, &dtype)?;

        // then cast back to the lowest types
        if result.data_type() != dtype {
            result.cast_with_type(&dtype)
        } else {
            Ok(result)
        }
    }
}

impl Neg for &Series {
    type Output = Result<Series>;

    fn neg(self) -> Self::Output {
        let lhs = coerce_to_signed(self)?;
        lhs.negative()
    }
}

pub trait NumOpsDispatch: Debug {
    fn subtract(&self, rhs: &Series) -> Result<Series> {
        Err(ErrorCode::BadDataValueType(format!(
            "subtraction operation not supported for {:?} and {:?}",
            self, rhs
        )))
    }

    fn add_to(&self, rhs: &Series) -> Result<Series> {
        Err(ErrorCode::BadDataValueType(format!(
            "addition operation not supported for {:?} and {:?}",
            self, rhs
        )))
    }
    fn multiply(&self, rhs: &Series) -> Result<Series> {
        Err(ErrorCode::BadDataValueType(format!(
            "multiplication operation not supported for {:?} and {:?}",
            self, rhs
        )))
    }
    fn divide(&self, rhs: &Series) -> Result<Series> {
        Err(ErrorCode::BadDataValueType(format!(
            "division operation not supported for {:?} and {:?}",
            self, rhs
        )))
    }

    fn remainder(&self, rhs: &Series, _dtype: &DataType) -> Result<Series> {
        Err(ErrorCode::BadDataValueType(format!(
            "remainder operation not supported for {:?} and {:?}",
            self, rhs
        )))
    }

    fn negative(&self) -> Result<Series> {
        Err(ErrorCode::BadDataValueType(format!(
            "negative operation not supported for {:?}",
            self,
        )))
    }
}

impl<T> NumOpsDispatch for DataArray<T>
where
    T: DFNumericType,

    T::Native: ops::Add<Output = T::Native>
        + ops::Sub<Output = T::Native>
        + ops::Mul<Output = T::Native>
        + ops::Div<Output = T::Native>
        + ops::Rem<Output = T::Native>
        + num::Zero
        + num::One
        + num::ToPrimitive
        + num::traits::AsPrimitive<u8>
        + num::NumCast,
    DataArray<T>: IntoSeries,
{
    fn subtract(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(rhs)? };
        let out = (self - rhs)?;
        Ok(out.into_series())
    }
    fn add_to(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(rhs)? };
        let out = (self + rhs)?;
        Ok(out.into_series())
    }
    fn multiply(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(rhs)? };
        let out = (self * rhs)?;
        Ok(out.into_series())
    }
    fn divide(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(rhs)? };
        let out = (self / rhs)?;
        Ok(out.into_series())
    }
    fn remainder(&self, rhs: &Series, dtype: &DataType) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(rhs)? };
        self.rem(rhs, dtype)
    }

    fn negative(&self) -> Result<Series> {
        let out = std::ops::Neg::neg(self)?;
        Ok(out.into_series())
    }
}

impl NumOpsDispatch for DFUtf8Array {
    fn add_to(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(rhs)? };
        let out = (self + rhs)?;
        Ok(out.into_series())
    }
}
impl NumOpsDispatch for DFBooleanArray {}
impl NumOpsDispatch for DFListArray {}
impl NumOpsDispatch for DFBinaryArray {}
impl NumOpsDispatch for DFNullArray {}
impl NumOpsDispatch for DFStructArray {}

fn coerce_lhs_rhs(
    op: &DataValueArithmeticOperator,
    lhs: &Series,
    rhs: &Series,
) -> Result<(Series, Series)> {
    let dtype = numerical_arithmetic_coercion(op, &lhs.data_type(), &rhs.data_type())?;

    let mut left = lhs.clone();
    if lhs.data_type() != dtype {
        left = lhs.cast_with_type(&dtype)?;
    }

    let mut right = rhs.clone();
    if rhs.data_type() != dtype {
        right = rhs.cast_with_type(&dtype)?;
    }

    Ok((left, right))
}

fn coerce_lhs_rhs_no_op(lhs: &Series, rhs: &Series) -> Result<(Series, Series)> {
    let dtype = numerical_coercion(&lhs.data_type(), &rhs.data_type())?;

    let mut left = lhs.clone();
    if lhs.data_type() != dtype {
        left = lhs.cast_with_type(&dtype)?;
    }

    let mut right = rhs.clone();
    if rhs.data_type() != dtype {
        right = rhs.cast_with_type(&dtype)?;
    }

    Ok((left, right))
}

fn coerce_to_signed(lhs: &Series) -> Result<Series> {
    let dtype = numerical_signed_coercion(&lhs.data_type())?;

    let mut left = lhs.clone();
    if lhs.data_type() != dtype {
        left = lhs.cast_with_type(&dtype)?;
    }

    Ok(left)
}
