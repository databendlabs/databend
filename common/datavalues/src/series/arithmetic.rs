use std::fmt::Debug;
use std::ops;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::arrays::DataArray;
use crate::numerical_arithmetic_coercion;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::DFBinaryArray;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFNullArray;
use crate::DFNumericType;
use crate::DFStringArray;
use crate::DFStructArray;
use crate::DataValueArithmeticOperator;

pub struct Arithmetic;

impl Arithmetic {
    pub fn sub(lhs: &Series, rhs: &Series) -> Result<Series> {
        let (lhs, rhs) = coerce_lhs_rhs(&DataValueArithmeticOperator::Minus, lhs, rhs)?;
        lhs.subtract(&rhs)
    }

    pub fn add(lhs: &Series, rhs: &Series) -> Result<Series> {
        let (lhs, rhs) = coerce_lhs_rhs(&DataValueArithmeticOperator::Plus, lhs, rhs)?;
        lhs.add_to(&rhs)
    }

    pub fn mul(lhs: &Series, rhs: &Series) -> Result<Series> {
        let (lhs, rhs) = coerce_lhs_rhs(&DataValueArithmeticOperator::Mul, lhs, rhs)?;
        lhs.multiply(&rhs)
    }

    pub fn div(lhs: &Series, rhs: &Series) -> Result<Series> {
        let (lhs, rhs) = coerce_lhs_rhs(&DataValueArithmeticOperator::Div, lhs, rhs)?;
        lhs.divide(&rhs)
    }

    pub fn rem(lhs: &Series, rhs: &Series) -> Result<Series> {
        let (lhs, rhs) = coerce_lhs_rhs(&DataValueArithmeticOperator::Modulo, lhs, rhs)?;
        lhs.subtract(&rhs)
    }
}

pub trait NumOpsDispatch: Debug {
    fn subtract(&self, rhs: &Series) -> Result<Series> {
        Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "subtraction operation not supported for {:?} and {:?}",
            self, rhs
        )))
    }

    fn add_to(&self, rhs: &Series) -> Result<Series> {
        Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "addition operation not supported for {:?} and {:?}",
            self, rhs
        )))
    }
    fn multiply(&self, rhs: &Series) -> Result<Series> {
        Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "multiplication operation not supported for {:?} and {:?}",
            self, rhs
        )))
    }
    fn divide(&self, rhs: &Series) -> Result<Series> {
        Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "division operation not supported for {:?} and {:?}",
            self, rhs
        )))
    }
    fn remainder(&self, rhs: &Series) -> Result<Series> {
        Err(ErrorCode::NumberArgumentsNotMatch(format!(
            "remainder operation not supported for {:?} and {:?}",
            self, rhs
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
        + num::One,
    DataArray<T>: IntoSeries,
{
    fn subtract(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(&rhs)? };
        let out = self - rhs;
        Ok(out.into_series().into())
    }
    fn add_to(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(&rhs)? };
        let out = self + rhs;
        Ok(out.into_series().into())
    }
    fn multiply(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(&rhs)? };
        let out = self * rhs;
        Ok(out.into_series().into())
    }
    fn divide(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(&rhs)? };
        let out = self / rhs;
        Ok(out.into_series().into())
    }
    fn remainder(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(&rhs)? };
        let out = self % rhs;
        Ok(out.into_series().into())
    }
}

impl NumOpsDispatch for DFStringArray {
    fn add_to(&self, rhs: &Series) -> Result<Series> {
        let rhs = unsafe { self.unpack_array_matching_physical_type(&rhs)? };
        let out = self + rhs;
        Ok(out.into_series().into())
    }
}
impl NumOpsDispatch for DFBooleanArray {}
impl NumOpsDispatch for DFListArray {}
impl NumOpsDispatch for DFBinaryArray {}
impl NumOpsDispatch for DFNullArray {}
impl NumOpsDispatch for DFStructArray {}

fn coerce_lhs_rhs<'a>(
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
